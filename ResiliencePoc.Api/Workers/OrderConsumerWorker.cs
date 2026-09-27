using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;
using ResiliencePoc.Api.Data;
using ResiliencePoc.Api.Hubs;
using ResiliencePoc.Api.Models;
using ResiliencePoc.Api.Services;
using StackExchange.Redis;

namespace ResiliencePoc.Api.Workers;

public class OrderConsumerWorker : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly IConnectionMultiplexer _redis;
    private readonly IPaymentService _paymentService;
    private readonly IHubContext< ResilienceHub > _hubContext;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger< OrderConsumerWorker > _logger;
    private readonly ResiliencePipeline _circuitBreaker;

    public OrderConsumerWorker(
        IConfiguration config,
        IConnectionMultiplexer redis,
        IPaymentService paymentService,
        IHubContext< ResilienceHub > hubContext,
        IServiceScopeFactory scopeFactory,
        ILogger< OrderConsumerWorker > logger)
    {
        _config = config;
        _redis = redis;
        _paymentService = paymentService;
        _hubContext = hubContext;
        _scopeFactory = scopeFactory;
        _logger = logger;

        _circuitBreaker = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(10),
                MinimumThroughput = 2,
                BreakDuration = TimeSpan.FromSeconds(15),
                OnOpened = async _ =>
                {
                    await _hubContext.Clients.All.SendAsync("ReceiveCircuitBreakerState", "OPEN");
                },
                OnClosed = async _ =>
                {
                    await _hubContext.Clients.All.SendAsync("ReceiveCircuitBreakerState", "CLOSED");
                },
                OnHalfOpened = async _ =>
                {
                    await _hubContext.Clients.All.SendAsync("ReceiveCircuitBreakerState", "HALF_OPEN");
                }
            })
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var bootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "enterprise-resilience-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var consumer = new ConsumerBuilder< string, string >(consumerConfig).Build();
        using var dlqProducer = new ProducerBuilder< string, string >(producerConfig).Build();

        consumer.Subscribe("order-events");
        var redisDb = _redis.GetDatabase();

        _logger.LogInformation("Enterprise Order Consumer devrede. 'order-events' dinleniyor...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (consumeResult == null) continue;

                var order = JsonSerializer.Deserialize< OrderCreatedEvent >(consumeResult.Message.Value);
                if (order == null) continue;

                var sw = Stopwatch.StartNew();

                await BroadcastStep(order.OrderId, "KAFKA_RECEIVED", "INFO", "Mesaj Kafka kuyruğundan çekildi.");

                var idempotencyKey = $"idempotency:order:{order.OrderId}";
                bool isFirstTime = await redisDb.StringSetAsync(idempotencyKey, "PROCESSING", TimeSpan.FromHours(24), When.NotExists);

                if (!isFirstTime)
                {
                    await BroadcastStep(order.OrderId, "IDEMPOTENCY_CHECK", "WARNING",
                        $"[DUPLICATE DETECTED] {order.OrderId} Redis'te mevcut! Downstream ve DB yazımı güvenle atlandı.");
                    
                    consumer.Commit(consumeResult);
                    continue;
                }

                await BroadcastStep(order.OrderId, "IDEMPOTENCY_CHECK", "SUCCESS", "Idempotency doğrulandı (İlk işlem).");

                var pipeline = new ResiliencePipelineBuilder()
                    .AddPipeline(_circuitBreaker)
                    .AddRetry(new RetryStrategyOptions
                    {
                        MaxRetryAttempts = 2,
                        BackoffType = DelayBackoffType.Exponential,
                        Delay = TimeSpan.FromSeconds(1),
                        UseJitter = true,
                        OnRetry = async args =>
                        {
                            await BroadcastStep(order.OrderId, "RETRY", "WARNING",
                                $"Deneme {args.AttemptNumber} başarısız! Hata: {args.Outcome.Exception?.Message}. Yeniden deneniyor...",
                                args.AttemptNumber);
                        }
                    })
                    .AddTimeout(new TimeoutStrategyOptions
                    {
                        Timeout = TimeSpan.FromSeconds(2)
                    })
                    .Build();

                async Task RecordFailureAndSendDlq(string reason)
                {
                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var db = scope.ServiceProvider.GetRequiredService< OrderDbContext >();
                        
                        var existing = await db.Orders.Where(o => o.OrderId == order.OrderId).FirstOrDefaultAsync(stoppingToken);
                        if (existing == null)
                        {
                            db.Orders.Add(new OrderEntity
                            {
                                OrderId = order.OrderId,
                                Amount = order.Amount,
                                CustomerId = order.CustomerId,
                                Status = "FAILED_DLQ",
                                FailureReason = reason
                            });
                        }
                        else
                        {
                            existing.Status = "FAILED_DLQ";
                            existing.FailureReason = reason;
                            existing.ProcessedAt = DateTime.UtcNow;
                        }
                        await db.SaveChangesAsync(stoppingToken);
                    }

                    var dlqMessage = new Message< string, string >
                    {
                        Key = consumeResult.Message.Key ?? order.OrderId,
                        Value = consumeResult.Message.Value,
                        Headers = new Headers
                        {
                            { "x-failure-reason", Encoding.UTF8.GetBytes(reason) },
                            { "x-failed-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
                        }
                    };

                    await dlqProducer.ProduceAsync("order-events.DLQ", dlqMessage, stoppingToken);
                }

                try
                {
                    await BroadcastStep(order.OrderId, "DOWNSTREAM_ATTEMPT", "INFO", "Ödeme Gateway çağrısı yapılıyor (Timeout: 2s)...");

                    await pipeline.ExecuteAsync(async token =>
                    {
                        await _paymentService.ProcessPaymentAsync(order.OrderId, order.Amount, token);
                    }, stoppingToken);

                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var db = scope.ServiceProvider.GetRequiredService< OrderDbContext >();
                        var existing = await db.Orders.Where(o => o.OrderId == order.OrderId).FirstOrDefaultAsync(stoppingToken);
                        if (existing == null)
                        {
                            db.Orders.Add(new OrderEntity
                            {
                                OrderId = order.OrderId,
                                Amount = order.Amount,
                                CustomerId = order.CustomerId,
                                Status = "COMPLETED"
                            });
                        }
                        else
                        {
                            existing.Status = "COMPLETED";
                            existing.FailureReason = null;
                            existing.ProcessedAt = DateTime.UtcNow;
                        }
                        await db.SaveChangesAsync(stoppingToken);
                    }

                    sw.Stop();
                    await redisDb.StringSetAsync(idempotencyKey, "COMPLETED", TimeSpan.FromHours(24));
                    await BroadcastStep(order.OrderId, "COMPLETED", "SUCCESS", "Ödeme onaylandı ve veritabanına COMPLETED olarak yazıldı.", null, sw.ElapsedMilliseconds);

                    consumer.Commit(consumeResult);
                }
                catch (BrokenCircuitException)
                {
                    sw.Stop();
                    await BroadcastStep(order.OrderId, "DLQ", "ERROR", 
                        "[FAIL-FAST] Circuit Breaker AÇIK (OPEN)! Downstream servise gidilmeden sipariş anında DLQ'ya alındı.");

                    await RecordFailureAndSendDlq("BrokenCircuitException: Downstream circuit open");
                    consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    sw.Stop();
                    await BroadcastStep(order.OrderId, "DLQ", "ERROR",
                        $"Denemeler tükendi ({ex.GetType().Name}). Sipariş DLQ kuyruğuna aktarıldı.");

                    await RecordFailureAndSendDlq($"{ex.GetType().Name}: {ex.Message}");
                    consumer.Commit(consumeResult);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer genel hatası.");
            }
        }

        consumer.Close();
    }

    private async Task BroadcastStep(string orderId, string stage, string status, string message, int? retry = null, long? latency = null)
    {
        var stepEvent = new PipelineStepEvent(orderId, stage, status, message, retry, latency);
        await _hubContext.Clients.All.SendAsync("ReceivePipelineStep", stepEvent);
    }
}