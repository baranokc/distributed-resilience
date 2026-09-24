using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Polly;
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
    private readonly IHubContext<ResilienceHub> _hubContext;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderConsumerWorker> _logger;

    public OrderConsumerWorker(
        IConfiguration config,
        IConnectionMultiplexer redis,
        IPaymentService paymentService,
        IHubContext<ResilienceHub> hubContext,
        IServiceScopeFactory scopeFactory,
        ILogger<OrderConsumerWorker> logger)
    {
        _config = config;
        _redis = redis;
        _paymentService = paymentService;
        _hubContext = hubContext;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var bootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "hepsiburada-resilience-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe("order-events");
        var redisDb = _redis.GetDatabase();

        _logger.LogInformation("Kafka Order Consumer devrede. 'order-events' dinleniyor...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (consumeResult == null) continue;

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(consumeResult.Message.Value);
                if (order == null) continue;

                var sw = Stopwatch.StartNew();

                // 1. KAFKA RECEIVED
                await BroadcastStep(order.OrderId, "KAFKA_RECEIVED", "INFO", "Mesaj Kafka kuyruğundan çekildi.");

                // 2. IDEMPOTENCY CHECK (Redis SETNX)
                var idempotencyKey = $"idempotency:order:{order.OrderId}";
                bool isFirstTime = await redisDb.StringSetAsync(idempotencyKey, "PROCESSING", TimeSpan.FromHours(24), When.NotExists);

                if (!isFirstTime)
                {
                    await BroadcastStep(order.OrderId, "IDEMPOTENCY_CHECK", "WARNING", 
                        $"[DUPLICATE DETECTED] {order.OrderId} Redis'te mevcut! Downstream ve DB yazımı atlandı.");
                    
                    consumer.Commit(consumeResult);
                    continue;
                }

                await BroadcastStep(order.OrderId, "IDEMPOTENCY_CHECK", "SUCCESS", "Idempotency onaylandı (İlk kez görülüyor).");

                // 3. POLLY RESILIENCE PIPELINE
                var pipeline = new ResiliencePipelineBuilder()
                    .AddRetry(new RetryStrategyOptions
                    {
                        MaxRetryAttempts = 3,
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

                // 4. DOWNSTREAM İŞLEMİ VE SQL KAYDI
                try
                {
                    await BroadcastStep(order.OrderId, "DOWNSTREAM_ATTEMPT", "INFO", "Ödeme Gateway çağrısı yapılıyor (Timeout: 2s)...");

                    await pipeline.ExecuteAsync(async token =>
                    {
                        await _paymentService.ProcessPaymentAsync(order.OrderId, order.Amount, token);
                    }, stoppingToken);

                    // Başarılı -> PostgreSQL'e COMPLETED olarak kaydet
                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
                        db.Orders.Add(new OrderEntity
                        {
                            OrderId = order.OrderId,
                            Amount = order.Amount,
                            CustomerId = order.CustomerId,
                            Status = "COMPLETED"
                        });
                        await db.SaveChangesAsync(stoppingToken);
                    }

                    sw.Stop();
                    await redisDb.StringSetAsync(idempotencyKey, "COMPLETED", TimeSpan.FromHours(24));
                    await BroadcastStep(order.OrderId, "COMPLETED", "SUCCESS", "Ödeme onaylandı ve sipariş PostgreSQL'e yazıldı.", null, sw.ElapsedMilliseconds);
                    
                    consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    // 5. TÜM DENEMELER TÜKENDİ -> SQL'e FAILED_DLQ KAYDI + DLQ PRODUCER
                    sw.Stop();

                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
                        db.Orders.Add(new OrderEntity
                        {
                            OrderId = order.OrderId,
                            Amount = order.Amount,
                            CustomerId = order.CustomerId,
                            Status = "FAILED_DLQ",
                            FailureReason = $"{ex.GetType().Name}: {ex.Message}"
                        });
                        await db.SaveChangesAsync(stoppingToken);
                    }

                    await BroadcastStep(order.OrderId, "DLQ", "ERROR", 
                        $"3 deneme tükendi ({ex.GetType().Name}). Sipariş DLQ'ya yönlendirildi ve PostgreSQL'e FAILED_DLQ olarak işlendi.");

                    var dlqMessage = new Message<string, string>
                    {
                        Key = consumeResult.Message.Key ?? order.OrderId,
                        Value = consumeResult.Message.Value,
                        Headers = new Headers
                        {
                            { "x-exception-type", Encoding.UTF8.GetBytes(ex.GetType().Name) },
                            { "x-error-message", Encoding.UTF8.GetBytes(ex.Message) },
                            { "x-retry-attempts", Encoding.UTF8.GetBytes("3") },
                            { "x-failed-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
                        }
                    };

                    await dlqProducer.ProduceAsync("order-events.DLQ", dlqMessage, stoppingToken);
                    consumer.Commit(consumeResult);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer döngü hatası.");
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