using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using ResiliencePoc.Api.Data;
using ResiliencePoc.Api.Hubs;
using ResiliencePoc.Api.Models;
using ResiliencePoc.Api.Services;
using ResiliencePoc.Api.Workers;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCors(opt => opt.AddPolicy("CorsPolicy", policy =>
    policy.WithOrigins("http://localhost:3000")
          .AllowAnyHeader()
          .AllowAnyMethod()
          .AllowCredentials()));

builder.Services.AddDbContext< OrderDbContext >(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton< IConnectionMultiplexer >(_ => 
    ConnectionMultiplexer.Connect(builder.Configuration["Redis:ConnectionString"] ?? "localhost:6379"));

builder.Services.AddSingleton< IPaymentService, MockPaymentService >();

builder.Services.AddSignalR();
builder.Services.AddHostedService< OrderConsumerWorker >();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService< OrderDbContext >();
    await db.Database.EnsureCreatedAsync();
}

app.UseCors("CorsPolicy");
app.MapHub< ResilienceHub >("/hubs/resilience");

app.MapGet("/api/orders", async (OrderDbContext db) =>
{
    var orders = await db.Orders.OrderByDescending(o => o.ProcessedAt).Take(20).ToListAsync();
    return Results.Ok(orders);
});

app.MapPost("/api/orders/produce", async (string type, IConfiguration config) =>
{
    var producerConfig = new ProducerConfig 
    { 
        BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092" 
    };
    
    using var producer = new ProducerBuilder< string, string >(producerConfig).Build();

    var orderId = type.ToLower() switch
    {
        "timeout" => $"ord-timeout-{Guid.NewGuid().ToString()[..6]}",
        "duplicate" => "ord-duplicate-trx-101",
        _ => $"ord-success-{Guid.NewGuid().ToString()[..6]}"
    };

    var payload = new OrderCreatedEvent(orderId, 1499.00m, "cust-enterprise-01", DateTime.UtcNow);
    var message = new Message< string, string >
    {
        Key = orderId,
        Value = JsonSerializer.Serialize(payload)
    };

    await producer.ProduceAsync("order-events", message);
    return Results.Ok(new { OrderId = orderId, Scenario = type });
});

app.MapPost("/api/orders/replay-dlq", async (OrderDbContext db, IConnectionMultiplexer redis, IConfiguration config) =>
{
    var failedOrders = await db.Orders
        .Where(o => o.Status == "FAILED_DLQ")
        .OrderBy(o => o.ProcessedAt)
        .Take(10)
        .ToListAsync();

    if (failedOrders.Count == 0)
    {
        return Results.Ok(new { ReplayedCount = 0, Message = "Replay edilecek DLQ siparişi bulunamadı." });
    }

    var producerConfig = new ProducerConfig 
    { 
        BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092" 
    };
    using var producer = new ProducerBuilder< string, string >(producerConfig).Build();
    var redisDb = redis.GetDatabase();

    foreach (var failed in failedOrders)
    {
        var cleanOrderId = failed.OrderId.Replace("-timeout-", "-replayed-");

        await redisDb.KeyDeleteAsync($"idempotency:order:{failed.OrderId}");
        await redisDb.KeyDeleteAsync($"idempotency:order:{cleanOrderId}");

        var payload = new OrderCreatedEvent(cleanOrderId, failed.Amount, failed.CustomerId, DateTime.UtcNow);
        await producer.ProduceAsync("order-events", new Message< string, string >
        {
            Key = cleanOrderId,
            Value = JsonSerializer.Serialize(payload)
        });
    }

    var failedOrderIds = failedOrders.Select(o => o.OrderId).ToList();
    
    await db.Orders
        .Where(o => failedOrderIds.Contains(o.OrderId))
        .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "REQUEUED"));

    return Results.Ok(new { ReplayedCount = failedOrders.Count, Message = $"{failedOrders.Count} sipariş DLQ'dan ana kuyruğa aktarıldı." });
});

app.Run();