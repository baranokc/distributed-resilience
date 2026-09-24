using Microsoft.EntityFrameworkCore;

namespace ResiliencePoc.Api.Data;

public class OrderEntity
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public required string OrderId { get; set; }
    public decimal Amount { get; set; }
    public required string CustomerId { get; set; }
    public required string Status { get; set; } // "COMPLETED", "FAILED_DLQ"
    public string? FailureReason { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime ProcessedAt { get; set; } = DateTime.UtcNow;
}

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options) { }

    public DbSet<OrderEntity> Orders => Set<OrderEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<OrderEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => e.OrderId).IsUnique();
        });
    }
}