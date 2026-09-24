namespace ResiliencePoc.Api.Models;

public record OrderCreatedEvent(string OrderId, decimal Amount, string CustomerId, DateTime CreatedAt);

public record PipelineStepEvent(
    string OrderId,
    string Stage,        // "KAFKA_RECEIVED", "IDEMPOTENCY_CHECK", "DOWNSTREAM_ATTEMPT", "RETRY", "DLQ", "COMPLETED"
    string Status,       // "INFO", "SUCCESS", "WARNING", "ERROR"
    string Message,
    int? RetryAttempt = null,
    long? LatencyMs = null
);