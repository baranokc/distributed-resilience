namespace ResiliencePoc.Api.Services;

public interface IPaymentService
{
    Task ProcessPaymentAsync(string orderId, decimal amount, CancellationToken cancellationToken);
}

public class MockPaymentService : IPaymentService
{
    public async Task ProcessPaymentAsync(string orderId, decimal amount, CancellationToken cancellationToken)
    {
        if (orderId.Contains("timeout", StringComparison.OrdinalIgnoreCase))
        {
            await Task.Delay(5000, cancellationToken);
        }
        else
        {
            await Task.Delay(150, cancellationToken);
        }
    }
}