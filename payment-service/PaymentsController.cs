using Microsoft.AspNetCore.Mvc;

namespace OmniShop.Payment;

public sealed record AuthorizeRestRequest(
    string OrderId,
    string? CustomerId,
    double Amount,
    string? Currency,
    string? IdempotencyKey,
    bool DryRun = false);

[ApiController]
public sealed class PaymentsController : ControllerBase
{
    private readonly PaymentEngine _engine;

    public PaymentsController(PaymentEngine engine) => _engine = engine;

    [HttpGet("/health")]
    public IActionResult Health() => Ok(new
    {
        status = "UP",
        service = "payment-service",
        runtime = "dotnet8",
        version = _engine.ServiceVersion,
        protocols = new[] { "REST:8080", "gRPC:9090" },
        fraudCheckStrict = _engine.FraudCheckStrict,
        artificialLatencyMs = _engine.ArtificialLatencyMs,
        timestamp = DateTimeOffset.UtcNow
    });

    [HttpGet("/version")]
    public IActionResult Version() => Ok(new { service = "payment-service", version = _engine.ServiceVersion });

    [HttpGet("/api/payments")]
    public async Task<IActionResult> List(CancellationToken ct)
    {
        var items = await _engine.ListRecentAsync(ct);
        return Ok(new { items, total = items.Count, serviceVersion = _engine.ServiceVersion });
    }

    [HttpGet("/api/payments/order/{orderId}")]
    public async Task<IActionResult> GetByOrder(string orderId, CancellationToken ct)
    {
        var rec = await _engine.FindByOrderAsync(orderId, ct);
        return rec is null
            ? NotFound(new { error = "PAYMENT_NOT_FOUND", orderId })
            : Ok(rec);
    }

    [HttpPost("/api/payments/authorize")]
    public async Task<IActionResult> Authorize([FromBody] AuthorizeRestRequest req, CancellationToken ct)
    {
        var res = await _engine.AuthorizeAsync(
            req.OrderId,
            req.CustomerId ?? "cust-anonymous",
            req.Amount,
            req.Currency ?? "USD",
            req.IdempotencyKey ?? $"rest:{req.OrderId}",
            req.DryRun,
            ct);
        return Ok(new
        {
            paymentId = res.PaymentId,
            orderId = res.OrderId,
            status = res.Status,
            fraudScore = res.FraudScore,
            serviceVersion = res.ServiceVersion,
            cachedIdempotency = res.CachedIdempotency,
            transport = "REST"
        });
    }
}
