using Grpc.Core;
using OmniShop.Payment.Grpc;

namespace OmniShop.Payment;

public sealed class PaymentGatewayGrpcService : PaymentGateway.PaymentGatewayBase
{
    private readonly PaymentEngine _engine;

    public PaymentGatewayGrpcService(PaymentEngine engine) => _engine = engine;

    public override async Task<AuthorizePaymentResponse> AuthorizePayment(AuthorizePaymentRequest request, ServerCallContext context)
    {
        var res = await _engine.AuthorizeAsync(
            request.OrderId,
            request.CustomerId,
            request.Amount,
            request.Currency,
            request.IdempotencyKey,
            request.DryRun,
            context.CancellationToken);

        return new AuthorizePaymentResponse
        {
            PaymentId = res.PaymentId,
            OrderId = res.OrderId,
            Status = res.Status,
            FraudScore = res.FraudScore,
            ServiceVersion = res.ServiceVersion,
            CachedIdempotency = res.CachedIdempotency
        };
    }

    public override async Task<PaymentStatusResponse> GetPaymentStatus(PaymentStatusRequest request, ServerCallContext context)
    {
        var rec = await _engine.FindByOrderAsync(request.OrderId, context.CancellationToken);
        if (rec is null)
            throw new RpcException(new Status(StatusCode.NotFound, $"Payment not found for order {request.OrderId}"));

        return new PaymentStatusResponse
        {
            PaymentId = rec.PaymentId,
            OrderId = rec.OrderId,
            Status = rec.Status,
            Amount = rec.Amount,
            ServiceVersion = rec.ServiceVersion
        };
    }

    public override Task<HealthProbeResponse> CheckHealth(HealthProbeRequest request, ServerCallContext context) =>
        Task.FromResult(new HealthProbeResponse
        {
            Status = "UP",
            ServiceVersion = _engine.ServiceVersion
        });
}
