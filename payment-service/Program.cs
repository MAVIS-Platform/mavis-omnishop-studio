using Microsoft.AspNetCore.Server.Kestrel.Core;
using OmniShop.Payment;

var builder = WebApplication.CreateBuilder(args);

var restPort = int.TryParse(Environment.GetEnvironmentVariable("REST_PORT"), out var rp) ? rp : 8080;
var grpcPort = int.TryParse(Environment.GetEnvironmentVariable("GRPC_PORT"), out var gp) ? gp : 9090;

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(restPort, o => o.Protocols = HttpProtocols.Http1);
    options.ListenAnyIP(grpcPort, o => o.Protocols = HttpProtocols.Http2);
});

builder.Services.AddSingleton<PaymentEngine>();
builder.Services.AddControllers();
builder.Services.AddGrpc();

var app = builder.Build();

// Warm up PostgreSQL payments table in the background without blocking startup readiness.
_ = Task.Run(async () =>
{
    var engine = app.Services.GetRequiredService<PaymentEngine>();
    for (var attempt = 0; attempt < 10; attempt++)
    {
        await engine.EnsureSchemaAsync();
        await Task.Delay(TimeSpan.FromSeconds(2));
    }
});

app.MapControllers();
app.MapGrpcService<PaymentGatewayGrpcService>();

app.Run();
