using System.Text;
using System.Text.Json;
using Npgsql;
using RabbitMQ.Client;
using StackExchange.Redis;

namespace OmniShop.Payment;

public sealed record PaymentAuthorizationResult(
    string PaymentId,
    string OrderId,
    string Status,
    double FraudScore,
    string ServiceVersion,
    bool CachedIdempotency,
    double Amount);

public sealed record PaymentRecordDto(
    string PaymentId,
    string OrderId,
    string CustomerId,
    double Amount,
    string Currency,
    string Status,
    double FraudScore,
    string ServiceVersion,
    DateTimeOffset CreatedAt);

public sealed class PaymentEngine : IDisposable
{
    private readonly ILogger<PaymentEngine> _log;
    private readonly string _pgConn;
    private readonly Lazy<IConnectionMultiplexer?> _redis;
    private readonly string _rabbitHost;
    private readonly int _rabbitPort;
    private readonly string _rabbitExchange;
    private volatile bool _schemaReady;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);

    public string ServiceVersion { get; }
    public bool FraudCheckStrict { get; }
    public int ArtificialLatencyMs { get; }
    public int FaultErrorRatePct { get; }

    public PaymentEngine(IConfiguration config, ILogger<PaymentEngine> log)
    {
        _log = log;
        _pgConn = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
            ?? config.GetConnectionString("Postgres")
            ?? "Host=postgres-db;Port=5432;Database=omnishop;Username=postgres;Password=postgres;Timeout=3;Command Timeout=3";
        var redisConn = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")
            ?? config.GetConnectionString("Redis")
            ?? "redis-cache:6379,abortConnect=false,connectTimeout=1500,syncTimeout=1500";
        _rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? config["RabbitMq:Host"] ?? "rabbitmq-broker";
        _rabbitPort = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var rp) ? rp : 5672;
        _rabbitExchange = config["RabbitMq:Exchange"] ?? "omnishop.events";

        ServiceVersion = Environment.GetEnvironmentVariable("APP_VERSION") ?? config["OmniShop:Version"] ?? "v1";
        FraudCheckStrict = string.Equals(Environment.GetEnvironmentVariable("FRAUD_CHECK_STRICT"), "true", StringComparison.OrdinalIgnoreCase);
        ArtificialLatencyMs = Math.Clamp(int.TryParse(Environment.GetEnvironmentVariable("ARTIFICIAL_LATENCY_MS"), out var lat) ? lat : 0, 0, 3000);
        FaultErrorRatePct = Math.Clamp(int.TryParse(Environment.GetEnvironmentVariable("FAULT_ERROR_RATE_PCT"), out var pct) ? pct : 0, 0, 100);

        _redis = new Lazy<IConnectionMultiplexer?>(() =>
        {
            try { return ConnectionMultiplexer.Connect(redisConn); }
            catch (Exception ex)
            {
                _log.LogWarning("Redis unavailable on lazy connect: {Message}", ex.Message);
                return null;
            }
        });
    }

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        if (_schemaReady) return;
        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaReady) return;
            await using var conn = new NpgsqlConnection(_pgConn);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE IF NOT EXISTS payments_records (
                    payment_id VARCHAR(64) PRIMARY KEY,
                    order_id VARCHAR(64) NOT NULL,
                    customer_id VARCHAR(64) NOT NULL,
                    amount NUMERIC(12,2) NOT NULL,
                    currency VARCHAR(8) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    fraud_score DOUBLE PRECISION NOT NULL,
                    service_version VARCHAR(32) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                INSERT INTO payments_records (payment_id, order_id, customer_id, amount, currency, status, fraud_score, service_version)
                VALUES
                    ('PAY-1001', 'ORD-1001', 'cust-alice', 2499.00, 'USD', 'AUTHORIZED', 0.08, 'v1'),
                    ('PAY-1002', 'ORD-1002', 'cust-bob', 2398.00, 'USD', 'AUTHORIZED', 0.11, 'v1')
                ON CONFLICT (payment_id) DO NOTHING;
                """;
            await cmd.ExecuteNonQueryAsync(ct);
            _schemaReady = true;
            _log.LogInformation("PostgreSQL payments_records schema initialized (version={Version})", ServiceVersion);
        }
        catch (Exception ex)
        {
            _log.LogWarning("Deferred Postgres schema init: {Message}", ex.Message);
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public async Task<PaymentAuthorizationResult> AuthorizeAsync(
        string orderId, string customerId, double amount, string currency, string idempotencyKey, bool dryRun, CancellationToken ct)
    {
        if (ArtificialLatencyMs > 0)
            await Task.Delay(ArtificialLatencyMs, ct);
        if (FaultErrorRatePct > 0 && Random.Shared.Next(100) < FaultErrorRatePct)
            throw new InvalidOperationException($"Injected payment fault ({FaultErrorRatePct}%)");

        var idemKey = string.IsNullOrWhiteSpace(idempotencyKey) ? $"order:{orderId}" : idempotencyKey.Trim();
        var redisKey = $"omnishop:pay:idem:{idemKey}";

        // 1. Redis idempotency check
        try
        {
            var mux = _redis.Value;
            if (mux is { IsConnected: true })
            {
                var cached = await mux.GetDatabase().StringGetAsync(redisKey);
                if (cached.HasValue)
                {
                    var parsed = JsonSerializer.Deserialize<PaymentAuthorizationResult>(cached.ToString());
                    if (parsed is not null)
                        return parsed with { CachedIdempotency = true, ServiceVersion = ServiceVersion };
                }
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning("Redis idempotency check fallback: {Message}", ex.Message);
        }

        // 2. Fraud scoring (strict mode flags high-value orders)
        var fraudScore = Math.Round(Math.Min(0.95, 0.05 + (amount / 10000.0) * (FraudCheckStrict ? 1.8 : 0.4)), 3);
        var status = FraudCheckStrict && fraudScore > 0.70 ? "FLAGGED_REVIEW" : "AUTHORIZED";
        var paymentId = dryRun ? $"PAY-PROBE-{orderId}" : $"PAY-{Guid.NewGuid().ToString("N")[..8].ToUpperInvariant()}";

        // 3. PostgreSQL persistence (skip insert when dryRun probe, still query Postgres so DB span is emitted!)
        await EnsureSchemaAsync(ct);
        try
        {
            await using var conn = new NpgsqlConnection(_pgConn);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            if (dryRun)
            {
                cmd.CommandText = "SELECT COUNT(*) FROM payments_records WHERE order_id = @oid";
                cmd.Parameters.AddWithValue("oid", orderId);
                await cmd.ExecuteScalarAsync(ct);
            }
            else
            {
                cmd.CommandText = """
                    INSERT INTO payments_records (payment_id, order_id, customer_id, amount, currency, status, fraud_score, service_version)
                    VALUES (@pid, @oid, @cid, @amt, @cur, @st, @fs, @ver)
                    ON CONFLICT (payment_id) DO NOTHING
                    """;
                cmd.Parameters.AddWithValue("pid", paymentId);
                cmd.Parameters.AddWithValue("oid", orderId);
                cmd.Parameters.AddWithValue("cid", customerId);
                cmd.Parameters.AddWithValue("amt", (decimal)amount);
                cmd.Parameters.AddWithValue("cur", string.IsNullOrWhiteSpace(currency) ? "USD" : currency);
                cmd.Parameters.AddWithValue("st", status);
                cmd.Parameters.AddWithValue("fs", fraudScore);
                cmd.Parameters.AddWithValue("ver", ServiceVersion);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning("PostgreSQL payment operation degraded: {Message}", ex.Message);
        }

        var result = new PaymentAuthorizationResult(paymentId, orderId, status, fraudScore, ServiceVersion, false, amount);

        // 4. Cache idempotency result in Redis (15s TTL for probes, 5m for real payments)
        try
        {
            var mux = _redis.Value;
            if (mux is { IsConnected: true })
            {
                await mux.GetDatabase().StringSetAsync(
                    redisKey,
                    JsonSerializer.Serialize(result),
                    dryRun ? TimeSpan.FromSeconds(10) : TimeSpan.FromMinutes(5));
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning("Redis idempotency write skipped: {Message}", ex.Message);
        }

        // 5. Publish payment.settled event to RabbitMQ
        PublishPaymentSettled(result, dryRun);
        return result;
    }

    public async Task<IReadOnlyList<PaymentRecordDto>> ListRecentAsync(CancellationToken ct)
    {
        await EnsureSchemaAsync(ct);
        var list = new List<PaymentRecordDto>();
        await using var conn = new NpgsqlConnection(_pgConn);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT payment_id, order_id, customer_id, amount, currency, status, fraud_score, service_version, created_at
            FROM payments_records ORDER BY created_at DESC LIMIT 25
            """;
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            list.Add(new PaymentRecordDto(
                r.GetString(0), r.GetString(1), r.GetString(2), (double)r.GetDecimal(3),
                r.GetString(4), r.GetString(5), r.GetDouble(6), r.GetString(7),
                r.GetFieldValue<DateTimeOffset>(8)));
        }
        return list;
    }

    public async Task<PaymentRecordDto?> FindByOrderAsync(string orderId, CancellationToken ct)
    {
        await EnsureSchemaAsync(ct);
        await using var conn = new NpgsqlConnection(_pgConn);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT payment_id, order_id, customer_id, amount, currency, status, fraud_score, service_version, created_at
            FROM payments_records WHERE order_id = @oid ORDER BY created_at DESC LIMIT 1
            """;
        cmd.Parameters.AddWithValue("oid", orderId);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (!await r.ReadAsync(ct)) return null;
        return new PaymentRecordDto(
            r.GetString(0), r.GetString(1), r.GetString(2), (double)r.GetDecimal(3),
            r.GetString(4), r.GetString(5), r.GetDouble(6), r.GetString(7),
            r.GetFieldValue<DateTimeOffset>(8));
    }

    private void PublishPaymentSettled(PaymentAuthorizationResult res, bool dryRun)
    {
        try
        {
            var factory = new ConnectionFactory
            {
                HostName = _rabbitHost,
                Port = _rabbitPort,
                RequestedConnectionTimeout = TimeSpan.FromSeconds(2)
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.ExchangeDeclare(_rabbitExchange, ExchangeType.Topic, durable: true, autoDelete: false);
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
            {
                eventType = dryRun ? "payment.probe.authorized" : "payment.settled",
                res.PaymentId,
                res.OrderId,
                res.Status,
                res.FraudScore,
                res.ServiceVersion,
                occurredAt = DateTimeOffset.UtcNow
            }));
            channel.BasicPublish(_rabbitExchange, "payment.settled", null, body);
        }
        catch (Exception ex)
        {
            _log.LogWarning("RabbitMQ payment.settled publish degraded: {Message}", ex.Message);
        }
    }

    public void Dispose()
    {
        if (_redis.IsValueCreated) _redis.Value?.Dispose();
        _schemaLock.Dispose();
    }
}
