using NATS.Client.Core;
using Streamly.Audit;

namespace Streamly.Dashboard.Aggregation;

/// <summary>
/// Lightweight IJetStreamConnectionAccessor for the dashboard.
/// Creates a bare NatsConnection from config — no IStreamingTransport,
/// no leader election, no server-side dependencies.
/// The connection is auto-established by the NATS client on first use.
/// </summary>
internal sealed class DashboardNatsConnection : IJetStreamConnectionAccessor, IAsyncDisposable
{
    private readonly NatsConnection _connection;

    public DashboardNatsConnection(IConfiguration configuration)
    {
        var url = configuration["Streamly:Nats:Url"] ?? "nats://localhost:4222";
        _connection = new NatsConnection(new NatsOpts { Url = url });
    }

    public NatsConnection Connection => _connection;

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
