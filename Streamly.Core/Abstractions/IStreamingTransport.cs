namespace Streamly.Core.Abstractions;

/// <summary>
/// Common transport abstraction over Redis Pub/Sub and NATS Core.
/// Both implementations accept raw bytes and string subject/channel names.
/// All business logic (naming, retry, lifecycle) lives in the Runtime layer.
/// </summary>
public interface IStreamingTransport : IAsyncDisposable
{
    /// <summary>
    /// Publish raw bytes to a subject/channel.
    /// Returns the number of subscribers that received the message (Redis) or 0 (NATS).
    /// </summary>
    Task<long> PublishAsync(string subject, byte[] data, CancellationToken ct = default);

    /// <summary>
    /// Subscribe to a subject/channel. Wildcard support is transport-specific:
    ///   Redis: pattern matching via PSUBSCRIBE (e.g. "stream:*")
    ///   NATS : native wildcard (e.g. "stream.>" or "stream.*")
    /// Multiple calls with the same subject are idempotent (handler is replaced).
    /// </summary>
    Task SubscribeAsync(string subject, Func<byte[], Task> handler, CancellationToken ct = default);

    /// <summary>
    /// Unsubscribe from a subject/channel. No-op if not subscribed.
    /// </summary>
    Task UnsubscribeAsync(string subject, CancellationToken ct = default);

    /// <summary>True when the underlying connection is established and healthy.</summary>
    bool IsConnected { get; }

    /// <summary>Raised when a connection error occurs (after internal reconnect attempts).</summary>
    event Action<Exception>? OnConnectionError;

    /// <summary>Raised after a successful reconnection.</summary>
    event Action? OnReconnected;
}
