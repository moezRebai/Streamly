using StackExchange.Redis;
using Streamly.Infrastructure.Redis;

namespace Streamly.Infrastructure.Interfaces;

/// <summary>
/// Manages Redis connection and provides raw Pub/Sub operations.
/// Infrastructure layer - no business logic, just Redis primitives.
/// </summary>
public interface IRedisConnectionManager : IAsyncDisposable
{
    /// <summary>
    /// Get the underlying Redis multiplexer
    /// </summary>
    IConnectionMultiplexer Multiplexer { get; }
    
    /// <summary>
    /// Check if connected to Redis
    /// </summary>
    bool IsConnected { get; }
    
    /// <summary>
    /// Connection state change event
    /// </summary>
    event EventHandler<ConnectionStateChangedEventArgs>? ConnectionStateChanged;
    
    /// <summary>
    /// Publish raw bytes to a channel
    /// </summary>
    /// <param name="channel">Channel name (e.g., "streams.requests.FxSwapPricer")</param>
    /// <param name="data">Serialized data</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of subscribers that received the message</returns>
    Task<long> PublishAsync(
        string channel, 
        byte[] data, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Subscribe to a channel with raw byte handler
    /// </summary>
    /// <param name="channel">Channel name</param>
    /// <param name="handler">Handler for raw bytes</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task SubscribeAsync(
        string channel, 
        Func<byte[], Task> handler, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Unsubscribe from a channel
    /// </summary>
    Task UnsubscribeAsync(string channel, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Unsubscribe from all channels
    /// </summary>
    Task UnsubscribeAllAsync(CancellationToken cancellationToken = default);
}