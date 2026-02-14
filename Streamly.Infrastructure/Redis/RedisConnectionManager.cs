using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Infrastructure.Redis;

/// <summary>
/// Redis connection manager with automatic reconnection.
/// Infrastructure layer - just Redis primitives, no business logic.
/// </summary>
public class RedisConnectionManager : IRedisConnectionManager
{
    private readonly ILogger<RedisConnectionManager> _logger;
    private readonly RedisConnectionOptions _options;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly ConcurrentDictionary<string, byte> _subscribedChannels = new();
    
    private IConnectionMultiplexer? _multiplexer;
    private bool _disposed;
    
    public event EventHandler<ConnectionStateChangedEventArgs>? ConnectionStateChanged;
    
    public IConnectionMultiplexer Multiplexer => 
        _multiplexer ?? throw new InvalidOperationException("Not connected to Redis");
    
    public bool IsConnected => _multiplexer?.IsConnected ?? false;
    
    public RedisConnectionManager(
        IOptions<RedisConnectionOptions> options,
        ILogger<RedisConnectionManager> logger)
    {
        _options = options.Value;
        _logger = logger;
        
        // Initialize connection on construction
        _ = EnsureConnectedAsync(CancellationToken.None);
    }
    
    #region Connection Management
    
    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_multiplexer?.IsConnected == true)
            return;
        
        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            // Double-check after acquiring lock
            if (_multiplexer?.IsConnected == true)
                return;
            
            _logger.LogInformation("Connecting to Redis: {Endpoints}", _options.ConnectionString);
            
            var config = ConfigurationOptions.Parse(_options.ConnectionString);
            config.AbortOnConnectFail = _options.AbortOnConnectFail;
            config.ConnectTimeout = _options.ConnectTimeoutMs;
            config.SyncTimeout = _options.SyncTimeoutMs;
            config.AsyncTimeout = _options.AsyncTimeoutMs;
            config.ConnectRetry = _options.ConnectRetryCount;
            config.KeepAlive = _options.KeepAliveSeconds;
            
            // Create new connection
            var newMultiplexer = await ConnectionMultiplexer.ConnectAsync(config);
            
            // Wire up events
            newMultiplexer.ConnectionRestored += OnConnectionRestored;
            newMultiplexer.ConnectionFailed += OnConnectionFailed;
            newMultiplexer.ErrorMessage += OnErrorMessage;
            
            // Swap connections
            var oldMultiplexer = _multiplexer;
            _multiplexer = newMultiplexer;
            
            // Dispose old connection
            if (oldMultiplexer != null)
            {
                oldMultiplexer.ConnectionRestored -= OnConnectionRestored;
                oldMultiplexer.ConnectionFailed -= OnConnectionFailed;
                oldMultiplexer.ErrorMessage -= OnErrorMessage;
                await oldMultiplexer.CloseAsync();
                oldMultiplexer.Dispose();
            }
            
            _logger.LogInformation("Connected to Redis");
            RaiseConnectionStateChanged(true, "Connected");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to Redis");
            RaiseConnectionStateChanged(false, ex.Message);
            throw;
        }
        finally
        {
            _connectionLock.Release();
        }
    }
    
    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        _logger.LogInformation("Redis connection restored: {EndPoint}", e.EndPoint);
        RaiseConnectionStateChanged(true, "Restored");
    }
    
    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        _logger.LogWarning(
            "Redis connection failed: {EndPoint}, {FailureType}, {Exception}", 
            e.EndPoint, e.FailureType, e.Exception?.Message);
        RaiseConnectionStateChanged(false, $"{e.FailureType}: {e.Exception?.Message}");
    }
    
    private void OnErrorMessage(object? sender, RedisErrorEventArgs e)
    {
        _logger.LogError("Redis error: {Message} on {EndPoint}", e.Message, e.EndPoint);
    }
    
    private void RaiseConnectionStateChanged(bool isConnected, string? reason)
    {
        try
        {
            ConnectionStateChanged?.Invoke(this, new ConnectionStateChangedEventArgs(isConnected, reason));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in ConnectionStateChanged event handler");
        }
    }
    
    #endregion
    
    #region Pub/Sub Operations
    
    public async Task<long> PublishAsync(
        string channel, 
        byte[] data, 
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(channel))
            throw new ArgumentException("Channel cannot be null or empty", nameof(channel));
        
        if (data == null)
            throw new ArgumentNullException(nameof(data));
        
        await EnsureConnectedAsync(cancellationToken);
        
        try
        {
            var subscriber = Multiplexer.GetSubscriber();
            
            var count = await subscriber.PublishAsync(
                RedisChannel.Literal(channel), 
                data,
                CommandFlags.FireAndForget);
            
            _logger.LogTrace("Published to {Channel}, {Count} subscribers", channel, count);
            
            return count;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish to {Channel}", channel);
            throw;
        }
    }
    
    public async Task SubscribeAsync(
        string channel, 
        Func<byte[], Task> handler, 
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(channel))
            throw new ArgumentException("Channel cannot be null or empty", nameof(channel));
        
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));
        
        await EnsureConnectedAsync(cancellationToken);
        
        try
        {
            var subscriber = Multiplexer.GetSubscriber();
            
            await subscriber.SubscribeAsync(
                RedisChannel.Literal(channel),
                async (redisChannel, value) =>
                {
                    try
                    {
                        await handler((byte[])value!);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error handling message from {Channel}", channel);
                    }
                });
            
            _subscribedChannels.TryAdd(channel, 0);
            _logger.LogInformation("Subscribed to channel: {Channel}", channel);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to subscribe to {Channel}", channel);
            throw;
        }
    }
    
    public async Task UnsubscribeAsync(string channel, CancellationToken cancellationToken = default)
    {
        if (_multiplexer?.IsConnected != true)
            return;
        
        try
        {
            var subscriber = Multiplexer.GetSubscriber();
            await subscriber.UnsubscribeAsync(RedisChannel.Literal(channel));
            
            _subscribedChannels.TryRemove(channel, out _);
            _logger.LogInformation("Unsubscribed from channel: {Channel}", channel);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unsubscribe from {Channel}", channel);
            throw;
        }
    }
    
    public async Task UnsubscribeAllAsync(CancellationToken cancellationToken = default)
    {
        if (_multiplexer?.IsConnected != true)
            return;
        
        try
        {
            var subscriber = Multiplexer.GetSubscriber();
            await subscriber.UnsubscribeAllAsync();
            
            _subscribedChannels.Clear();
            _logger.LogInformation("Unsubscribed from all channels");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unsubscribe from all channels");
            throw;
        }
    }
    
    #endregion
    
    #region Disposal
    
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        
        try
        {
            await UnsubscribeAllAsync();
            
            if (_multiplexer != null)
            {
                _multiplexer.ConnectionRestored -= OnConnectionRestored;
                _multiplexer.ConnectionFailed -= OnConnectionFailed;
                _multiplexer.ErrorMessage -= OnErrorMessage;
                
                await _multiplexer.CloseAsync();
                _multiplexer.Dispose();
            }
            
            _connectionLock.Dispose();
            
            _logger.LogInformation("Redis connection manager disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing Redis connection manager");
        }
    }
    
    #endregion
}
