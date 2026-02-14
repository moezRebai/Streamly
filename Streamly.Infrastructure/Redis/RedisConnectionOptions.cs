namespace Streamly.Infrastructure.Redis;

/// <summary>
/// Configuration options for Redis connection.
/// Infrastructure layer - connection settings only.
/// </summary>
public class RedisConnectionOptions
{
    public const string SectionName = "Streamly:Redis";

    /// <summary>
    /// Redis connection string (e.g., "localhost:6379" or "host1:6379,host2:6379")
    /// </summary>
    public string ConnectionString { get; set; } = "localhost:6379";
    
    /// <summary>
    /// Connection timeout in milliseconds
    /// Default: 5000ms (5 seconds)
    /// </summary>
    public int ConnectTimeoutMs { get; set; } = 5000;
    
    /// <summary>
    /// Synchronous operation timeout in milliseconds
    /// Default: 5000ms (5 seconds)
    /// </summary>
    public int SyncTimeoutMs { get; set; } = 5000;
    
    /// <summary>
    /// Asynchronous operation timeout in milliseconds
    /// Default: 5000ms (5 seconds)
    /// </summary>
    public int AsyncTimeoutMs { get; set; } = 5000;
    
    /// <summary>
    /// Number of connection retry attempts
    /// Default: 3
    /// </summary>
    public int ConnectRetryCount { get; set; } = 3;
    
    /// <summary>
    /// Keep-alive interval in seconds (-1 to disable)
    /// Default: 60 seconds
    /// </summary>
    public int KeepAliveSeconds { get; set; } = 60;
    
    /// <summary>
    /// Abort connection attempt on first failure
    /// Default: false (allow retries)
    /// </summary>
    public bool AbortOnConnectFail { get; set; } = false;
}