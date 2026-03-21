using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Streamly.Client.Internal;

/// <summary>
/// Sharded response dispatcher.
/// ONE NATS subscription feeds messages into this dispatcher.
/// N shards process messages in parallel.
/// Shards are assigned by RequestId hash to preserve per-request ordering.
///
/// Why:
///   10,000 simultaneous updates on ONE NATS channel
///   Sequential processing = too slow (violates 50ms target)
///   N shards = N parallel processing lanes
///   Same RequestId always goes to same shard = ordering preserved
/// </summary>
internal sealed class ResponseShardDispatcher : IAsyncDisposable
{
    private readonly Channel<Response>[] _shardChannels;
    private readonly Task[] _shardTasks;
    private readonly int _shardCount;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    public ResponseShardDispatcher(
        int shardCount,
        int channelCapacity,
        Func<Response, Task> processResponse,
        ILogger logger)
    {
        // FIX: validate inputs — a zero/negative shardCount causes a divide-by-zero
        // in the capacity-per-shard calculation and an index-out-of-range in Dispatch().
        if (shardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(shardCount), "Must be >= 1");
        if (channelCapacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(channelCapacity), "Must be >= 1");
        ArgumentNullException.ThrowIfNull(processResponse);
        ArgumentNullException.ThrowIfNull(logger);

        _shardCount = shardCount;
        _logger = logger;
        _cts = new CancellationTokenSource();

        var capacityPerShard = Math.Max(1, channelCapacity / shardCount);

        _shardChannels = new Channel<Response>[shardCount];
        _shardTasks    = new Task[shardCount];

        for (var i = 0; i < shardCount; i++)
        {
            _shardChannels[i] = Channel.CreateBounded<Response>(
                new BoundedChannelOptions(capacityPerShard)
                {
                    FullMode     = BoundedChannelFullMode.DropOldest,
                    SingleReader = true,  // One shard processor per channel
                    SingleWriter = false  // Multiple NATS threads write
                });

            var shardIndex = i;
            _shardTasks[i] = Task.Run(
                () => RunShardAsync(shardIndex, processResponse, _cts.Token));
        }

        _logger.LogInformation(
            "ResponseShardDispatcher started with {ShardCount} workers, {Capacity} requests/worker",
            shardCount, capacityPerShard);
    }

    /// <summary>
    /// Routes a response to the correct shard based on RequestId hash.
    /// Same RequestId always goes to same shard — per-request ordering preserved.
    /// Non-blocking: BoundedChannelFullMode.DropOldest means the channel itself
    /// evicts the oldest queued message when full — TryWrite always returns true
    /// under this mode, so no warning is needed here.
    /// </summary>
    public void Dispatch(string requestId, byte[] data, long epoch)
    {
        var shardIndex = Math.Abs(requestId.GetHashCode()) % _shardCount;
        _shardChannels[shardIndex].Writer.TryWrite(new Response(requestId, data, epoch));
    }

    private async Task RunShardAsync(
        int shardIndex,
        Func<Response, Task> processResponse,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Response shard {ShardIndex} started", shardIndex);

        try
        {
            await foreach (var response in _shardChannels[shardIndex].Reader
                               .ReadAllAsync(cancellationToken))
            {
                try
                {
                    await processResponse(response);
                }
                catch (Exception ex)
                {
                    // Isolate per-item failures — the shard keeps running.
                    _logger.LogError(ex,
                        "Shard {ShardIndex} error processing response for request '{RequestId}'",
                        shardIndex, response.RequestId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Response shard {ShardIndex} cancelled", shardIndex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Response shard {ShardIndex} unexpected error", shardIndex);
        }
        finally
        {
            _logger.LogDebug("Response shard {ShardIndex} stopped", shardIndex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogDebug("Stopping ResponseShardDispatcher");

        await _cts.CancelAsync();

        foreach (var channel in _shardChannels)
            channel.Writer.TryComplete();

        try
        {
            await Task.WhenAll(_shardTasks);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }

        _cts.Dispose();

        _logger.LogDebug("ResponseShardDispatcher stopped");
    }
}