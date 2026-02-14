using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Streamly.Subscriber.Internal;

/// <summary>
/// Parallel dispatch worker pool
/// ONE Redis subscription feeds messages into this pool
/// N workers process messages in parallel
/// Workers are sharded by RequestId to preserve per-request ordering
///
/// Why:
///   10,000 simultaneous updates on ONE Redis channel
///   Sequential processing = too slow (violates 50ms target)
///   N workers = N parallel processing lanes
///   Same RequestId always goes to same worker = ordering preserved
/// </summary>
internal class DispatchWorkerPool<TResponse> : IAsyncDisposable
{
    private readonly Channel<DispatchItem>[] _workerChannels;
    private readonly Task[] _workerTasks;
    private readonly int _workerCount;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    public DispatchWorkerPool(
        int workerCount,
        int channelCapacity,
        Func<DispatchItem, Task> processItem,
        ILogger logger)
    {
        _workerCount = workerCount;
        _logger = logger;
        _cts = new CancellationTokenSource();

        // Create one bounded channel per worker
        // Bounded = backpressure: if worker falls behind, oldest messages drop
        _workerChannels = new Channel<DispatchItem>[workerCount];
        _workerTasks = new Task[workerCount];

        for (var i = 0; i < workerCount; i++)
        {
            _workerChannels[i] = Channel.CreateBounded<DispatchItem>(
                new BoundedChannelOptions(channelCapacity / workerCount)
                {
                    FullMode = BoundedChannelFullMode.DropOldest, // Drop oldest if full
                    SingleReader = true,                           // One worker per channel
                    SingleWriter = false                           // Multiple Redis threads write
                });

            var workerIndex = i;
            _workerTasks[i] = Task.Run(
                () => RunWorkerAsync(workerIndex, processItem, _cts.Token));
        }

        _logger.LogInformation(
            "DispatchWorkerPool started with {WorkerCount} workers, capacity {Capacity} per worker",
            workerCount,
            channelCapacity / workerCount);
    }

    /// <summary>
    /// Dispatch a message to the correct worker based on RequestId
    /// Same RequestId always goes to same worker (ordering preserved)
    /// Non-blocking: if channel full, oldest message is dropped
    /// </summary>
    public void Dispatch(string requestId, byte[] data, long epoch)
    {
        // Shard by RequestId hash to preserve per-request ordering
        var workerIndex = Math.Abs(requestId.GetHashCode()) % _workerCount;

        var item = new DispatchItem(requestId, data, epoch);

        // TryWrite is non-blocking
        // If channel full: DropOldest mode handles it automatically
        if (!_workerChannels[workerIndex].Writer.TryWrite(item))
        {
            _logger.LogWarning(
                "Dispatch channel {WorkerIndex} is full, message for request '{RequestId}' may be dropped",
                workerIndex,
                requestId);
        }
    }

    private async Task RunWorkerAsync(
        int workerIndex,
        Func<DispatchItem, Task> processItem,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Dispatch worker {WorkerIndex} started", workerIndex);

        try
        {
            var reader = _workerChannels[workerIndex].Reader;

            await foreach (var item in reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await processItem(item);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Worker {WorkerIndex} error processing message for request '{RequestId}'",
                        workerIndex,
                        item.RequestId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Dispatch worker {WorkerIndex} cancelled", workerIndex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Dispatch worker {WorkerIndex} unexpected error",
                workerIndex);
        }
        finally
        {
            _logger.LogDebug("Dispatch worker {WorkerIndex} stopped", workerIndex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogInformation("Stopping dispatch worker pool");

        // Signal all workers to stop
        _cts.Cancel();

        // Complete all writer channels
        foreach (var channel in _workerChannels)
            channel.Writer.TryComplete();

        // Wait for all workers to finish
        try
        {
            await Task.WhenAll(_workerTasks);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        _cts.Dispose();

        _logger.LogInformation("Dispatch worker pool stopped");
    }
}

/// <summary>
/// A single item to be dispatched by the worker pool
/// </summary>
internal record DispatchItem(
    string RequestId,
    byte[] Data,
    long Epoch);
