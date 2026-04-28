using System.Collections.Concurrent;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Monitoring.Models;

namespace Streamly.Monitoring.Collectors;

/// <summary>
/// In-process, lock-free implementation of <see cref="IStreamlyMetricsCollector"/>.
/// All counter writes use <see cref="Interlocked"/> operations — no lock contention
/// on the hot publish path. Per-stream state is stored in a
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> keyed by requestId.
///
/// Registered as a singleton by AddStreamlyMonitoring(), replacing the
/// NullMetricsCollector default registered by AddStreamlyServer() /
/// AddStreamlyClient().
/// </summary>
public sealed class InMemoryMetricsCollector(
    MonitoringOptions options,
    ProcessMetricsSampler processSampler) : IStreamlyMetricsCollector
{
    // ── Process-level counters (Interlocked) ──────────────────────────────────
    private long _totalStreamsOpened;
    private long _totalStreamsClosed;
    private long _totalPublishes;
    private long _totalPublishSkips;
    private long _totalPublishErrors;
    private long _totalSubscriptionsOpened;
    private long _totalSubscriptionsClosed;
    private long _totalMessagesReceived;
    private long _totalWatchdogTriggers;
    private long _totalReconnectionAttempts;
    private long _totalConfirmationFailures;

    // ── Leadership state (volatile — single writer from election events) ──────
    private volatile bool _isLeader;
    private volatile int _leaderEpoch;
    private volatile bool _natsConnected = true;

    // ── Per-stream live state ─────────────────────────────────────────────────
    private readonly ConcurrentDictionary<string, StreamEntry> _streams = new();

    // ── Publish rate tracking ─────────────────────────────────────────────────
    // Ring buffer of publish timestamps for rolling rate computation.
    // Capacity = max expected publishes per rate window (default 60s × 40k/s = 2.4M).
    // Sized conservatively — entries outside the window are ignored on read.
    private readonly ConcurrentQueue<long> _publishTimestampsTicks = new();
    private readonly TimeSpan _rateWindow = options.PublishRateWindow;

    private readonly DateTimeOffset _startedAt = DateTimeOffset.UtcNow;
    private readonly MonitoringOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    // ── IStreamlyMetricsCollector — publisher stream lifecycle ────────────────

    public void RecordStreamOpened(string requestId, string streamName)
    {
        Interlocked.Increment(ref _totalStreamsOpened);

        // Use indexer (not TryAdd) — requestIds are deterministic content hashes, so the same
        // logical stream reuses the same requestId after a subscriber reconnect. TryAdd would
        // silently leave a stale "Closed" entry in place, causing the stream to appear inactive.
        _streams[requestId] = new StreamEntry
        {
            RequestId  = requestId,
            StreamName = streamName,
            State      = "Streaming",
            OpenedAt   = DateTimeOffset.UtcNow
        };
    }

    public void RecordStreamClosed(string requestId, CloseReason reason)
    {
        Interlocked.Increment(ref _totalStreamsClosed);

        if (_streams.TryGetValue(requestId, out var entry))
        {
            entry.State    = "Closed";
            entry.ClosedAt = DateTimeOffset.UtcNow;
        }
    }

    // ── IStreamlyMetricsCollector — publish decisions ─────────────────────────

    public void RecordPublish(string requestId, string streamName, int payloadBytes)
    {
        Interlocked.Increment(ref _totalPublishes);

        // Stamp for rolling rate — store ticks (long) to avoid DateTime allocation
        _publishTimestampsTicks.Enqueue(DateTime.UtcNow.Ticks);

        if (_streams.TryGetValue(requestId, out var entry))
            Interlocked.Increment(ref entry.PublishCount);
    }

    public void RecordPublishSkipped(string requestId)
    {
        Interlocked.Increment(ref _totalPublishSkips);

        if (_streams.TryGetValue(requestId, out var entry))
            Interlocked.Increment(ref entry.SkipCount);
    }

    public void RecordPublishError(string requestId, string errorMessage)
    {
        Interlocked.Increment(ref _totalPublishErrors);
    }

    public void RecordLatestImage(string requestId, string streamName, string rawJson)
    {
        if (_streams.TryGetValue(requestId, out var entry))
        {
            // Volatile write — readers see the most recent value without a lock.
            // string assignment is atomic on all .NET platforms (reference type).
            entry.LatestImageJson  = rawJson;
            entry.LastPublishedAt  = DateTimeOffset.UtcNow;
            entry.SubscriberCount  = entry.SubscriberCount; // no-op; updated separately
        }
    }

    // ── IStreamlyMetricsCollector — subscriber lifecycle ─────────────────────

    public void RecordSubscriptionAttempted(string requestId, string streamName, string requestJson)
    {
        // Create a "Pending" entry the moment the subscriber attempts the stream.
        // This makes the stream visible in monitoring even if no publisher ever responds.
        // RecordSubscriptionOpened promotes it to "Streaming" on confirmation.
        // RecordSubscriptionClosed marks it "NoProvider" when all retries are exhausted.
        _streams.GetOrAdd(requestId, id => new StreamEntry
        {
            RequestId  = id,
            StreamName = streamName,
            State      = "Pending",
            OpenedAt   = DateTimeOffset.UtcNow,
            RequestJson = requestJson
        });
    }

    public void RecordSubscriptionConfirmed(string trackingId, string requestId, string streamName)
    {
        Interlocked.Increment(ref _totalSubscriptionsOpened);

        // Remove the ephemeral GUID-keyed pending entry, carrying its RequestJson forward
        // so the stream detail page continues to show the original request payload.
        _streams.TryRemove(trackingId, out var pending);

        // Use indexer so a reconnect after a prior Closed entry gets a fresh Streaming state.
        _streams[requestId] = new StreamEntry
        {
            RequestId   = requestId,
            StreamName  = streamName,
            State       = "Streaming",
            OpenedAt    = DateTimeOffset.UtcNow,
            RequestJson = pending?.RequestJson
        };
    }

    public void RecordSubscriptionOpened(string requestId, string streamName)
    {
        Interlocked.Increment(ref _totalSubscriptionsOpened);

        // Promote an existing "Pending" entry to "Streaming" on publisher confirmation,
        // or create a fresh entry if RecordSubscriptionAttempted was not called
        // (backward-compatible path for publisher-only instances).
        var entry = _streams.GetOrAdd(requestId, id => new StreamEntry
        {
            RequestId  = id,
            StreamName = streamName,
            State      = "Streaming",
            OpenedAt   = DateTimeOffset.UtcNow
        });

        entry.State = "Streaming";
    }

    public void RecordSubscriptionClosed(string requestId, CloseReason reason)
    {
        if (_streams.TryGetValue(requestId, out var entry))
        {
            // Only count as closed if it was confirmed — Pending entries that expire
            // to NoProvider were never opened, so adjusting the counter would corrupt
            // the ActiveSubscriptions calculation.
            if (entry.State == "Streaming")
                Interlocked.Increment(ref _totalSubscriptionsClosed);

            entry.State    = reason == CloseReason.NoProvider ? "NoProvider" : "Closed";
            entry.ClosedAt = DateTimeOffset.UtcNow;
        }
    }

    public void RecordMessageReceived(string requestId)
    {
        Interlocked.Increment(ref _totalMessagesReceived);
    }

    public void RecordWatchdogTrigger(string requestId)
    {
        Interlocked.Increment(ref _totalWatchdogTriggers);
    }

    public void RecordReconnectionAttempt(string requestId)
    {
        Interlocked.Increment(ref _totalReconnectionAttempts);
    }

    public void RecordConfirmationFailure(string streamName)
    {
        Interlocked.Increment(ref _totalConfirmationFailures);
    }

    // ── IStreamlyMetricsCollector — infrastructure ────────────────────────────

    public void RecordLeaderStateChange(bool isLeader, int epoch)
    {
        _isLeader    = isLeader;
        _leaderEpoch = epoch;
    }

    public void RecordNatsConnectionStateChange(bool isConnected)
    {
        _natsConnected = isConnected;
    }

    // ── Snapshot production ───────────────────────────────────────────────────

    /// <summary>
    /// Produces a point-in-time snapshot of all process-level counters.
    /// Called by MetricsEndpoint on every HTTP request — allocation is
    /// acceptable here since it is off the hot publish path.
    /// </summary>
    public InstanceMetricsSnapshot GetSnapshot()
    {
        var pendingSubscriptions   = _streams.Values.Count(e => e.State == "Pending");
        var publisherActiveStreams  = (int)Math.Max(0, _totalStreamsOpened - _totalStreamsClosed);
        var subscriberActiveStreams = (int)Math.Max(0, _totalSubscriptionsOpened - _totalSubscriptionsClosed) + pendingSubscriptions;
        var activeStreams           = publisherActiveStreams + subscriberActiveStreams;

        var activeSubscriptions = (int)(_totalSubscriptionsOpened - _totalSubscriptionsClosed);

        return new InstanceMetricsSnapshot
        {
            InstanceId               = _options.InstanceId,
            InstanceRole             = _options.InstanceRole,
            StartedAt                = _startedAt,
            SnapshotAt               = DateTimeOffset.UtcNow,
            IsLeader                 = _isLeader,
            LeaderEpoch              = _leaderEpoch,
            NatsConnected            = _natsConnected,
            ActiveStreams             = activeStreams,
            PendingSubscriptions     = pendingSubscriptions,
            PublisherActiveStreams    = publisherActiveStreams,
            SubscriberActiveStreams   = subscriberActiveStreams,
            TotalStreamsOpened        = Interlocked.Read(ref _totalStreamsOpened),
            TotalStreamsClosed       = Interlocked.Read(ref _totalStreamsClosed),
            TotalPublishes           = Interlocked.Read(ref _totalPublishes),
            TotalPublishSkips        = Interlocked.Read(ref _totalPublishSkips),
            TotalPublishErrors       = Interlocked.Read(ref _totalPublishErrors),
            PublishRatePerSec        = ComputePublishRate(),
            ActiveSubscriptions      = Math.Max(0, activeSubscriptions),
            TotalSubscriptionsOpened = Interlocked.Read(ref _totalSubscriptionsOpened),
            TotalSubscriptionsClosed = Interlocked.Read(ref _totalSubscriptionsClosed),
            TotalMessagesReceived    = Interlocked.Read(ref _totalMessagesReceived),
            TotalWatchdogTriggers     = Interlocked.Read(ref _totalWatchdogTriggers),
            TotalReconnectionAttempts = Interlocked.Read(ref _totalReconnectionAttempts),
            TotalConfirmationFailures = Interlocked.Read(ref _totalConfirmationFailures),
            CpuPercent                = processSampler.CpuPercent,
            WorkingSetBytes           = processSampler.WorkingSetBytes,
            GcHeapBytes               = processSampler.GcHeapBytes
        };
    }

    /// <summary>
    /// Returns all active stream records, optionally filtered by streamName.
    /// Closed streams are included until RetainClosedStreamsFor has elapsed.
    /// Called by StreamsEndpoint — allocation here is acceptable.
    /// </summary>
    public IReadOnlyList<StreamDetailRecord> GetStreams(string? streamNameFilter = null)
    {
        var cutoff = DateTimeOffset.UtcNow - _options.RetainClosedStreamsFor;

        var result = new List<StreamDetailRecord>();

        foreach (var entry in _streams.Values)
        {
            // Evict closed/no-provider streams past the retention window
            if (entry.State is "Closed" or "NoProvider" && entry.ClosedAt.HasValue && entry.ClosedAt < cutoff)
                continue;

            if (streamNameFilter != null &&
                !string.Equals(entry.StreamName, streamNameFilter,
                    StringComparison.OrdinalIgnoreCase))
                continue;

            result.Add(new StreamDetailRecord
            {
                RequestId                = entry.RequestId,
                StreamName               = entry.StreamName,
                State                    = entry.State,
                SubscriberCount          = entry.SubscriberCount,
                PublishCount             = Interlocked.Read(ref entry.PublishCount),
                SkipCount                = Interlocked.Read(ref entry.SkipCount),
                OpenedAt                 = entry.OpenedAt,
                LastPublishedAt          = entry.LastPublishedAt,
                FirstResponseLatencyMs   = entry.FirstResponseLatencyMs,
                LastLatencyMs            = entry.LastLatencyMs,
                AvgLatencyMs             = entry.AvgLatencyMs,
                LatestImageJson          = entry.LatestImageJson,
                RequestJson              = entry.RequestJson
            });
        }

        return result;
    }

    /// <summary>
    /// Returns a single stream record by requestId, or null if not found
    /// or past the retention window.
    /// </summary>
    public StreamDetailRecord? GetStream(string requestId)
    {
        if (!_streams.TryGetValue(requestId, out var entry))
            return null;

        var cutoff = DateTimeOffset.UtcNow - _options.RetainClosedStreamsFor;
        if (entry.State is "Closed" or "NoProvider" && entry.ClosedAt.HasValue && entry.ClosedAt < cutoff)
            return null;

        return new StreamDetailRecord
        {
            RequestId                = entry.RequestId,
            StreamName               = entry.StreamName,
            State                    = entry.State,
            SubscriberCount          = entry.SubscriberCount,
            PublishCount             = Interlocked.Read(ref entry.PublishCount),
            SkipCount                = Interlocked.Read(ref entry.SkipCount),
            OpenedAt                 = entry.OpenedAt,
            LastPublishedAt          = entry.LastPublishedAt,
            FirstResponseLatencyMs   = entry.FirstResponseLatencyMs,
            LastLatencyMs            = entry.LastLatencyMs,
            AvgLatencyMs             = entry.AvgLatencyMs,
            LatestImageJson          = entry.LatestImageJson,
            RequestJson              = entry.RequestJson
        };
    }
    
    public void RecordSubscriberCountChanged(string requestId, int subscriberCount)
    {
        if (_streams.TryGetValue(requestId, out var entry))
            entry.SubscriberCount = subscriberCount;
    }

    public void RecordRequestJson(string requestId, string requestJson)
    {
        if (_streams.TryGetValue(requestId, out var entry))
            entry.RequestJson = requestJson;
    }

    public void RecordMessageLatency(string requestId, double latencyMs)
    {
        if (!_streams.TryGetValue(requestId, out var entry)) return;

        entry.LastLatencyMs = latencyMs;
        // Exponential moving average — α=0.1 gives a ~10-sample smoothing window.
        // First sample initialises the average directly to avoid starting from zero.
        entry.AvgLatencyMs = entry.AvgLatencyMs == 0
            ? latencyMs
            : 0.1 * latencyMs + 0.9 * entry.AvgLatencyMs;
    }

    public void RecordFirstResponseLatency(string requestId, double elapsedMs)
    {
        // Write once — reconnections create a new SubscriptionState (LocalImage=null)
        // which would otherwise overwrite the original cold-start measurement with a
        // faster warm reconnect time. The first value is the meaningful one.
        if (_streams.TryGetValue(requestId, out var entry) && entry.FirstResponseLatencyMs == 0)
            entry.FirstResponseLatencyMs = elapsedMs;
    }
    // ── Private helpers ───────────────────────────────────────────────────────

    private double ComputePublishRate()
    {
        var windowStart = DateTime.UtcNow.Ticks - _rateWindow.Ticks;
        var count       = 0;

        // Drain stale entries from the front while counting recent ones.
        // ConcurrentQueue enumeration is snapshot-based — safe without locking.
        foreach (var ticks in _publishTimestampsTicks)
        {
            if (ticks >= windowStart)
                count++;
        }

        // Evict old entries — dequeue from front until within window
        while (_publishTimestampsTicks.TryPeek(out var oldest) && oldest < windowStart)
            _publishTimestampsTicks.TryDequeue(out _);

        return Math.Round(count / _rateWindow.TotalSeconds, 1);
    }

    // ── Inner type — mutable per-stream state ─────────────────────────────────

    /// <summary>
    /// Mutable state for a single stream. Fields written by the publisher
    /// hot path use long for Interlocked compatibility. String fields
    /// (LatestImageJson, State) use volatile semantics via CLR reference
    /// atomicity — no explicit lock needed for single-writer scenarios.
    /// </summary>
    private sealed class StreamEntry
    {
        public required string RequestId  { get; init; }
        public required string StreamName { get; init; }

        public volatile string State = "Streaming";

        public int SubscriberCount;        // updated by monitoring hook
        public long PublishCount;          // Interlocked
        public long SkipCount;             // Interlocked

        public DateTimeOffset OpenedAt        { get; init; }
        public DateTimeOffset LastPublishedAt { get; set; }
        public DateTimeOffset? ClosedAt       { get; set; }

        /// <summary>
        /// Raw JSON string of last published TResponse.
        /// String assignment is atomic on .NET — safe to read without lock.
        /// </summary>
        public volatile string? LatestImageJson;

        /// <summary>
        /// Raw JSON string of the original request. Set once on stream open.
        /// </summary>
        public volatile string? RequestJson;

        /// <summary>
        /// Time from request sent to first response received (ms).
        /// Zero until the first response arrives; set exactly once.
        /// </summary>
        public double FirstResponseLatencyMs;

        /// <summary>Last observed latency in ms. Written on subscriber hot path.</summary>
        public double LastLatencyMs;

        /// <summary>Exponential moving average latency (α=0.1). Updated on each message.</summary>
        public double AvgLatencyMs;
    }
}
