using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Net;
using Streamly.Core.Abstractions;
using Streamly.Core.Configurations;

namespace Streamly.Infrastructure.Nats;

/// <summary>
///     NATS.Net implementation of <see cref="IStreamingTransport" />.
///     Design decisions:
///     - Uses NATS.Net v2 (NATS.Client.Core) with NatsConnection
///     - Subscriptions run as background tasks (fire-and-forget per message)
///     - Each subject maps to exactly one subscription task (idempotent subscribe)
///     - Raw byte[] transport; serialization handled by IMessageSerializer in the Runtime layer
/// </summary>
public sealed class NatsConnectionManager(
    IOptions<NatsConnectionOptions> options,
    IOptions<StreamlySettings> runtimeOptions,
    ISubjectResolver subjectResolver,
    ILogger<NatsConnectionManager> logger)
    : IStreamingTransport, INatsConnectionAccessor
{
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly NatsConnectionOptions _options = options.Value;

    // subject → active subscription (one NATS loop, fan-out to multiple handlers)
    private readonly ConcurrentDictionary<string, SubjectSubscription> _subscriptions = new();
    private readonly Lock _subscribeLock = new();

    private NatsConnection? _connection;

    private volatile bool _disposed;

    // Internal accessor for NatsLeaderElection (avoids exposing NatsConnection publicly)
    NatsConnection INatsConnectionAccessor.NatsConnection =>
        _connection ?? throw new InvalidOperationException(
            "NATS not connected. Call ConnectAsync() first.");

    /// <summary>
    /// Exposes the underlying NatsConnection for infrastructure components
    /// that need direct JetStream or KV access (NatsAuditSink, NatsAuditReader,
    /// NatsLeaderElection) without requiring a cast to INatsConnectionAccessor.
    /// </summary>
    public NatsConnection Connection =>
        _connection ?? throw new InvalidOperationException(
            "NATS not connected. Call ConnectAsync() first.");
    
    public event Action<Exception>? OnConnectionError;
    public event Action? OnReconnected;

    public bool IsConnected => _connection?.ConnectionState == NatsConnectionState.Open;

    // ── IStreamingTransport ───────────────────────────────────────────────────

    public async Task<long> PublishAsync(string subject, byte[] data, CancellationToken ct = default)
    {
        EnsureConnected();

        await _connection!.PublishAsync(
            subject,
            data,
            cancellationToken: ct).ConfigureAwait(false);

        return 0L;
    }

    public Task SubscribeAsync(string subject, Func<byte[], Task> handler, CancellationToken ct = default)
    {
        EnsureConnected();

        lock (_subscribeLock)
        {
            if (!_subscriptions.TryGetValue(subject, out var sub))
            {
                sub = new SubjectSubscription();
                _subscriptions[subject] = sub;
                _ = RunSubscriptionLoopAsync(subject, sub, sub.Cts.Token);
            }

            sub.AddHandler(handler);
        }

        logger.LogDebug("Subscribed to NATS subject: {Subject}", subject);
        return Task.CompletedTask;
    }

    public Task UnsubscribeAsync(string subject, CancellationToken ct = default)
    {
        if (!_subscriptions.TryRemove(subject, out var sub)) return Task.CompletedTask;
        sub.Cts.Cancel();
        sub.Cts.Dispose();
        logger.LogDebug("Unsubscribed from NATS subject: {Subject}", subject);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Cancel all subscriptions
        foreach (var sub in _subscriptions.Values)
        {
            sub.Cts.Cancel();
            sub.Cts.Dispose();
        }

        _subscriptions.Clear();

        if (_connection is not null) await _connection.DisposeAsync().ConfigureAwait(false);

        _connectionLock.Dispose();
    }

    /// <summary>
    ///     Ensures this InstanceId is not already taken by another running instance.
    ///     Uses NATS KV atomic CreateAsync — fails hard if key already exists.
    ///     TTL ensures the key expires naturally if process crashes.
    /// </summary>
    public async Task EnsureUniqueInstanceAsync(string instanceId, CancellationToken ct = default)
    {
        var nats = ((INatsConnectionAccessor)this).NatsConnection;
        var kvContext = nats.CreateKeyValueStoreContext();

        // Get or create the instances bucket
        INatsKVStore kvStore;
        try
        {
            kvStore = await kvContext.GetStoreAsync(
                subjectResolver.GetInstancesBucketSubject(), ct).ConfigureAwait(false);
        }
        catch (NatsJSApiException)
        {
            kvStore = await kvContext.CreateStoreAsync(
                new NatsKVConfig(subjectResolver.GetInstancesBucketSubject())
                {
                    History = 1,
                    MaxAge = TimeSpan.FromSeconds(30), // auto-expires on crash
                    Storage = NatsKVStorageType.Memory
                }, ct).ConfigureAwait(false);
        }

        // Atomic check — only succeeds if key doesn't exist
        try
        {
            await kvStore.CreateAsync(
                instanceId,
                Encoding.UTF8.GetBytes(Environment.MachineName),
                cancellationToken: ct).ConfigureAwait(false);

            logger.LogInformation(
                "InstanceId '{InstanceId}' verified unique (machine: {Machine})",
                instanceId, Environment.MachineName);
        }
        catch (NatsKVCreateException)
        {
            // Read existing to give helpful error
            var existingMachine = "unknown";
            try
            {
                var entry = await kvStore.GetEntryAsync<string>(instanceId, cancellationToken: ct);
                existingMachine = entry.Value ?? "unknown";
            }
            catch
            {
                /* best effort */
            }

            throw new InvalidOperationException(
                $"InstanceId '{instanceId}' is already in use by another instance " +
                $"on machine '{existingMachine}'. " +
                $"Change 'Streamly:InstanceId' in your configuration.");
        }
    }

    // ── Initialisation ────────────────────────────────────────────────────────

    /// <summary>
    ///     Eagerly establish the NATS connection.
    ///     Called by the DI extension; safe to call multiple times.
    /// </summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        await _connectionLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_connection is { ConnectionState: NatsConnectionState.Open })
                return;

            logger.LogInformation("Connecting to NATS at {Url} as {InstanceId}",
                _options.Url, runtimeOptions.Value.InstanceId);

            var opts = BuildNatsOpts();
            _connection = new NatsConnection(opts);

            // Wire up event handlers (AsyncEventHandler in v2.7.2)
            _connection.ConnectionOpened += async (sender, args) =>
            {
                logger.LogInformation("NATS connection opened: {Message}", args.Message);
                OnReconnected?.Invoke();
                await ValueTask.CompletedTask;
            };

            _connection.ConnectionDisconnected += async (sender, args) =>
            {
                logger.LogWarning("NATS connection disconnected: {Message}", args.Message);
                await ValueTask.CompletedTask;
            };

            _connection.ReconnectFailed += async (sender, args) =>
            {
                logger.LogError("NATS reconnect failed: {Message}", args.Message);
                OnConnectionError?.Invoke(new Exception($"Reconnect failed: {args.Message}"));
                await ValueTask.CompletedTask;
            };

            // In v2.7.2, connection starts automatically on first publish/subscribe
            // But we can force connect immediately to validate configuration
            await _connection.ConnectAsync().ConfigureAwait(false);
            logger.LogInformation("NATS connected successfully");
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private async Task RunSubscriptionLoopAsync(
        string subject,
        SubjectSubscription sub,
        CancellationToken ct)
    {
        // Start drain loop before entering the NATS read loop so no messages are missed.
        var drainTask = RunDrainLoopAsync(subject, sub, ct);

        try
        {
            await foreach (var msg in _connection!
                               .SubscribeAsync<byte[]>(subject, cancellationToken: ct)
                               .ConfigureAwait(false))
            {
                if (msg.Data is null)
                    continue;

                // Non-blocking enqueue — keeps the NATS read loop fast.
                // The drain loop processes handlers sequentially on its own task.
                sub.Writer.TryWrite(msg.Data);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Expected on unsubscribe / shutdown
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "NATS subscription loop faulted for subject {Subject}", subject);
            OnConnectionError?.Invoke(ex);
        }
        finally
        {
            // Signal drain loop that no more messages are coming, then wait for it to finish.
            sub.Writer.TryComplete();
            await drainTask.ConfigureAwait(false);
        }
    }

    private async Task RunDrainLoopAsync(
        string subject,
        SubjectSubscription sub,
        CancellationToken ct)
    {
        try
        {
            await foreach (var data in sub.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                foreach (var handler in sub.GetHandlers())
                {
                    try
                    {
                        await handler(data).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex,
                            "Error in NATS message handler for subject {Subject}", subject);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Expected on unsubscribe / shutdown
        }
    }

    private NatsOpts BuildNatsOpts()
    {
        var opts = NatsOpts.Default with
        {
            Url = _options.Url,
            Name = _options.ConnectionName,
            MaxReconnectRetry = _options.MaxReconnectAttempts,
            ReconnectWaitMin = _options.ReconnectWait, // Start delay
            ReconnectWaitMax = _options.ReconnectWait * 5
        };

        // Authentication if provided
        if (!string.IsNullOrWhiteSpace(_options.Username) &&
            !string.IsNullOrWhiteSpace(_options.Password))
            opts = opts with
            {
                AuthOpts = NatsAuthOpts.Default with
                {
                    Username = _options.Username,
                    Password = _options.Password
                }
            };
        else if (!string.IsNullOrWhiteSpace(_options.CredentialsFile))
            // Load credentials from file
            opts = opts with
            {
                AuthOpts = NatsAuthOpts.Default with
                {
                    CredsFile = _options.CredentialsFile
                }
            };

        return opts;
    }

    private void EnsureConnected()
    {
        if (_connection is null)
            throw new InvalidOperationException(
                "NATS connection not initialised. Call ConnectAsync() first " +
                "or register via AddNatsInfrastructure().");
    }

    // One NATS subscription loop per subject; messages are fanned out to all registered handlers.
    // This allows multiple consumers (e.g. GetSpotPrice and GetIrsPrice RequestManagers) to
    // independently subscribe to shared subjects like streams.subscriber-heartbeat.
    private sealed class SubjectSubscription
    {
        public CancellationTokenSource Cts { get; } = new();
        private readonly List<Func<byte[], Task>> _handlers = [];

        // Unbounded channel: NATS read loop writes (non-blocking),
        // drain loop reads and invokes handlers sequentially.
        // Unbounded avoids backpressure on the NATS read loop while
        // eliminating the per-message Task.Run thread pool pressure.
        private readonly Channel<byte[]> _channel =
            Channel.CreateUnbounded<byte[]>(
                new UnboundedChannelOptions { SingleReader = true });

        public ChannelWriter<byte[]> Writer => _channel.Writer;
        public ChannelReader<byte[]> Reader => _channel.Reader;

        public void AddHandler(Func<byte[], Task> handler)
        {
            lock (_handlers) _handlers.Add(handler);
        }

        public List<Func<byte[], Task>> GetHandlers()
        {
            lock (_handlers) return [.._handlers];
        }
    }
}