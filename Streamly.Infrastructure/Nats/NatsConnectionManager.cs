using System.Collections.Concurrent;
using System.Text;
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
/// NATS.Net implementation of <see cref="IStreamingTransport"/>.
///
/// Design decisions:
///   - Uses NATS.Net v2 (NATS.Client.Core) with NatsConnection
///   - Subscriptions run as background tasks (fire-and-forget per message)
///   - Each subject maps to exactly one subscription task (idempotent subscribe)
///   - Raw byte[] transport; serialization handled by IMessageSerializer in the Runtime layer
/// </summary>
public sealed class NatsConnectionManager(
    IOptions<NatsConnectionOptions> options,
    IOptions<StreamlyRuntimeOptions> runtimeOptions,
    ISubjectResolver subjectResolver,
    ILogger<NatsConnectionManager> logger)
    : IStreamingTransport, INatsConnectionAccessor
{
    private readonly NatsConnectionOptions _options = options.Value;

    private NatsConnection? _connection;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // subject → active subscription cancellation
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _subscriptions = new();

    private volatile bool _disposed;

    public event Action<Exception>? OnConnectionError;
    public event Action? OnReconnected;

    public bool IsConnected => _connection?.ConnectionState == NatsConnectionState.Open;

    // Internal accessor for NatsLeaderElection (avoids exposing NatsConnection publicly)
    NatsConnection INatsConnectionAccessor.NatsConnection =>
        _connection ?? throw new InvalidOperationException(
            "NATS not connected. Call ConnectAsync() first.");

    /// <summary>
    /// Ensures this InstanceId is not already taken by another running instance.
    /// Uses NATS KV atomic CreateAsync — fails hard if key already exists.
    /// TTL ensures the key expires naturally if process crashes.
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
    /// Eagerly establish the NATS connection.
    /// Called by the DI extension; safe to call multiple times.
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

    // ── IStreamingTransport ───────────────────────────────────────────────────

    public async Task<long> PublishAsync(string subject, byte[] data, CancellationToken ct = default)
    {
        EnsureConnected();

        await _connection!.PublishAsync(
            subject: subject,
            data: data,
            cancellationToken: ct).ConfigureAwait(false);
        
        return 0L;
    }

    public Task SubscribeAsync(string subject, Func<byte[], Task> handler, CancellationToken ct = default)
    {
        EnsureConnected();

        // Cancel any existing subscription for this subject
        if (_subscriptions.TryRemove(subject, out var existing))
        {
            existing.Cancel();
            existing.Dispose();
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _subscriptions[subject] = cts;

        // Run the subscription loop as a background task
        _ = RunSubscriptionLoopAsync(subject, handler, cts.Token);

        logger.LogDebug("Subscribed to NATS subject: {Subject}", subject);
        return Task.CompletedTask;
    }

    public Task UnsubscribeAsync(string subject, CancellationToken ct = default)
    {
        if (!_subscriptions.TryRemove(subject, out var cts)) return Task.CompletedTask;
        cts.Cancel();
        cts.Dispose();
        logger.LogDebug("Unsubscribed from NATS subject: {Subject}", subject);
        return Task.CompletedTask;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private async Task RunSubscriptionLoopAsync(
        string subject,
        Func<byte[], Task> handler,
        CancellationToken ct)
    {
        try
        {
            await foreach (var msg in _connection!
                               .SubscribeAsync<byte[]>(subject, cancellationToken: ct)
                               .ConfigureAwait(false))
            {
                if (msg.Data is null)
                    continue;

                // Dispatch without blocking the subscription loop
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await handler(msg.Data).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex,
                            "Error in NATS message handler for subject {Subject}", subject);
                    }
                }, ct);
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
    }

    private NatsOpts BuildNatsOpts()
    {
        var opts = NatsOpts.Default with
        {
            Url = _options.Url,
            Name = _options.ConnectionName,
            MaxReconnectRetry = _options.MaxReconnectAttempts,
            ReconnectWaitMin = _options.ReconnectWait, // Start delay
            ReconnectWaitMax = _options.ReconnectWait * 5,
        };

        // Authentication if provided
        if (!string.IsNullOrWhiteSpace(_options.Username) &&
            !string.IsNullOrWhiteSpace(_options.Password))
        {
            opts = opts with
            {
                AuthOpts = NatsAuthOpts.Default with
                {
                    Username = _options.Username,
                    Password = _options.Password
                }
            };
        }
        else if (!string.IsNullOrWhiteSpace(_options.CredentialsFile))
        {
            // Load credentials from file
            opts = opts with
            {
                AuthOpts = NatsAuthOpts.Default with
                {
                    CredsFile = _options.CredentialsFile
                }
            };
        }

        return opts;
    }

    private void EnsureConnected()
    {
        if (_connection is null)
            throw new InvalidOperationException(
                "NATS connection not initialised. Call ConnectAsync() first " +
                "or register via AddNatsInfrastructure().");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Cancel all subscriptions
        foreach (var cts in _subscriptions.Values)
        {
            cts.Cancel();
            cts.Dispose();
        }

        _subscriptions.Clear();

        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        _connectionLock.Dispose();
    }
}