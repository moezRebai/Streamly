using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using Streamly.Dashboard.Hubs;
using Streamly.Monitoring.Models;

namespace Streamly.Dashboard.Aggregation;

/// <summary>
/// Background service that polls every configured Streamly instance on a fixed interval,
/// updates <see cref="InstanceRegistry"/>, then pushes a "metrics updated" notification
/// to all connected browsers via <see cref="MetricsHub"/>.
///
/// Each poll cycle fans out over all instances in parallel so that a single slow instance
/// does not delay updates for healthy ones.
/// </summary>
public sealed class MetricsAggregator(
    IOptions<DashboardOptions> options,
    InstanceRegistry registry,
    IHttpClientFactory httpFactory,
    IHubContext<MetricsHub> hub,
    ILogger<MetricsAggregator> log)
    : BackgroundService
{
    private readonly DashboardOptions          _options = options.Value;

    // Monitoring path prefix — matches Streamly.Monitoring default
    private const string MonitoringPrefix = "/streamly";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Seed the registry with all configured instances before first poll
        registry.Initialise(_options.Instances);

        log.LogInformation(
            "MetricsAggregator starting — {Count} instance(s), poll interval {IntervalMs}ms",
            _options.Instances.Count,
            _options.PollIntervalMs);

        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(_options.PollIntervalMs));

        while (!stoppingToken.IsCancellationRequested &&
               await timer.WaitForNextTickAsync(stoppingToken))
        {
            await PollAllInstancesAsync(stoppingToken);

            // Notify all connected browser sessions that fresh data is available.
            // Components re-read from the registry on receiving this signal.
            await hub.Clients.All.SendAsync(
                MetricsHub.MetricsUpdatedMethod,
                cancellationToken: stoppingToken);
        }

        log.LogInformation("MetricsAggregator stopped.");
    }

    // ── Private helpers ────────────────────────────────────────────────────

    private async Task PollAllInstancesAsync(CancellationToken ct)
    {
        // Fan out — do not await each instance sequentially
        var tasks = _options.Instances.Select(cfg => PollInstanceAsync(cfg, ct));
        await Task.WhenAll(tasks);
    }

    private async Task PollInstanceAsync(InstanceConfig cfg, CancellationToken ct)
    {
        var state = registry.GetOrCreate(cfg.Id, cfg);
        state.LastPolledAt = DateTimeOffset.UtcNow;

        // Use a named HttpClient registered per instance (see Program.cs)
        var client = httpFactory.CreateClient(HttpClientName(cfg.Id));

        try
        {
            // Run both requests concurrently per instance
            var metricsTask = FetchMetricsAsync(client, ct);
            var streamsTask  = FetchStreamsAsync(client, ct);
            await Task.WhenAll(metricsTask, streamsTask);

            state.Metrics          = await metricsTask;
            state.Streams          = await streamsTask ?? [];
            state.IsReachable      = true;
            state.LastErrorMessage = null;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
        {
            state.IsReachable      = false;
            state.Metrics          = null;  // keep Streams so UI can show stale data with a warning
            state.LastErrorMessage = ex.Message;

            log.LogDebug(
                "Instance {Id} unreachable: {Error}",
                cfg.Id, ex.Message);
        }
        catch (Exception ex)
        {
            state.IsReachable      = false;
            state.LastErrorMessage = ex.Message;

            log.LogWarning(ex,
                "Unexpected error polling instance {Id}", cfg.Id);
        }
    }

    private static async Task<InstanceMetricsSnapshot?> FetchMetricsAsync(
        HttpClient client, CancellationToken ct)
    {
        var response = await client.GetAsync(
            $"{MonitoringPrefix}/metrics", ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<InstanceMetricsSnapshot>(ct);
    }

    private static async Task<StreamDetailRecord[]?> FetchStreamsAsync(
        HttpClient client, CancellationToken ct)
    {
        // Omit LatestImageJson / RequestJson from the background poll — the list page
        // only needs metadata. Full content is fetched on demand via the single-stream endpoint.
        var response = await client.GetAsync(
            $"{MonitoringPrefix}/streams?content=false", ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<StreamDetailRecord[]>(ct);
    }

    /// <summary>
    /// Convention: named HttpClient key = "instance-{id}".
    /// Registered in Program.cs via AddHttpClient(name, cfg => ...).
    /// </summary>
    public static string HttpClientName(string instanceId) => $"instance-{instanceId}";
}
