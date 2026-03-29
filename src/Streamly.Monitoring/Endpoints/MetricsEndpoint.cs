using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Streamly.Monitoring.Collectors;
using Streamly.Monitoring.Models;

namespace Streamly.Monitoring.Endpoints;

/// <summary>
/// GET /streamly/metrics
///
/// Returns the full InstanceMetricsSnapshot for this instance.
/// Supports two formats selected via the Accept header:
///
///   Accept: application/json (default)
///     → JSON object matching InstanceMetricsSnapshot
///
///   Accept: text/plain
///     → Prometheus text exposition format (OpenMetrics compatible)
///     → Suitable for direct scraping by a Prometheus server
///
/// The dashboard's MetricsAggregator uses the JSON format.
/// Prometheus scrapers use the text/plain format.
/// </summary>
internal static class MetricsEndpoint
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
        WriteIndented               = false,
        DefaultIgnoreCondition      =
            System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    public static async Task HandleAsync(HttpContext context, InMemoryMetricsCollector collector)
    {
        var snapshot = collector.GetSnapshot();

        var acceptHeader = context.Request.Headers.Accept.ToString();
        var wantsPrometheus = acceptHeader.Contains("text/plain",
            StringComparison.OrdinalIgnoreCase);

        if (wantsPrometheus)
        {
            context.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
            context.Response.StatusCode  = StatusCodes.Status200OK;
            await context.Response.WriteAsync(BuildPrometheusOutput(snapshot));
        }
        else
        {
            context.Response.ContentType = "application/json";
            context.Response.StatusCode  = StatusCodes.Status200OK;
            await context.Response.WriteAsync(
                JsonSerializer.Serialize(snapshot, JsonOptions));
        }
    }

    private static string BuildPrometheusOutput(InstanceMetricsSnapshot s)
    {
        var instance = s.InstanceId;

        return
            $"# HELP streamly_active_streams Currently active publishing streams\n" +
            $"# TYPE streamly_active_streams gauge\n" +
            $"streamly_active_streams{{instance=\"{instance}\"}} {s.ActiveStreams}\n\n" +

            $"# HELP streamly_streams_opened_total Streams opened since start\n" +
            $"# TYPE streamly_streams_opened_total counter\n" +
            $"streamly_streams_opened_total{{instance=\"{instance}\"}} {s.TotalStreamsOpened}\n\n" +

            $"# HELP streamly_streams_closed_total Streams closed since start\n" +
            $"# TYPE streamly_streams_closed_total counter\n" +
            $"streamly_streams_closed_total{{instance=\"{instance}\"}} {s.TotalStreamsClosed}\n\n" +

            $"# HELP streamly_publishes_total Prices published since start\n" +
            $"# TYPE streamly_publishes_total counter\n" +
            $"streamly_publishes_total{{instance=\"{instance}\"}} {s.TotalPublishes}\n\n" +

            $"# HELP streamly_publish_skips_total Publishes suppressed by change detector\n" +
            $"# TYPE streamly_publish_skips_total counter\n" +
            $"streamly_publish_skips_total{{instance=\"{instance}\"}} {s.TotalPublishSkips}\n\n" +

            $"# HELP streamly_publish_errors_total Publish errors since start\n" +
            $"# TYPE streamly_publish_errors_total counter\n" +
            $"streamly_publish_errors_total{{instance=\"{instance}\"}} {s.TotalPublishErrors}\n\n" +

            $"# HELP streamly_publish_rate_per_sec Rolling publish rate (msgs/sec)\n" +
            $"# TYPE streamly_publish_rate_per_sec gauge\n" +
            $"streamly_publish_rate_per_sec{{instance=\"{instance}\"}} {s.PublishRatePerSec}\n\n" +

            $"# HELP streamly_is_leader 1 if this instance is the current leader\n" +
            $"# TYPE streamly_is_leader gauge\n" +
            $"streamly_is_leader{{instance=\"{instance}\"}} {(s.IsLeader ? 1 : 0)}\n\n" +

            $"# HELP streamly_leader_epoch Current leader election epoch\n" +
            $"# TYPE streamly_leader_epoch gauge\n" +
            $"streamly_leader_epoch{{instance=\"{instance}\"}} {s.LeaderEpoch}\n\n" +

            $"# HELP streamly_nats_connected 1 if NATS is connected\n" +
            $"# TYPE streamly_nats_connected gauge\n" +
            $"streamly_nats_connected{{instance=\"{instance}\"}} {(s.NatsConnected ? 1 : 0)}\n\n" +

            $"# HELP streamly_active_subscriptions Currently active subscriber subscriptions\n" +
            $"# TYPE streamly_active_subscriptions gauge\n" +
            $"streamly_active_subscriptions{{instance=\"{instance}\"}} {s.ActiveSubscriptions}\n\n" +

            $"# HELP streamly_messages_received_total Messages received by subscribers\n" +
            $"# TYPE streamly_messages_received_total counter\n" +
            $"streamly_messages_received_total{{instance=\"{instance}\"}} {s.TotalMessagesReceived}\n\n" +

            $"# HELP streamly_watchdog_triggers_total Watchdog heartbeat timeouts\n" +
            $"# TYPE streamly_watchdog_triggers_total counter\n" +
            $"streamly_watchdog_triggers_total{{instance=\"{instance}\"}} {s.TotalWatchdogTriggers}\n\n" +

            $"# HELP streamly_reconnection_attempts_total Subscriber reconnection attempts\n" +
            $"# TYPE streamly_reconnection_attempts_total counter\n" +
            $"streamly_reconnection_attempts_total{{instance=\"{instance}\"}} {s.TotalReconnectionAttempts}\n";
    }
}
