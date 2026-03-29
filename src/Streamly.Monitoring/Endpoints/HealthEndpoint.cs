using Microsoft.AspNetCore.Http;
using Streamly.Monitoring.Collectors;

namespace Streamly.Monitoring.Endpoints;

/// <summary>
/// GET /streamly/health
///
/// Returns a minimal JSON object suitable for load-balancer health checks
/// and Kubernetes liveness/readiness probes. Always returns 200 OK while
/// the process is running — the body fields indicate degraded state.
///
/// Response shape:
/// {
///   "status":       "ok" | "degraded",
///   "isLeader":     true | false,
///   "natsConnected": true | false,
///   "instanceId":   "pricing-svc-1"
/// }
/// </summary>
internal static class HealthEndpoint
{
    public static async Task HandleAsync(HttpContext context, InMemoryMetricsCollector collector)
    {
        var snapshot = collector.GetSnapshot();

        var status = snapshot.NatsConnected ? "ok" : "degraded";

        context.Response.ContentType = "application/json";
        context.Response.StatusCode  = StatusCodes.Status200OK;

        await context.Response.WriteAsync(
            $"{{" +
            $"\"status\":\"{status}\"," +
            $"\"isLeader\":{snapshot.IsLeader.ToString().ToLowerInvariant()}," +
            $"\"natsConnected\":{snapshot.NatsConnected.ToString().ToLowerInvariant()}," +
            $"\"instanceId\":\"{snapshot.InstanceId}\"" +
            $"}}");
    }
}
