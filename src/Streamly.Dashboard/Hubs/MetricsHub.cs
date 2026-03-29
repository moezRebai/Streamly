using Microsoft.AspNetCore.SignalR;

namespace Streamly.Dashboard.Hubs;

/// <summary>
/// SignalR hub used exclusively for server-to-client push notifications.
///
/// After each poll cycle, <see cref="Aggregation.MetricsAggregator"/> calls
/// <c>IHubContext&lt;MetricsHub&gt;.Clients.All.SendAsync(MetricsUpdatedMethod)</c>,
/// which causes all connected Blazor components to re-read the (already updated)
/// <see cref="Aggregation.InstanceRegistry"/> and re-render.
///
/// There are no client-to-server methods — clients never push data through this hub.
/// </summary>
public sealed class MetricsHub : Hub
{
    /// <summary>
    /// Name of the server→client method Blazor components listen for.
    /// Components subscribe to this via the JavaScript interop layer embedded in
    /// Blazor's SignalR client, or via <c>HubConnectionBuilder</c> if using
    /// a manual JS connection.
    ///
    /// In practice, Blazor Server pages use
    /// <c>NavigationManager + JS interop</c> OR a simple
    /// <c>HubConnection</c> constructed on the component.
    /// </summary>
    public const string MetricsUpdatedMethod = "MetricsUpdated";

    /// <summary>
    /// Called automatically by SignalR when a browser tab connects.
    /// Logged for diagnostics — no state tracking is needed here because
    /// the aggregator always broadcasts to <c>Clients.All</c>.
    /// </summary>
    public override Task OnConnectedAsync()
    {
        // No-op beyond framework base — connection count tracked by SignalR itself.
        return base.OnConnectedAsync();
    }
}
