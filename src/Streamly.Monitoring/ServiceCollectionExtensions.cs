using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Streamly.Core.Abstractions;
using Streamly.Monitoring.Collectors;
using Streamly.Monitoring.Endpoints;

namespace Streamly.Monitoring;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the in-process metrics collector and replaces the
    /// NullMetricsCollector default registered by AddStreamlyServer() /
    /// AddStreamlyClient().
    ///
    /// Call this after AddStreamlyServer() / AddStreamlyClient() so that
    /// TryAddSingleton in those methods registers the null default first,
    /// which this method then replaces.
    ///
    /// Usage in Program.cs:
    /// <code>
    /// builder.Services.AddStreamlyServer(configuration, options => { ... });
    /// builder.Services.AddStreamlyMonitoring();          // opt-in
    /// app.MapStreamlyEndpoints();                         // opt-in
    /// </code>
    /// </summary>
    public static IServiceCollection AddStreamlyMonitoring(
        this IServiceCollection services,
        Action<MonitoringOptions>? configure = null)
    {
        var options = new MonitoringOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);

        services.RemoveAll<IStreamlyMetricsCollector>();
        services.AddSingleton<InMemoryMetricsCollector>(sp =>
            new InMemoryMetricsCollector(sp.GetRequiredService<MonitoringOptions>()));
        services.AddSingleton<IStreamlyMetricsCollector>(sp =>
            sp.GetRequiredService<InMemoryMetricsCollector>());

        return services;
    }

    /// <summary>
    /// Maps the Streamly monitoring HTTP endpoints.
    /// Requires UseRouting() to have been called before this in the pipeline.
    ///
    /// Endpoints registered:
    ///   GET {prefix}/health              → liveness / readiness probe
    ///   GET {prefix}/metrics             → full counter snapshot (JSON or Prometheus)
    ///   GET {prefix}/streams             → live stream table
    ///   GET {prefix}/streams/{requestId} → single stream detail
    ///   GET {prefix}/audit               → audit record query (only if IAuditReader registered)
    ///
    /// The audit endpoint is registered unconditionally but returns HTTP 501
    /// if IAuditReader is not in the DI container — so callers don't need to
    /// conditionally call MapStreamlyEndpoints() based on audit being enabled.
    ///
    /// The prefix defaults to "/streamly" and is configurable via MonitoringOptions.
    /// </summary>
    public static IEndpointRouteBuilder MapStreamlyEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        var options   = endpoints.ServiceProvider.GetRequiredService<MonitoringOptions>();
        var collector = endpoints.ServiceProvider.GetRequiredService<InMemoryMetricsCollector>();
        var prefix    = options.RoutePrefix.TrimEnd('/');

        endpoints.MapGet($"{prefix}/health",
            (HttpContext ctx) => HealthEndpoint.HandleAsync(ctx, collector));

        endpoints.MapGet($"{prefix}/metrics",
            (HttpContext ctx) => MetricsEndpoint.HandleAsync(ctx, collector));

        endpoints.MapGet($"{prefix}/streams",
            (HttpContext ctx) => StreamsEndpoint.HandleListAsync(ctx, collector));

        endpoints.MapGet($"{prefix}/streams/{{requestId}}",
            (HttpContext ctx, string requestId) =>
                StreamsEndpoint.HandleSingleAsync(ctx, collector, requestId));

        // Audit endpoint — always registered, returns 501 if IAuditReader not available.
        // This keeps MapStreamlyEndpoints() unconditional — callers never need to
        // check whether audit is enabled before calling it.
        endpoints.MapGet($"{prefix}/audit", (HttpContext ctx) =>
        {
            var auditReader = ctx.RequestServices
                .GetService<IAuditReader>(); // null if AddStreamlyAudit() not called
            return AuditEndpoint.HandleAsync(ctx, auditReader);
        });

        return endpoints;
    }
}