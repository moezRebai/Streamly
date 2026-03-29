using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Streamly.Audit.Nats;
using Streamly.Core.Abstractions;
using Streamly.Infrastructure.Nats;

namespace Streamly.Audit;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the NATS JetStream audit sink and replaces the NullAuditSink
    /// default registered by AddStreamlyServer().
    ///
    /// Registers two hosted services:
    ///   1. NatsAuditStreamConfig — creates STREAMLY_AUDIT stream on startup (idempotent)
    ///   2. NatsAuditSink         — background drain loop, flushes on graceful shutdown
    ///
    /// Also registers IAuditReader so the same service can both write and read
    /// audit records (e.g. a dashboard co-located with the publisher).
    /// For services that only need to read, use AddStreamlyAuditReader() instead.
    ///
    /// Call after AddStreamlyServer():
    /// <code>
    /// builder.Services.AddStreamlyServer(configuration, options => { ... });
    /// builder.Services.AddStreamlyAudit();   // opt-in
    /// </code>
    /// </summary>
    public static IServiceCollection AddStreamlyAudit(
        this IServiceCollection services,
        Action<AuditOptions>? configure = null)
    {
        var options = new AuditOptions();
        configure?.Invoke(options);

        // Register options as singleton shared by sink, reader, and stream config
        services.TryAddSingleton(options);

        // Adapt NatsConnectionManager → IJetStreamConnectionAccessor so that
        // NatsAuditReader and NatsAuditStreamConfig do not depend on the concrete manager.
        services.TryAddSingleton<IJetStreamConnectionAccessor>(sp =>
            new NatsConnectionManagerAdapter(sp.GetRequiredService<NatsConnectionManager>()));

        // Replace NullAuditSink with the real NATS implementation.
        // AddStreamlyServer uses TryAddSingleton so RemoveAll + AddSingleton
        // is required — a second TryAddSingleton would be a no-op.
        services.RemoveAll<IAuditSink>();
        services.AddSingleton<NatsAuditSink>(sp => new NatsAuditSink(
            sp.GetRequiredService<NatsConnectionManager>(),
            sp.GetRequiredService<AuditOptions>(),
            sp.GetRequiredService<ILogger<NatsAuditSink>>()));

        // Register as IAuditSink so ResponsePublisher resolves the real sink
        services.AddSingleton<IAuditSink>(sp =>
            sp.GetRequiredService<NatsAuditSink>());

        // Register drain loop as hosted service — starts/stops with the application
        services.AddHostedService(sp =>
            sp.GetRequiredService<NatsAuditSink>());

        // Register stream creation as hosted service — runs before sink starts draining
        services.AddHostedService<NatsAuditStreamConfig>();

        // Register reader for services that co-locate reading with writing
        services.TryAddSingleton<IAuditReader, NatsAuditReader>();

        return services;
    }

    /// <summary>
    /// Registers only the audit reader — for services that query audit records
    /// but do not publish them (e.g. the dashboard, a support tool).
    ///
    /// Does NOT register NatsAuditSink or the stream creation hosted service.
    /// The STREAMLY_AUDIT stream must already exist (created by a publisher service
    /// that called AddStreamlyAudit()).
    ///
    /// The caller must register <see cref="IJetStreamConnectionAccessor"/> before
    /// calling this method. For the dashboard, register DashboardNatsConnection:
    /// <code>
    /// builder.Services.AddSingleton&lt;IJetStreamConnectionAccessor, DashboardNatsConnection&gt;();
    /// builder.Services.AddStreamlyAuditReader(options => { ... });
    /// </code>
    /// </summary>
    public static IServiceCollection AddStreamlyAuditReader(
        this IServiceCollection services,
        Action<AuditOptions>? configure = null)
    {
        var options = new AuditOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IAuditReader, NatsAuditReader>();

        return services;
    }
}
