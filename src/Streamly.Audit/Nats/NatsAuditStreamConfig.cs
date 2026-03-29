using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Streamly.Audit.Nats;

/// <summary>
/// Hosted service that creates the STREAMLY_AUDIT JetStream stream on startup.
/// The operation is idempotent — if the stream already exists with compatible
/// configuration it is left unchanged. If it exists with incompatible
/// configuration (e.g. different retention) a warning is logged but startup
/// continues — the stream is not overwritten.
///
/// Registered by AddStreamlyAudit() as a hosted service so it runs before
/// NatsAuditSink begins draining records.
/// </summary>
internal sealed class NatsAuditStreamConfig(
    IJetStreamConnectionAccessor connectionAccessor,
    AuditOptions options,
    ILogger<NatsAuditStreamConfig> logger)
    : IHostedService
{
    private readonly IJetStreamConnectionAccessor _connectionAccessor = connectionAccessor
                                                                ?? throw new ArgumentNullException(nameof(connectionAccessor));
    private readonly AuditOptions _options = options
                                             ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<NatsAuditStreamConfig> _logger = logger
                                                              ?? throw new ArgumentNullException(nameof(logger));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.CreateStreamOnStartup)
        {
            _logger.LogDebug(
                "CreateStreamOnStartup is false — skipping STREAMLY_AUDIT stream creation");
            return;
        }

        try
        {
            var js = new NatsJSContext(_connectionAccessor.Connection);

            var config = new StreamConfig(
                name: _options.JetStreamName,
                subjects: [$"{_options.SubjectPrefix}.>"])
            {
                Storage    = StreamConfigStorage.File,
                MaxAge     = TimeSpan.FromDays(_options.RetentionDays),
                MaxBytes   = (long)_options.MaxStorageGb * 1024L * 1024L * 1024L,
                Retention  = StreamConfigRetention.Limits,
                Discard    = StreamConfigDiscard.Old,
                NumReplicas = _options.StreamReplicas,
                Compression = StreamConfigCompression.S2
            };

            await js.CreateStreamAsync(config, cancellationToken);

            _logger.LogInformation(
                "NATS JetStream stream '{StreamName}' ready " +
                "(subjects: {Prefix}.>, retention: {Days}d, max: {Gb}GB, replicas: {Replicas})",
                _options.JetStreamName,
                _options.SubjectPrefix,
                _options.RetentionDays,
                _options.MaxStorageGb,
                _options.StreamReplicas);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400)
        {
            // Stream exists with incompatible config — log and continue.
            // Do not overwrite — the operator must reconcile manually.
            _logger.LogWarning(
                "JetStream stream '{StreamName}' already exists with different configuration. " +
                "Audit records will still be written if the stream accepts the subject pattern. " +
                "Error: {Error}",
                _options.JetStreamName,
                ex.Error.Description);
        }
        catch (Exception ex)
        {
            // Non-fatal — service still starts. Audit records will be dropped
            // until JetStream becomes available and the next restart creates the stream.
            _logger.LogError(ex,
                "Failed to create JetStream stream '{StreamName}'. " +
                "Audit records will be dropped until the stream exists.",
                _options.JetStreamName);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
