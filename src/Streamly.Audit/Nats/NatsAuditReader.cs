using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Audit.Nats;

/// <summary>
/// NATS JetStream implementation of <see cref="IAuditReader"/>.
/// Uses ephemeral ordered consumers — no durable name, no ack required,
/// server-side time-range or sequence-based seek.
/// requestId filtering uses the X-Streamly-RequestId header written by
/// NatsAuditSink — no payload deserialization needed.
/// </summary>
internal sealed class NatsAuditReader(
    IJetStreamConnectionAccessor connectionAccessor,
    AuditOptions options,
    ILogger<NatsAuditReader> logger)
    : IAuditReader
{
    private readonly IJetStreamConnectionAccessor _connectionAccessor = connectionAccessor
        ?? throw new ArgumentNullException(nameof(connectionAccessor));
    private readonly AuditOptions _options = options
        ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<NatsAuditReader> _logger = logger
        ?? throw new ArgumentNullException(nameof(logger));

    public async IAsyncEnumerable<AuditRecord> QueryAsync(
        string streamName,
        DateTimeOffset from,
        DateTimeOffset to,
        string? requestId = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamName);

        if (from >= to)
            throw new ArgumentException(
                $"'from' ({from:O}) must be earlier than 'to' ({to:O}).");

        var subject = $"{_options.SubjectPrefix}.{streamName}";
        var js      = new NatsJSContext(_connectionAccessor.Connection);

        _logger.LogDebug(
            "Audit query: stream='{StreamName}' from={From:O} to={To:O} requestId='{RequestId}'",
            streamName, from, to, requestId ?? "*");

        var stream = await js.GetStreamAsync(_options.JetStreamName, cancellationToken: ct);

        var consumerOpts = new NatsJSOrderedConsumerOpts
        {
            FilterSubjects = [subject],
            DeliverPolicy  = ConsumerConfigDeliverPolicy.ByStartTime,
            OptStartTime   = from.UtcDateTime,
            ReplayPolicy   = ConsumerConfigReplayPolicy.Instant,
            InactiveThreshold = TimeSpan.FromSeconds(30)
        };

        var consumer = await stream.CreateOrderedConsumerAsync(consumerOpts, ct);
        var count    = 0;

        await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct))
        {
            var pastTo     = msg.Metadata?.Timestamp > to;
            var noPending  = msg.Metadata?.NumPending == 0;

            if (!pastTo)
            {
                var record = BuildRecord(msg, streamName);

                if (requestId is null ||
                    string.Equals(record.RequestId, requestId, StringComparison.Ordinal))
                {
                    count++;
                    yield return record;
                }
            }

            if (pastTo || noPending)
                break;
        }

        _logger.LogDebug(
            "Audit query complete: {Count} records for stream '{StreamName}'",
            count, streamName);
    }

    public async IAsyncEnumerable<AuditRecord> ReplayFromAsync(
        string streamName,
        ulong fromSequence,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamName);

        var subject = $"{_options.SubjectPrefix}.{streamName}";
        var js      = new NatsJSContext(_connectionAccessor.Connection);

        _logger.LogDebug(
            "Audit replay: stream='{StreamName}' fromSequence={Sequence}",
            streamName, fromSequence);

        var stream = await js.GetStreamAsync(_options.JetStreamName, cancellationToken: ct);

        // ReplayFromAsync uses NatsJSOrderedConsumerOpts — same type as QueryAsync.
        // OptStartSeq drives the sequence-based seek.
        var consumerOpts = new NatsJSOrderedConsumerOpts
        {
            FilterSubjects = [subject],
            DeliverPolicy  = ConsumerConfigDeliverPolicy.ByStartSequence,
            OptStartSeq    = fromSequence,
            ReplayPolicy   = ConsumerConfigReplayPolicy.Instant,
            InactiveThreshold = TimeSpan.FromSeconds(30)
        };

        var consumer = await stream.CreateOrderedConsumerAsync(consumerOpts, ct);

        await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct))
        {
            yield return BuildRecord(msg, streamName);
        }
    }

    private static AuditRecord BuildRecord(INatsJSMsg<byte[]> msg, string streamName)
    {
        var payload = msg.Data ?? [];

        return new AuditRecord
        {
            RequestId   = ExtractHeader(msg, "X-Streamly-RequestId"),
            StreamName  = streamName,
            Timestamp   = msg.Metadata?.Timestamp ?? DateTimeOffset.UtcNow,
            Sequence    = msg.Metadata?.Sequence.Stream ?? 0,
            PayloadSize = payload.Length,
            Payload     = payload
        };
    }

    private static string ExtractHeader(INatsJSMsg<byte[]> msg, string headerName)
    {
        if (msg.Headers is not null &&
            msg.Headers.TryGetValue(headerName, out var values))
            return values.FirstOrDefault() ?? string.Empty;

        return string.Empty;
    }
}