using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Monitoring.Endpoints;

/// <summary>
/// GET {prefix}/audit
///   Streams audit records from JetStream for a given time range.
///
/// Required query parameters:
///   streamName  — e.g. "FxSpot"
///   from        — ISO 8601 UTC datetime, e.g. "2026-03-27T00:00:00Z"
///   to          — ISO 8601 UTC datetime, e.g. "2026-03-28T00:00:00Z"
///
/// Optional query parameters:
///   requestId   — filter to a specific stream request
///
/// Response: JSON array of AuditRecord objects, streamed as records arrive.
/// Returns 400 if required parameters are missing or invalid.
/// Returns 501 if IAuditReader is not registered (audit not enabled).
/// </summary>
internal static class AuditEndpoint
{
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static async Task HandleAsync(
        HttpContext context,
        IAuditReader? auditReader)
    {
        var logger = context.RequestServices
            .GetRequiredService<ILoggerFactory>()
            .CreateLogger("Streamly.Monitoring.AuditEndpoint");

        // ── Guard: audit not enabled ──────────────────────────────────────
        if (auditReader is null)
        {
            logger.LogWarning("Audit endpoint called but IAuditReader is not registered.");
            context.Response.StatusCode  = StatusCodes.Status501NotImplemented;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsync(
                "{\"error\":\"Audit is not enabled on this instance. " +
                "Call AddStreamlyAudit() in Program.cs to enable it.\"}");
            return;
        }

        // ── Parse query parameters ────────────────────────────────────────
        var streamName = context.Request.Query["streamName"].FirstOrDefault();
        var fromStr    = context.Request.Query["from"].FirstOrDefault();
        var toStr      = context.Request.Query["to"].FirstOrDefault();
        var requestId  = context.Request.Query["requestId"].FirstOrDefault();

        logger.LogDebug(
            "Audit query received — streamName='{StreamName}' from='{From}' to='{To}' requestId='{RequestId}'",
            streamName, fromStr, toStr, requestId ?? "*");

        if (string.IsNullOrWhiteSpace(streamName))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync(
                "{\"error\":\"'streamName' query parameter is required.\"}");
            return;
        }

        if (!DateTimeOffset.TryParse(fromStr, out var from) ||
            !DateTimeOffset.TryParse(toStr,   out var to))
        {
            logger.LogWarning(
                "Audit query has invalid date range — from='{From}' to='{To}'",
                fromStr, toStr);
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync(
                "{\"error\":\"'from' and 'to' must be valid ISO 8601 datetimes.\"}");
            return;
        }

        if (from >= to)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync(
                "{\"error\":\"'from' must be earlier than 'to'.\"}");
            return;
        }

        // ── Stream results ────────────────────────────────────────────────
        logger.LogInformation(
            "Audit query starting — streamName='{StreamName}' from={From:O} to={To:O} requestId='{RequestId}'",
            streamName, from, to, requestId ?? "*");

        context.Response.ContentType = "application/json";
        context.Response.StatusCode  = StatusCodes.Status200OK;

        var ct    = context.RequestAborted;
        var first = true;
        var count = 0;

        await context.Response.WriteAsync("[", ct);

        await foreach (var record in auditReader.QueryAsync(
            streamName, from, to,
            string.IsNullOrWhiteSpace(requestId) ? null : requestId,
            ct))
        {
            if (!first)
                await context.Response.WriteAsync(",", ct);

            first = false;
            count++;

            logger.LogTrace(
                "Audit record #{Count} — seq={Sequence} requestId='{RequestId}' size={Size}B ts={Timestamp:O}",
                count, record.Sequence, record.RequestId, record.PayloadSize, record.Timestamp);

            await context.Response.WriteAsync(
                JsonSerializer.Serialize(record, _jsonOptions), ct);

            await context.Response.Body.FlushAsync(ct);
        }

        await context.Response.WriteAsync("]", ct);

        logger.LogInformation(
            "Audit query complete — streamName='{StreamName}' returned {Count} record(s)",
            streamName, count);
    }
}