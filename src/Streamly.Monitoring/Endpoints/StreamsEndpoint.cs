using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Streamly.Monitoring.Collectors;
using Streamly.Monitoring.Models;

namespace Streamly.Monitoring.Endpoints;

/// <summary>
/// GET /streamly/streams
///   Returns a JSON array of all active StreamDetailRecord entries.
///   Optional query param: ?streamName=FxSpot filters by stream name.
///
/// GET /streamly/streams/{requestId}
///   Returns a single StreamDetailRecord for the given requestId.
///   Returns 404 if not found or past the retention window.
///
/// The LatestImageJson field is embedded as a raw JSON value (not a string)
/// in the response — it is injected directly into the output buffer without
/// re-serialization so the response body contains clean, readable JSON.
/// </summary>
internal static class StreamsEndpoint
{
    // ── GET /streamly/streams ─────────────────────────────────────────────────

    public static async Task HandleListAsync(
        HttpContext context,
        InMemoryMetricsCollector collector)
    {
        var streamNameFilter = context.Request.Query["streamName"].FirstOrDefault();
        var includeContent   = !string.Equals(
            context.Request.Query["content"].FirstOrDefault(),
            "false",
            StringComparison.OrdinalIgnoreCase);

        var records = collector.GetStreams(streamNameFilter);

        context.Response.ContentType = "application/json";
        context.Response.StatusCode  = StatusCodes.Status200OK;

        await WriteStreamArrayAsync(context.Response, records, includeContent);
    }

    // ── GET /streamly/streams/{requestId} ─────────────────────────────────────

    public static async Task HandleSingleAsync(
        HttpContext context,
        InMemoryMetricsCollector collector,
        string requestId)
    {
        var record = collector.GetStream(requestId);

        if (record is null)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        context.Response.ContentType = "application/json";
        context.Response.StatusCode  = StatusCodes.Status200OK;

        await WriteStreamRecordAsync(context.Response, record);
    }

    // ── Serialization helpers ─────────────────────────────────────────────────

    /// <summary>
    /// Writes the stream array directly to the response body.
    /// LatestImageJson is injected as a raw JSON value — not double-encoded
    /// as a string — so the response is clean domain JSON, not escaped JSON-in-JSON.
    /// </summary>
    private static async Task WriteStreamArrayAsync(
        HttpResponse response,
        IReadOnlyList<StreamDetailRecord> records,
        bool includeContent = true)
    {
        var sb = new StringBuilder("[");

        for (var i = 0; i < records.Count; i++)
        {
            AppendRecord(sb, records[i], includeContent);
            if (i < records.Count - 1)
                sb.Append(',');
        }

        sb.Append(']');
        await response.WriteAsync(sb.ToString());
    }

    private static async Task WriteStreamRecordAsync(
        HttpResponse response,
        StreamDetailRecord record)
    {
        var sb = new StringBuilder();
        AppendRecord(sb, record);
        await response.WriteAsync(sb.ToString());
    }

    /// <summary>
    /// Appends a single StreamDetailRecord as JSON to the StringBuilder.
    /// LatestImageJson is written as a raw JSON value using
    /// <c>"latestImage": {rawJson}</c> — not as a string literal.
    /// If LatestImageJson is null the field is omitted.
    /// </summary>
    private static void AppendRecord(StringBuilder sb, StreamDetailRecord r, bool includeContent = true)
    {
        sb.Append('{');
        sb.Append($"\"requestId\":\"{EscapeJson(r.RequestId)}\",");
        sb.Append($"\"streamName\":\"{EscapeJson(r.StreamName)}\",");
        sb.Append($"\"state\":\"{EscapeJson(r.State)}\",");
        sb.Append($"\"subscriberCount\":{r.SubscriberCount},");
        sb.Append($"\"publishCount\":{r.PublishCount},");
        sb.Append($"\"skipCount\":{r.SkipCount},");
        sb.Append($"\"openedAt\":\"{r.OpenedAt:O}\",");
        sb.Append($"\"lastPublishedAt\":\"{r.LastPublishedAt:O}\",");
        sb.Append($"\"firstResponseLatencyMs\":{r.FirstResponseLatencyMs},");
        sb.Append($"\"lastLatencyMs\":{r.LastLatencyMs},");
        sb.Append($"\"avgLatencyMs\":{r.AvgLatencyMs}");

        if (includeContent)
        {
            if (r.LatestImageJson is not null)
            {
                sb.Append(",\"latestImageJson\":");
                sb.Append(JsonSerializer.Serialize(r.LatestImageJson));
            }

            if (r.RequestJson is not null)
            {
                sb.Append(",\"requestJson\":");
                sb.Append(JsonSerializer.Serialize(r.RequestJson));
            }
        }

        sb.Append('}');
    }

    /// <summary>
    /// Minimal JSON string escaping for known-safe identifiers.
    /// requestId and streamName are internal identifiers — they should
    /// never contain control characters, but we escape backslash and
    /// double-quote defensively.
    /// </summary>
    private static string EscapeJson(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
