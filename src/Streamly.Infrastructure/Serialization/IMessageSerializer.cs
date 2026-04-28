namespace Streamly.Infrastructure.Interfaces;

/// <summary>
/// Single serialization contract for the Streamly pipeline.
///
/// Two distinct output forms:
///
///   Serialize / Deserialize  — compact camelCase bytes for NATS transport.
///                              Nulls are omitted (WhenWritingNull) to keep
///                              delta payloads small.
///
///   SerializeToJson          — human-readable JSON string for monitoring and
///                              dashboard display. All fields are included
///                              (Never ignore nulls) so the full state is visible.
///                              The dashboard re-pretty-prints on read; storage
///                              stays compact (WriteIndented = false).
/// </summary>
public interface IMessageSerializer
{
    // ── Transport ─────────────────────────────────────────────────────────────

    /// <summary>Serialize to compact camelCase bytes for NATS transport.</summary>
    byte[] Serialize<T>(T obj);

    /// <summary>Deserialize from NATS transport bytes.</summary>
    T Deserialize<T>(byte[] data);

    /// <summary>Deserialize from NATS transport bytes (zero-copy span overload).</summary>
    T Deserialize<T>(ReadOnlySpan<byte> data);

    // ── Display ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Serialize to a compact JSON string for monitoring storage.
    /// Uses PascalCase property names and includes all fields (no null suppression)
    /// so every field is visible in the dashboard regardless of value.
    /// </summary>
    string SerializeToJson<T>(T obj);
}