using System.Text.Json;
using System.Text.Json.Serialization;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Infrastructure.Serialization;

/// <summary>
/// Base class for custom IMessageSerializer implementations.
///
/// Derive from this class to swap the transport serialization format
/// (e.g. MessagePack, MemoryPack, Protobuf) without losing monitoring
/// support. The three abstract transport methods are the only requirement —
/// SerializeToJson is sealed here because the monitoring dashboard always
/// expects JSON regardless of what format travels over NATS.
///
/// Example — MessagePack transport:
///
///   public class MsgPackSerializer : MessageSerializerBase
///   {
///       public override byte[] Serialize&lt;T&gt;(T obj)
///           => MessagePackSerializer.Serialize(obj);
///
///       public override T Deserialize&lt;T&gt;(byte[] data)
///           => MessagePackSerializer.Deserialize&lt;T&gt;(data);
///
///       public override T Deserialize&lt;T&gt;(ReadOnlySpan&lt;byte&gt; data)
///           => MessagePackSerializer.Deserialize&lt;T&gt;(data);
///   }
///
/// Register before AddStreamlyServer/AddStreamlyClient:
///   services.AddSingleton&lt;IMessageSerializer, MsgPackSerializer&gt;();
/// </summary>
public abstract class MessageSerializerBase : IMessageSerializer
{
    private static readonly JsonSerializerOptions DisplayOptions = new()
    {
        WriteIndented          = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        Converters             = { new JsonStringEnumConverter() }
    };

    // ── Transport — implement these ───────────────────────────────────────────

    /// <summary>Serialize to bytes for NATS transport.</summary>
    public abstract byte[] Serialize<T>(T obj);

    /// <summary>Deserialize from NATS transport bytes.</summary>
    public abstract T Deserialize<T>(byte[] data);

    /// <summary>Deserialize from NATS transport bytes (zero-copy span).</summary>
    public abstract T Deserialize<T>(ReadOnlySpan<byte> data);

    // ── Display — sealed, always JSON ─────────────────────────────────────────

    /// <summary>
    /// Serialize to a compact JSON string for the monitoring dashboard.
    /// Sealed: display is always JSON regardless of the transport format.
    /// </summary>
    public string SerializeToJson<T>(T obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        return JsonSerializer.Serialize(obj, DisplayOptions);
    }
}
