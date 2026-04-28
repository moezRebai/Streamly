using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Streamly.Infrastructure.Exceptions;

namespace Streamly.Infrastructure.Serialization;

/// <summary>
///     Default JSON-based message serializer using System.Text.Json.
///     Extend <see cref="MessageSerializerBase"/> to swap the transport format.
/// </summary>
public class DefaultMessageSerializer(ILogger<DefaultMessageSerializer> logger) : MessageSerializerBase
{
    private readonly JsonSerializerOptions _transportOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented               = false,
        Converters                  = { new JsonStringEnumConverter() }
    };

    public override byte[] Serialize<T>(T obj)
    {
        if (obj == null) throw new ArgumentNullException(nameof(obj));
        try
        {
            return JsonSerializer.SerializeToUtf8Bytes(obj, _transportOptions);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to serialize object of type {Type}", typeof(T).Name);
            throw new SerializationException($"Failed to serialize {typeof(T).Name}", ex);
        }
    }

    public override T Deserialize<T>(byte[] data)
    {
        if (data == null || data.Length == 0)
            throw new ArgumentException("Data cannot be null or empty", nameof(data));
        try
        {
            var result = JsonSerializer.Deserialize<T>(data, _transportOptions);
            if (result == null)
                throw new SerializationException($"Deserialization resulted in null for type {typeof(T).Name}");
            return result;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to deserialize to type {Type}", typeof(T).Name);
            throw new SerializationException($"Failed to deserialize {typeof(T).Name}", ex);
        }
    }

    public override T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            throw new ArgumentException("Data cannot be empty", nameof(data));
        try
        {
            var result = JsonSerializer.Deserialize<T>(data, _transportOptions);
            if (result == null)
                throw new SerializationException($"Deserialization resulted in null for type {typeof(T).Name}");
            return result;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to deserialize to type {Type}", typeof(T).Name);
            throw new SerializationException($"Failed to deserialize {typeof(T).Name}", ex);
        }
    }
}