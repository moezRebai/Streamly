using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Streamly.Infrastructure.Execptions;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Infrastructure.Serialization;

/// <summary>
/// JSON-based message serializer using System.Text.Json.
/// Infrastructure layer - just serialization, no business logic.
/// </summary>
public class MessageSerializer(ILogger<MessageSerializer> logger) : IMessageSerializer
{
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,  // Compact for network transport
        Converters =
        {
            new JsonStringEnumConverter()  // Enums as strings
        }
    };

    // Compact for network transport
    // Enums as strings

    public byte[] Serialize<T>(T obj)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));
        
        try
        {
            return JsonSerializer.SerializeToUtf8Bytes(obj, _jsonOptions);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to serialize object of type {Type}", typeof(T).Name);
            throw new SerializationException($"Failed to serialize {typeof(T).Name}", ex);
        }
    }
    
    public T Deserialize<T>(byte[] data)
    {
        if (data == null || data.Length == 0)
            throw new ArgumentException("Data cannot be null or empty", nameof(data));
        
        try
        {
            var result = JsonSerializer.Deserialize<T>(data, _jsonOptions);
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
    
    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            throw new ArgumentException("Data cannot be empty", nameof(data));
        
        try
        {
            var result = JsonSerializer.Deserialize<T>(data, _jsonOptions);
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