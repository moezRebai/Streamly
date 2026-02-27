namespace Streamly.Infrastructure.Interfaces;

/// <summary>
/// Serializes objects to bytes for Redis transport.
/// Infrastructure layer - just serialization, no business logic.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serialize an object to bytes
    /// </summary>
    /// <typeparam name="T">Type to serialize</typeparam>
    /// <param name="obj">Object to serialize</param>
    /// <returns>Serialized bytes</returns>
    byte[] Serialize<T>(T obj);
    
    /// <summary>
    /// Deserialize bytes to an object
    /// </summary>
    /// <typeparam name="T">Type to deserialize to</typeparam>
    /// <param name="data">Serialized data</param>
    /// <returns>Deserialized object</returns>
    T Deserialize<T>(byte[] data);
    
    /// <summary>
    /// Deserialize bytes to an object (ReadOnlySpan overload for zero-copy)
    /// </summary>
    /// <typeparam name="T">Type to deserialize to</typeparam>
    /// <param name="data">Serialized data</param>
    /// <returns>Deserialized object</returns>
    T Deserialize<T>(ReadOnlySpan<byte> data);
}
