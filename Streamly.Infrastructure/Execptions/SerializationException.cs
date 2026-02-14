namespace Streamly.Infrastructure.Execptions;

/// <summary>
/// Exception thrown when serialization or deserialization fails
/// </summary>
public class SerializationException : Exception
{
    public SerializationException(string message) : base(message) { }
    
    public SerializationException(string message, Exception innerException) 
        : base(message, innerException) { }
}
