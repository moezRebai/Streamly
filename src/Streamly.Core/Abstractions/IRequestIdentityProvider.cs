using Streamly.Core.Models;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Computes unique identity for requests
/// Enables deduplication and stream sharing
/// </summary>
public interface IRequestIdentityProvider<TRequest>
{
    /// <summary>
    /// Compute deterministic RequestId
    /// Includes StreamBehavior so Live and Snapshot
    /// of same request have different IDs
    /// </summary>
    string ComputeRequestId(TRequest request, StreamBehavior behavior);
    
    /// <summary>
    /// Serialize request for Redis transport and batch sync recovery
    /// </summary>
    byte[] Serialize(TRequest request);
    
    /// <summary>
    /// Deserialize request from Redis
    /// </summary>
    TRequest Deserialize(ReadOnlySpan<byte> data);
}