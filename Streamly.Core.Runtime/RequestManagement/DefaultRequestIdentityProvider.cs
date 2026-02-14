// File: Streamly.Core.Runtime/RequestManagement/DefaultRequestIdentityProvider.cs
using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Core.Runtime.RequestManagement;

/// <summary>
/// Default implementation using JSON serialization + SHA256 hashing
/// Includes StreamBehavior in hash to differentiate
/// Live vs Snapshot for same request payload
/// </summary>
public class DefaultRequestIdentityProvider<TRequest> : IRequestIdentityProvider<TRequest>
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never
    };

    public string ComputeRequestId(TRequest request, StreamBehavior behavior)
    {
        if (request == null)
            throw new ArgumentNullException(nameof(request));

        // Create composite object including behavior
        // This ensures Live and Snapshot have different IDs
        var composite = new
        {
            Request = request,
            Behavior = behavior.ToString() // "Live" or "Snapshot"
        };

        var json = JsonSerializer.Serialize(composite, SerializerOptions);
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(json));
        var hashHex = Convert.ToHexString(hashBytes).ToLowerInvariant();

        return $"req-{hashHex}";
    }

    public byte[] Serialize(TRequest request)
    {
        if (request == null)
            throw new ArgumentNullException(nameof(request));

        return JsonSerializer.SerializeToUtf8Bytes(request, SerializerOptions);
    }

    public TRequest Deserialize(ReadOnlySpan<byte> data)
    {
        return JsonSerializer.Deserialize<TRequest>(data, SerializerOptions)
            ?? throw new InvalidOperationException("Deserialization resulted in null");
    }
}