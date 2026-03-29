namespace Streamly.Client.Internal;

/// <summary>
/// A response routed through the shard dispatcher.
/// </summary>
internal record Response(
    string RequestId,
    byte[] Data,
    long Epoch);
