namespace Streamly.Core.ChangeDetection;

/// <summary>
/// Result of a diff computation between two responses.
/// </summary>
/// <param name="LatestImage">Full merged state — always stored in registry.</param>
/// <param name="Update">
/// Partial object containing only changed properties — published to NATS.
/// Null means nothing changed; publish should be skipped.
/// </param>
/// <param name="ChangedProperties">
/// Names of properties that are meaningful in <see cref="Update"/>.
/// Null signals a full snapshot (first message or close) — subscriber sets LocalImage directly.
/// </param>
public record ResponseDiff<TResponse>(
    TResponse           LatestImage,
    TResponse?          Update,
    HashSet<string>?    ChangedProperties)
    where TResponse : class;
