using Streamly.Core.ChangeDetection;

namespace Streamly.Core.Abstractions;

/// <summary>
/// Computes the difference between two responses and merges deltas onto a local image.
/// Publisher side uses <see cref="Compute"/> to produce the delta sent over NATS.
/// Subscriber side uses <see cref="Merge"/> to apply an incoming delta onto its local image.
/// </summary>
public interface IResponseDiffComputer<TResponse> where TResponse : class
{
    /// <summary>
    /// Compare <paramref name="currentImage"/> with <paramref name="newResponse"/> and return the diff.
    /// When <paramref name="currentImage"/> is null (first message) the full response is returned as a snapshot.
    /// </summary>
    ResponseDiff<TResponse> Compute(TResponse? currentImage, TResponse newResponse);

    /// <summary>
    /// Apply <paramref name="delta"/> onto <paramref name="localImage"/> using
    /// <paramref name="changedProperties"/> as the mask of which fields to copy.
    /// </summary>
    TResponse Merge(TResponse localImage, TResponse delta, IReadOnlySet<string> changedProperties);
}
