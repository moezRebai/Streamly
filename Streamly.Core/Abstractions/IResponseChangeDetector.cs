namespace Streamly.Core.Abstractions;

/// <summary>
/// Detects which fields changed between responses
/// Returns set of changed property names for delta updates
/// </summary>
/// <typeparam name="TResponse">Response type</typeparam>
public interface IResponseChangeDetector<in TResponse>
{
    /// <summary>
    /// Detect changes and return changed property names
    /// </summary>
    /// <returns>
    /// Set of changed property names. Empty if no changes.
    /// Properties marked [AlwaysPublish] are included even if unchanged.
    /// </returns>
    HashSet<string> GetChangedProperties(TResponse? lastResponse, TResponse newResponse);
}