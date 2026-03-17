namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Factory for creating ConfirmationQueue instances per stream.
/// Needed because ConfirmationQueue requires a stream-scoped ILeaderElectionService
/// which cannot be resolved by DI automatically — same reason as IConfirmationPublisherFactory.
/// </summary>
internal interface IConfirmationQueueFactory
{
    Task<ConfirmationQueue> CreateAsync(string streamName);
}
