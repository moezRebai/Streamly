namespace Streamly.Core.Runtime.Publishing;

/// <summary>
/// Factory for creating ConfirmationPublisher instances per stream.
/// Needed because ConfirmationPublisher requires streamName at construction time,
/// which cannot be resolved by DI automatically.
/// </summary>
internal interface IConfirmationPublisherFactory
{
    ConfirmationPublisher Create(string streamName);
}