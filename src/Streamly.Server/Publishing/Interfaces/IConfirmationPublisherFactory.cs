namespace Streamly.Server.Publishing.Interfaces;

internal interface IConfirmationPublisherFactory
{
    Task<ConfirmationPublisher> CreateAsync(string streamName);
}
