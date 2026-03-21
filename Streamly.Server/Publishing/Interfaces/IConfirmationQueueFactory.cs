namespace Streamly.Server.Publishing.Interfaces;

internal interface IConfirmationQueueFactory
{
    Task<ConfirmationQueue> CreateAsync(string streamName);
}
