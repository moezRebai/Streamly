using Streamly.Core.Models;
using Streamly.Subscriber.Models;

namespace Streamly.Subscriber;

public interface IStreamingSubscriber<in TRequest, out TResponse>
{
    /// <summary>
    /// Subscribe to a streaming request.
    /// Returns IObservable - only terminates on normal close or final failure.
    /// Reconnection is transparent - stream resume automatically.
    ///
    /// Usage:
    ///   subscriber.Stream(
    ///       new SpotRequest { CurrencyPair = "EUR/USD" },
    ///       onStatusChanged: status =>
    ///       {
    ///           if (status.State == StreamState.Reconnecting)
    ///               ShowBanner($"Stream lost: {status.Message}");
    ///           else if (status.State == StreamState.Active)
    ///               HideBanner();
    ///           else if (status.State == StreamState.Failed)
    ///               ShowError(status.Message);
    ///       })
    ///   .Subscribe(price => Console.WriteLine(price.Bid));
    /// </summary>
    IObservable<TResponse> Subscribe(
        TRequest request,
        StreamBehavior behavior = StreamBehavior.Live,
        Action<StreamStatus>? onStatusChanged = null);
}