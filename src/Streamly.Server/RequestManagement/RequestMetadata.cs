using Streamly.Core.Models;

namespace Streamly.Server.RequestManagement;

internal class RequestMetadata<TRequest, TResponse>
{
    public string RequestId { get; set; } = string.Empty;
    public TRequest Request { get; set; } = default!;
    public byte[] SerializedRequest { get; set; } = [];
    public RequestState State { get; set; }
    public TResponse? LatestImage { get; set; }
    public int SubscriberCount { get; set; }
    public StreamBehavior StreamBehavior { get; set; } = StreamBehavior.Live;
    public DateTime OpenedAt { get; set; }
    public DateTime? LastUpdateAt { get; set; }
    public long Epoch { get; set; }
    public CancellationTokenSource HandlerCts { get; } = new();
}
