namespace Streamly.Server.RequestManagement;

internal interface IRequestRegistry<TRequest, TResponse>
{
    bool TryAdd(string requestId, RequestMetadata<TRequest, TResponse> metadata);
    bool TryGet(string requestId, out RequestMetadata<TRequest, TResponse>? metadata);
    bool TryUpdate(string requestId, Action<RequestMetadata<TRequest, TResponse>> updater);
    bool TryRemove(string requestId, out RequestMetadata<TRequest, TResponse>? metadata);
    IEnumerable<string> GetAllRequestIds();
    IEnumerable<RequestMetadata<TRequest, TResponse>> GetAll();
    int Count { get; }
    bool Contains(string requestId);
    void Clear();
}
