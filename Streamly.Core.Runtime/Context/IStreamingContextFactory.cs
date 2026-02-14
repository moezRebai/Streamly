// File: Streamly.Core.Runtime/Context/IStreamingContextFactory.cs
using Streamly.Core.Abstractions;
using Streamly.Core.Models;

namespace Streamly.Core.Runtime.Context;

internal interface IStreamingContextFactory<TRequest, in TResponse>
{
    /// <summary>
    /// Create a streaming context for a specific request
    /// StreamBehavior determines auto-close behavior
    /// </summary>
    IStreamingContext<TResponse> Create(
        string requestId,
        StreamBehavior streamBehavior);
}