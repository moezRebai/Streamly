// File: Streamly.Core.Runtime/Context/StreamingContextFactory.cs
using System;
using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Publishing;

namespace Streamly.Core.Runtime.Context;

internal class StreamingContextFactory<TRequest, TResponse>(
    IResponsePublisher<TRequest, TResponse> publisher,
    ILoggerFactory loggerFactory)
    : IStreamingContextFactory<TRequest, TResponse>
{
    private readonly IResponsePublisher<TRequest, TResponse> _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
    private readonly ILoggerFactory _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

    public IStreamingContext<TResponse> Create(
        string requestId,
        StreamBehavior streamBehavior) // ← ADDED
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));

        return new StreamingContext<TRequest, TResponse>(
            requestId,
            streamBehavior,
            _publisher,
            _loggerFactory.CreateLogger<StreamingContext<TRequest, TResponse>>());
    }
}