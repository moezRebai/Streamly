// File: Streamly.Core.Runtime/Context/StreamingContext.cs

using Microsoft.Extensions.Logging;
using Streamly.Core.Abstractions;
using Streamly.Core.Models;
using Streamly.Core.Runtime.Publishing;

namespace Streamly.Core.Runtime.Context;

/// <summary>
/// Context provided to handlers for publishing responses
/// Aware of StreamBehavior:
///   Live: stays open after publish
///   Snapshot: auto-closes after first publish
/// </summary>
internal class StreamingContext<TRequest, TResponse> : IStreamingContext<TResponse>
{
    private readonly string _requestId;
    private readonly StreamBehavior _streamBehavior;
    private readonly IResponsePublisher<TRequest, TResponse> _publisher;
    private readonly ILogger<StreamingContext<TRequest, TResponse>> _logger;
    
    private bool _published; // Track if already published (for Snapshot)

    public string RequestId => _requestId;
    public StreamBehavior StreamBehavior => _streamBehavior;

    public StreamingContext(
        string requestId,
        StreamBehavior streamBehavior,
        IResponsePublisher<TRequest, TResponse> publisher,
        ILogger<StreamingContext<TRequest, TResponse>> logger)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            throw new ArgumentException("Request ID cannot be null or whitespace", nameof(requestId));

        _requestId = requestId;
        _streamBehavior = streamBehavior;
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task PublishAsync(
        TResponse response,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(response);

        // Snapshot: only allow one publish
        if (_streamBehavior == StreamBehavior.Snapshot && _published)
        {
            _logger.LogWarning(
                "Snapshot request '{RequestId}' already published, ignoring subsequent publish",
                _requestId);
            return;
        }

        _logger.LogTrace(
            "PublishAsync called for request '{RequestId}' (behavior: {Behavior})",
            _requestId,
            _streamBehavior);

        // Publish the response
        await _publisher.PublishAsync(_requestId, response, null,  cancellationToken);

        // Snapshot: auto-close after first publish
        if (_streamBehavior == StreamBehavior.Snapshot)
        {
            _published = true;
            
            _logger.LogInformation(
                "Snapshot request '{RequestId}' published, auto-closing",
                _requestId);

            await _publisher.CloseAsync(_requestId, CloseReason.Normal, cancellationToken);
        }
    }

    public async Task CloseAsync(
        CloseReason reason = CloseReason.Normal,
        CancellationToken cancellationToken = default)
    {
        // Snapshot streams auto-close, no need to call this
        if (_streamBehavior == StreamBehavior.Snapshot)
        {
            _logger.LogWarning(
                "CloseAsync called on Snapshot request '{RequestId}' - auto-close handles this",
                _requestId);
            return;
        }

        _logger.LogInformation(
            "CloseAsync called for Live request '{RequestId}' with reason: {Reason}",
            _requestId,
            reason);

        await _publisher.CloseAsync(_requestId, reason, cancellationToken);
    }
}