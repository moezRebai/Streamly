using NATS.Client.Core;

namespace Streamly.Infrastructure.Nats;

/// <summary>
/// Internal interface allowing NatsLeaderElection to access the underlying NatsConnection
/// without exposing it on the public API of NatsConnectionManager.
/// </summary>
internal interface INatsConnectionAccessor
{
    NatsConnection NatsConnection { get; }
}