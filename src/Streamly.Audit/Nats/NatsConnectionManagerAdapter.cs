using NATS.Client.Core;
using Streamly.Infrastructure.Nats;

namespace Streamly.Audit.Nats;

/// <summary>
/// Adapts NatsConnectionManager to IJetStreamConnectionAccessor.
/// Only used server-side (publisher/subscriber) — registered in AddStreamlyAudit().
/// </summary>
internal sealed class NatsConnectionManagerAdapter(NatsConnectionManager manager)
    : IJetStreamConnectionAccessor
{
    public NatsConnection Connection => manager.Connection;
}
