using NATS.Client.Core;

namespace Streamly.Audit;

/// <summary>
/// Minimal abstraction over a NATS connection needed for JetStream operations.
///
/// Server-side (publisher/subscriber): implemented via an adapter over
/// NatsConnectionManager, registered automatically by AddStreamlyAudit().
///
/// Dashboard / read-only clients: implemented by a lightweight wrapper that
/// creates a bare NatsConnection from config, registered before calling
/// AddStreamlyAuditReader(). This avoids pulling NatsConnectionManager
/// (and its server-side dependencies) into the dashboard.
/// </summary>
public interface IJetStreamConnectionAccessor
{
    NatsConnection Connection { get; }
}
