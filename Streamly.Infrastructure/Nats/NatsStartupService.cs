using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamly.Core.Configurations;

namespace Streamly.Infrastructure.Nats;

/// <summary>
///     Hosted service that initializes NATS connection and optionally starts leader election.
///     For Publishers: ILeaderElection is required and leadership is attempted on startup.
///     For Subscribers: ILeaderElection is optional (null) - subscribers don't need leadership.
/// </summary>
internal sealed class NatsStartupService(
    NatsConnectionManager connectionManager,
    IOptions<StreamlySettings> runtimeOptions,
    ILogger<NatsStartupService> logger)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initialising NATS infrastructure...");

        // Just connect to NATS
        await connectionManager.ConnectAsync(cancellationToken);

        // Hard fail if InstanceId already taken by another running instance
        await connectionManager.EnsureUniqueInstanceAsync(
            runtimeOptions.Value.InstanceId, cancellationToken);

        logger.LogInformation("NATS infrastructure ready");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Shutting down NATS infrastructure...");
        await connectionManager.DisposeAsync();
    }
}