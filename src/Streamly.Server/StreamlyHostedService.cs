using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Streamly.Server.Configuration;
using Streamly.Server.Publishing;
using Streamly.Server.RequestManagement;

namespace Streamly.Server;

/// <summary>
/// Hosted service that starts/stops all registered RequestManagers.
/// One RequestManager per registered stream handler.
/// Registered automatically by AddStreamlyServer().
/// </summary>
internal class StreamlyHostedService(
    IServiceProvider serviceProvider,
    StreamlyServerOptions options,
    ILogger<StreamlyHostedService> logger)
    : IHostedService
{
    // Holds the started managers for clean shutdown
    private readonly List<IRequestManager> _startedManagers = [];
    private readonly List<KeepaliveService> _keepaliveServices = [];

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Starting Streamly - {Count} stream(s) registered",
            options.Handlers.Count);

        foreach (var handler in options.Handlers)
        {
            try
            {
                // Resolve IRequestManager<TRequest, TResponse> and upcast to the non-generic base
                var managerType = typeof(IRequestManager<,>)
                    .MakeGenericType(handler.RequestType, handler.ResponseType);

                var manager = (IRequestManager)serviceProvider.GetRequiredService(managerType);

                await manager.StartAsync(cancellationToken);

                _startedManagers.Add(manager);

                // Start keepalive for this stream
                var keepalive = await serviceProvider.GetRequiredService<KeepaliveServiceFactory>()
                    .CreateAsync(handler.StreamName);

                await keepalive.StartAsync();

                _keepaliveServices.Add(keepalive);

                logger.LogInformation(
                    "Started RequestManager for stream '{StreamName}'",
                    handler.StreamName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Failed to start RequestManager for stream '{StreamName}'",
                    handler.StreamName);
                throw;
            }
        }

        logger.LogInformation("Streamly started successfully");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping Streamly...");

        // Stop keepalives first — subscribers will detect silence
        // and begin reconnecting before we close streams
        foreach (var keepalive in _keepaliveServices)
        {
            try { await keepalive.StopAsync(); }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error stopping keepalive service");
            }
        }

        // Stop in reverse order
        foreach (var manager in _startedManagers.AsEnumerable().Reverse())
        {
            try
            {
                await manager.StopAsync(cancellationToken);

                logger.LogInformation(
                    "Stopped RequestManager for stream '{StreamName}'",
                    manager.StreamName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Error stopping RequestManager for stream '{StreamName}'",
                    manager.StreamName);
            }
        }

        logger.LogInformation("Streamly stopped");
    }
}
