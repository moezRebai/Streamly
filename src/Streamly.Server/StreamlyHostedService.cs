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
    private readonly List<(string StreamName, object Manager)> _startedManagers = new();
    private readonly List<KeepaliveService> _keepaliveServices = new();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Starting Streamly - {Count} stream(s) registered",
            options.Handlers.Count);

        foreach (var handler in options.Handlers)
        {
            try
            {
                // Resolve IRequestManager<TRequest, TResponse> for this stream
                var managerType = typeof(IRequestManager<,>)
                    .MakeGenericType(handler.RequestType, handler.ResponseType);

                var manager = serviceProvider.GetRequiredService(managerType);

                // Call StartAsync via reflection (manager is a non-generic reference)
                var startMethod = managerType.GetMethod(nameof(IRequestManager<object, object>.StartAsync))!;
                await (Task)startMethod.Invoke(manager, [cancellationToken])!;

                _startedManagers.Add((handler.StreamName, manager));

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
        foreach (var (streamName, manager) in _startedManagers.AsEnumerable().Reverse())
        {
            try
            {
                var managerType = manager.GetType();
                var stopMethod = managerType.GetMethod("StopAsync")!;
                await (Task)stopMethod.Invoke(manager, [cancellationToken])!;

                logger.LogInformation(
                    "Stopped RequestManager for stream '{StreamName}'",
                    streamName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Error stopping RequestManager for stream '{StreamName}'",
                    streamName);
            }
        }

        logger.LogInformation("Streamly stopped");
    }
}
