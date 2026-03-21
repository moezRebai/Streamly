using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Streamly.Client;
using Streamly.Server;

namespace Streamly;

public static class StreamlyServiceCollectionExtensions
{
    /// <summary>
    /// Add Streamly with unified server and/or client components.
    /// Automatically registers server-side handlers, client-side subscribers,
    /// or both, depending on what is configured in StreamlyOptions.
    /// </summary>
    public static IServiceCollection AddStreamly(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<StreamlyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new StreamlyOptions();
        configure(options);

        if (options.HasHandlers)
        {
            services.AddStreamlyServer(configuration, serverOptions =>
            {
                options.ApplyServerOptions(serverOptions);
            });
        }

        if (options.HasSubscribers)
        {
            services.AddStreamlyClient(configuration, clientOptions =>
            {
                options.ApplyClientOptions(clientOptions);
            });
        }

        return services;
    }
}
