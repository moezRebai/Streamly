using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Streamly.Infrastructure.Interfaces;

namespace Streamly.Infrastructure.Redis;

/// <summary>
/// Extension methods for registering Redis infrastructure services.
/// Infrastructure layer - just DI registration, no business logic.
/// </summary>
public static class RedisInfrastructureExtensions
{
    /// <summary>
    /// Add Redis infrastructure services to the service collection
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="configure">Optional configuration action</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddRedisInfrastructure(
        this IServiceCollection services,
        Action<RedisConnectionOptions>? configure = null)
    {
        // Register configuration
        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<RedisConnectionOptions>();
        }
        
        // Register infrastructure services as singletons
        services.TryAddSingleton<IMessageSerializer, MessageSerializer>();
        services.TryAddSingleton<IRedisConnectionManager, RedisConnectionManager>();
        
        return services;
    }
    
    /// <summary>
    /// Add Redis infrastructure with connection string
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="connectionString">Redis connection string</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddRedisInfrastructure(
        this IServiceCollection services,
        string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionString));
        
        return services.AddRedisInfrastructure(options =>
        {
            options.ConnectionString = connectionString;
        });
    }
}
