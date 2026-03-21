// Hosts a subscriber-only process for BenchmarkDotNet benchmarks.
// The publisher (Streamly.Test.Publisher) must already be running externally.
//
// Launch order:
//   1. nats-server -js
//   2. Streamly.Test.Publisher  (SpotPricer + IrsPricer handlers registered)
//   3. dotnet run -c Release    (this benchmark)

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Streamly;
using Streamly.Client;

namespace Streamly.Benchmarks;

public sealed class BenchmarkHarness : IAsyncDisposable
{
    public const string NatsUrl = "nats://localhost:4222";
    public const string SpotStreamName = "SpotPricer";
    public const string IrsStreamName = "IrsPricer";

    private IHost? _subscriberHost;

    public IStreamingSubscriber<BenchSpotRequest, BenchSpotPrice> Subscriber { get; private set; } = null!;
    public IStreamingSubscriber<IrsRequest, IrsResponse> IrsSubscriber { get; private set; } = null!;

    public async Task StartAsync()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Streamly:NatsUrl"] = NatsUrl,
                ["Streamly:ServiceName"] = $"Bench-{Guid.NewGuid():N}"[..20],
                ["Streamly:SubscriberHeartbeatTimeoutMs"] = "2000",
            })
            .Build();

        _subscriberHost = Host.CreateDefaultBuilder()
            .ConfigureLogging(log => log
                .ClearProviders()
                .AddFilter("Streamly", LogLevel.Warning))
            .ConfigureServices((_, services) =>
            {
                services.AddStreamly(config, options =>
                {
                    options.AddSubscriber<BenchSpotRequest, BenchSpotPrice>(SpotStreamName);
                    options.AddSubscriber<IrsRequest, IrsResponse>(IrsStreamName);
                });
            })
            .Build();

        await _subscriberHost.StartAsync();

        Subscriber = _subscriberHost.Services
            .GetRequiredService<IStreamingSubscriber<BenchSpotRequest, BenchSpotPrice>>();

        IrsSubscriber = _subscriberHost.Services
            .GetRequiredService<IStreamingSubscriber<IrsRequest, IrsResponse>>();

        // Give the subscriber time to connect to NATS and discover
        // both running publishers.
        await Task.Delay(TimeSpan.FromSeconds(3));
    }

    public async Task StopAsync()
    {
        if (_subscriberHost is not null)
            await _subscriberHost.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _subscriberHost?.Dispose();
    }
}