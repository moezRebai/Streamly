// FILE: Streamly.Benchmarks/BenchmarkHarness.cs
//
// The benchmark process only creates a SUBSCRIBER.
// The PUBLISHER (Streamly.Test.Publisher) must already be running externally.
//
// Launch order:
//   1. nats-server -js
//   2. Streamly.Test.Publisher  (dotnet run or from Rider)
//   3. dotnet run -c Release    (this benchmark)
//
// This avoids the BDN child-process isolation problem where hosting two
// IHost instances inside GlobalSetup causes them to be torn down before
// iterations run.

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Streamly.Subscriber;

namespace Streamly.Benchmarks;

public sealed class BenchmarkHarness : IAsyncDisposable
{
    public const string NatsUrl    = "nats://localhost:4222";
    public const string StreamName = "SpotPricer"; // must match the running publisher

    private IHost? _subscriberHost;

    public IStreamingSubscriber<BenchSpotRequest, BenchSpotPrice> Subscriber { get; private set; } = null!;

    public async Task StartAsync()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Streamly:NatsUrl"]      = NatsUrl,
                ["Streamly:ServiceName"] = $"Bench-{Guid.NewGuid():N}"[..20],
            })
            .Build();

        _subscriberHost = Host.CreateDefaultBuilder()
            .ConfigureLogging(log => log
                .ClearProviders()
                .AddFilter("Streamly", LogLevel.Warning))
            .ConfigureServices((_, services) =>
            {
                services.AddStreamlySubscriber(config, options =>
                {
                    options.AddSubscriber<BenchSpotRequest, BenchSpotPrice>(StreamName);
                });
            })
            .Build();

        await _subscriberHost.StartAsync();

        Subscriber = _subscriberHost.Services
            .GetRequiredService<IStreamingSubscriber<BenchSpotRequest, BenchSpotPrice>>();

        // Give subscriber time to connect to NATS and discover the running publisher
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