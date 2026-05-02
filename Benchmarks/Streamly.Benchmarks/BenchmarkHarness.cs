// Hosts a subscriber-only process for BenchmarkDotNet benchmarks.
// The publisher (Streamly.Test.Publisher) must already be running externally.
//
// Launch order:
//   1. nats-server -js
//   2. Streamly.Test.Publisher  (SpotPricer + IrsPricer handlers registered)
//   3. dotnet run -c Release    (this benchmark)

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Streamly;
using Streamly.Client;

namespace Streamly.Benchmarks;

public sealed class BenchmarkHarness : IAsyncDisposable
{
    public const string NatsUrl = "nats://localhost:4222";
    public const string SpotStreamName = "GetSpotPrice";
    public const string IrsStreamName = "GetSwapPrice";

    private IHost? _subscriberHost;

    public IStreamingSubscriber<SpotRequest, SpotPrice> Subscriber { get; private set; } = null!;
    public IStreamingSubscriber<IrsRequest, IrsResponse> IrsSubscriber { get; private set; } = null!;

    public async Task StartAsync()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Streamly:NatsUrl"] = NatsUrl,
            ["Streamly:ServiceName"] = $"Bench-{Guid.NewGuid():N}"[..20],
            ["Streamly:SubscriberHeartbeatTimeoutMs"] = "2000",
        });
        builder.Logging
            .ClearProviders()
            .AddFilter("Streamly", LogLevel.Warning);
        builder.Services.AddStreamly(builder.Configuration, options =>
        {
            options.AddSubscriber<SpotRequest, SpotPrice>(SpotStreamName);
            options.AddSubscriber<IrsRequest, IrsResponse>(IrsStreamName);
        });

        _subscriberHost = builder.Build();
        await _subscriberHost.StartAsync();

        Subscriber = _subscriberHost.Services
            .GetRequiredService<IStreamingSubscriber<SpotRequest, SpotPrice>>();

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
