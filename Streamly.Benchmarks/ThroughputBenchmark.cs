// FILE: Streamly.Benchmarks/Benchmarks/ThroughputBenchmark.cs
//
// Measures messages received per second across N concurrent subscriptions.
// The publisher (Streamly.Test.Publisher) ticks at 500ms per stream by default.
// Expected baseline: N streams × 2 msg/sec.
//
// The benchmark opens all subscriptions, waits for the warmup phase (all
// streams receive at least one message), then counts messages over a
// 10-second window.
//
// [Params] ConcurrentStreams: 100, 1k, 5k, 10k, 20k

using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Streamly.Core.Models;

namespace Streamly.Benchmarks;

[Config(typeof(StreamlyBenchmarkConfig))]
[MemoryDiagnoser]
public class ThroughputBenchmark
{
    private BenchmarkHarness _harness = null!;

    [Params(100, 1_000, 5_000, 10_000, 20_000)]
    public int ConcurrentStreams;

    private const int MeasurementWindowSec = 10;

    private long _totalReceived;
    private readonly List<IDisposable> _subscriptions = new();

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _harness = new BenchmarkHarness();
        await _harness.StartAsync();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        Interlocked.Exchange(ref _totalReceived, 0);
        _subscriptions.Clear();
    }

    [Benchmark]
    public async Task<double> MessagesPerSecond()
    {
        // Open all subscriptions - all on EUR/USD since that is what the
        // running publisher handles (deduplication means one handler serves all)
        for (var i = 0; i < ConcurrentStreams; i++)
        {
            var sub = _harness.Subscriber
                .Subscribe(
                    new BenchSpotRequest { CurrencyPair = "EUR/USD" },
                    behavior: StreamBehavior.Live)
                .Subscribe(
                    onNext: _ => Interlocked.Increment(ref _totalReceived),
                    onError: _ => { });

            _subscriptions.Add(sub);
        }

        // Warmup: wait until every subscription has received at least one message
        var warmupDeadline = DateTime.UtcNow.AddSeconds(30);
        while (Interlocked.Read(ref _totalReceived) < ConcurrentStreams
               && DateTime.UtcNow < warmupDeadline)
        {
            await Task.Delay(500);
        }

        if (Interlocked.Read(ref _totalReceived) < ConcurrentStreams)
            return 0; // warmup timed out - publisher not keeping up

        // Measurement window
        var baseline = Interlocked.Read(ref _totalReceived);
        var sw = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(MeasurementWindowSec));
        sw.Stop();

        var delta = Interlocked.Read(ref _totalReceived) - baseline;
        return delta / sw.Elapsed.TotalSeconds;
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        foreach (var sub in _subscriptions)
            sub.Dispose();
        _subscriptions.Clear();

        Thread.Sleep(1000);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _harness.DisposeAsync();
    }
}