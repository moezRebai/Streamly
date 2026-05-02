// FILE: Streamly.Benchmarks/Benchmarks/LatencyBenchmark.cs
//
// Measures time-to-first-price: wall-clock elapsed from Subscribe() call
// to the first onNext callback arriving from the publisher.
//
// NFR target: P99 < 50ms
//
// BackgroundStreams all subscribe to the same EUR/USD pair. Publisher
// deduplication means one handler serves all of them — the background load
// exercises the client-side fan-out path, not additional publisher handlers.

using System.Collections.Concurrent;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Streamly.Core.Models;

namespace Streamly.Benchmarks;

[Config(typeof(StreamlyBenchmarkConfig))]
[MemoryDiagnoser]
public class LatencyBenchmark
{
    private BenchmarkHarness _harness = null!;

    [Params(0, 100, 1_000, 5_000)]
    public int BackgroundStreams;

    private ConcurrentBag<IDisposable> _backgroundSubscriptions = new();

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _harness = new BenchmarkHarness();
        await _harness.StartAsync();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        for (var i = 0; i < BackgroundStreams; i++)
        {
            var sub = _harness.Subscriber
                .Subscribe(
                    new SpotRequest { CurrencyPair = "EUR/USD" },
                    behavior: StreamBehavior.Live)
                .Subscribe(_ => { });

            _backgroundSubscriptions.Add(sub);
        }

        if (BackgroundStreams > 0)
            Thread.Sleep(2000);
    }

    [Benchmark]
    public async Task<double> TimeToFirstPrice()
    {
        var tcs = new TaskCompletionSource<double>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var sw = Stopwatch.StartNew();
        IDisposable? subscription = null;

        subscription = _harness.Subscriber
            .Subscribe(
                new SpotRequest { CurrencyPair = "EUR/USD" },
                behavior: StreamBehavior.Live)
            .Subscribe(
                onNext: _ => tcs.TrySetResult(sw.Elapsed.TotalMilliseconds),
                onError: ex => tcs.TrySetException(ex),
                onCompleted: () => tcs.TrySetCanceled());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        cts.Token.Register(() =>
            tcs.TrySetException(new TimeoutException(
                "No price received in 30s - is Streamly.Test.Publisher running?")));

        try
        {
            return await tcs.Task;
        }
        finally
        {
            subscription?.Dispose();
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        BenchmarkHelpers.DrainAndDispose(ref _backgroundSubscriptions);
        Thread.Sleep(500);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        BenchmarkHelpers.DrainAndDispose(ref _backgroundSubscriptions);
        await _harness.DisposeAsync();
    }
}
