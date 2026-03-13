// FILE: Streamly.Benchmarks/Benchmarks/LatencyBenchmark.cs
//
// Measures time-to-first-price: wall-clock elapsed from Subscribe() call
// to the first onNext callback arriving from the publisher.
//
// NFR target: P99 < 50ms

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

    // ConcurrentBag is thread-safe for Add (IterationSetup)
    // and TryTake (cleanup) without any explicit locking
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
                    new BenchSpotRequest { CurrencyPair = "EUR/USD" },
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
                new BenchSpotRequest { CurrencyPair = "EUR/USD" },
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
        DrainAndDispose(ref _backgroundSubscriptions);
        Thread.Sleep(500);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        // Drain anything IterationCleanup may have left (e.g. on crash path)
        DrainAndDispose(ref _backgroundSubscriptions);
        await _harness.DisposeAsync();
    }

    // Drains a ConcurrentBag by replacing it atomically with a fresh empty
    // instance, then disposing all items from the drained snapshot.
    // Replacing rather than draining in-place avoids a race where IterationSetup
    // adds to the bag while cleanup is still draining it.
    private static void DrainAndDispose(ref ConcurrentBag<IDisposable> bag)
    {
        var snapshot = Interlocked.Exchange(ref bag, new ConcurrentBag<IDisposable>());
        while (snapshot.TryTake(out var sub))
            sub.Dispose();
    }
}