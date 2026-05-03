// FILE: Streamly.Benchmarks/Benchmarks/LatencyBenchmark.cs
//
// Measures time-to-first-price: wall-clock elapsed from Subscribe() call
// to the first onNext callback arriving from the publisher.
//
// NFR target: P99 < 50ms  (requires IterationCount >= 5 for a valid P99 estimate)
//
// BackgroundStreams=0 is a cold-start edge case: with no existing subscribers
// the publisher may be mid-jitter delay, producing artificially high latency
// unrelated to Streamly's fan-out path. The warm-system NFR applies to
// BackgroundStreams > 0.
//
// BackgroundStreams all subscribe to the same EUR/USD pair. Publisher
// deduplication means one handler serves all of them — the background load
// exercises the client-side fan-out path, not additional publisher handlers.

using System.Collections.Concurrent;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Streamly.Core.Models;

namespace Streamly.Benchmarks.TestCases;

[Config(typeof(StreamlyBenchmarkConfig))]
[MemoryDiagnoser]
public class LatencyBenchmark
{
    private BenchmarkHarness _harness = null!;

    [Params(0, 100, 1_000, 5_000, 10000)]
    public int BackgroundStreams;

    private ConcurrentBag<IDisposable> _backgroundSubscriptions = new();
    private long _establishedStreams;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _harness = new BenchmarkHarness();
        await _harness.StartAsync();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        Interlocked.Exchange(ref _establishedStreams, 0);

        for (var i = 0; i < BackgroundStreams; i++)
        {
            var firstMsg = 1;
            var sub = _harness.SpotSubscriber
                .Subscribe(
                    new SpotRequest { CurrencyPair = "EUR/USD" },
                    behavior: StreamBehavior.Live)
                .Subscribe(_ =>
                {
                    if (Interlocked.Exchange(ref firstMsg, 0) == 1)
                        Interlocked.Increment(ref _establishedStreams);
                });

            _backgroundSubscriptions.Add(sub);
        }

        if (BackgroundStreams > 0)
        {
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (Interlocked.Read(ref _establishedStreams) < BackgroundStreams
                   && DateTime.UtcNow < deadline)
                Thread.Sleep(100);
        }

        // Flush setup allocations so they don't inflate the MemoryDiagnoser reading.
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    }

    [Benchmark]
    public async Task<double> TimeToFirstPrice()
    {
        var tcs = new TaskCompletionSource<double>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var sw = Stopwatch.StartNew();
        IDisposable? subscription = null;

        subscription = _harness.SpotSubscriber
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
