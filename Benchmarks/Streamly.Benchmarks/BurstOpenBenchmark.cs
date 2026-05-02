// Measures burst-open latency: wall-clock elapsed from the first SubscribeAsync
// call to the moment the LAST of N subscribers receives its first price.
//
// This is the critical 00:05 scenario: 10,000 simultaneous requests from
// TrepGateway must all be streaming before 08:30.
//
// Suspected bottlenecks identified from RequestManager / ConfirmationPublisher:
//   1. OnRequestReceivedAsync is a single sequential NATS callback — all N
//      envelopes queue behind each other (deserialize + SHA256 + TryAdd +
//      ConfirmAsync per message, no parallelism).
//   2. ConfirmationPublisher has no batching: N individual PublishAsync calls
//      issued serially from inside that same sequential callback.
//   3. Thread-pool starvation: N fire-and-forget handler tasks spawned
//      near-simultaneously compete for worker threads at their first await.

using System.Collections.Concurrent;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Streamly.Core.Models;

namespace Streamly.Benchmarks;

[Config(typeof(StreamlyBenchmarkConfig))]
[MemoryDiagnoser]
public class BurstOpenBenchmark
{
    private BenchmarkHarness _harness = null!;

    [Params(1000, 5000)]
    public int N;

    private ConcurrentBag<IDisposable> _subscriptions = new();

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _harness = new BenchmarkHarness();
        await _harness.StartAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        BenchmarkHelpers.DrainAndDispose(ref _subscriptions);
        await _harness.DisposeAsync();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        BenchmarkHelpers.DrainAndDispose(ref _subscriptions);
        // Let the publisher clear its registry before the next iteration
        // floods it with N fresh requests.
        Thread.Sleep(5000);
    }

    [Benchmark]
    public async Task<double> TimeToLastFirstPrice()
    {
        // Each subscriber gets a unique pair so N distinct RequestIds are
        // created on the publisher (no TryGet early-exit masking TryAdd
        // contention). Index is stable across iterations so JIT / NATS
        // connection paths are exercised uniformly.
        var remaining = N;
        var tcs = new TaskCompletionSource<double>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var sw = Stopwatch.StartNew();

        // Fan out N subscriptions concurrently — same pressure profile as
        // TrepGateway firing all requests simultaneously.
        var tasks = new Task[N];
        for (var i = 0; i < N; i++)
        {
            var pair = $"EUR/USD_{i}";
            tasks[i] = Task.Run(() =>
            {
                var sub = _harness.Subscriber
                    .Subscribe(
                        new SpotRequest { CurrencyPair = pair },
                        behavior: StreamBehavior.Live)
                    .Subscribe(
                        onNext: _ =>
                        {
                            // Last subscriber to receive its first price
                            // stops the clock.
                            if (Interlocked.Decrement(ref remaining) == 0)
                                tcs.TrySetResult(sw.Elapsed.TotalMilliseconds);
                        },
                        onError: ex =>
                        {
                            // Count failed subs down so the benchmark never
                            // deadlocks; the anomalous run shows up as an
                            // outlier in the CSV.
                            if (Interlocked.Decrement(ref remaining) == 0)
                                tcs.TrySetResult(sw.Elapsed.TotalMilliseconds);
                        });

                _subscriptions.Add(sub);
            });
        }

        await Task.WhenAll(tasks);

        // Safety net: 5 minutes is far beyond any realistic burst window.
        // Hitting it means the run is invalid (publisher overloaded / crashed).
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        cts.Token.Register(() =>
            tcs.TrySetException(new TimeoutException(
                $"Only {N - remaining}/{N} streams received first price within 5 min.")));

        return await tcs.Task;
    }
}
