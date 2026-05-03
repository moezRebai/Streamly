// FILE: Streamly.Benchmarks/Benchmarks/MemoryBenchmark.cs
//
// Measures subscriber-side memory footprint per open stream.
// Only the subscriber process memory is measured here - the publisher
// is running externally so its memory is not included.
//
// NFR target: <= 5 KB managed heap per stream on the subscriber side.

using BenchmarkDotNet.Attributes;
using Streamly.Core.Models;

namespace Streamly.Benchmarks.TestCases;

[Config(typeof(StreamlyBenchmarkConfig))]
[MemoryDiagnoser]
public class MemoryBenchmark
{
    private BenchmarkHarness _harness = null!;

    [Params(100, 1_000, 5_000, 10_000, 20_000)]
    public int StreamCount;

    private readonly List<IDisposable> _subscriptions = new();

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _harness = new BenchmarkHarness();
        await _harness.StartAsync();
        ForceFullGc();
    }

    [Benchmark]
    public async Task<MemorySnapshot> MemoryPerStream()
    {
        ForceFullGc();
        var managedBefore    = GC.GetTotalMemory(false);
        var workingSetBefore = Environment.WorkingSet;

        var established = 0L;
        for (var i = 0; i < StreamCount; i++)
        {
            var firstMsg = 1;
            var sub = _harness.SpotSubscriber
                .Subscribe(
                    new SpotRequest { CurrencyPair = "EUR/USD" },
                    behavior: StreamBehavior.Live)
                .Subscribe(
                    onNext: _ =>
                    {
                        if (Interlocked.Exchange(ref firstMsg, 0) == 1)
                            Interlocked.Increment(ref established);
                    },
                    onError: _ => { });

            _subscriptions.Add(sub);
        }

        // Wait for all streams to receive their first message (fully established), up to 30s
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (Interlocked.Read(ref established) < StreamCount && DateTime.UtcNow < deadline)
            await Task.Delay(500);

        ForceFullGc();
        var managedAfter    = GC.GetTotalMemory(false);
        var workingSetAfter = Environment.WorkingSet;

        var managedKbPerStream =
            (managedAfter - managedBefore) / 1024.0 / StreamCount;
        var workingSetKbPerStream =
            (workingSetAfter - workingSetBefore) / 1024.0 / StreamCount;

        return new MemorySnapshot(
            StreamCount,
            managedKbPerStream,
            workingSetKbPerStream,
            managedAfter    / 1024.0 / 1024.0,
            workingSetAfter / 1024.0 / 1024.0);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        foreach (var sub in _subscriptions)
            sub.Dispose();
        _subscriptions.Clear();

        Thread.Sleep(1000);
        ForceFullGc();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _harness.DisposeAsync();
    }

    private static void ForceFullGc()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    }
}

public record MemorySnapshot(
    int    Streams,
    double ManagedKbPerStream,
    double WorkingSetKbPerStream,
    double TotalManagedMb,
    double TotalWorkingSetMb)
{
    public override string ToString() =>
        $"Managed {ManagedKbPerStream:F1} KB/stream | " +
        $"WS {WorkingSetKbPerStream:F1} KB/stream | " +
        $"Total WS {TotalWorkingSetMb:F0} MB";
}
