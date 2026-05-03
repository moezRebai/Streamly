using System.Collections.Concurrent;

namespace Streamly.Benchmarks;

internal static class BenchmarkHelpers
{
    // Replaces the bag atomically so a concurrent IterationSetup cannot race
    // with cleanup while the snapshot is being drained.
    public static void DrainAndDispose(ref ConcurrentBag<IDisposable> bag)
    {
        var snapshot = Interlocked.Exchange(ref bag, []);
        while (snapshot.TryTake(out var sub))
            sub.Dispose();
    }
}
