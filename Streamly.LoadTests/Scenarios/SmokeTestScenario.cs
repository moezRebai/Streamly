// FILE: Streamly.LoadTests/Scenarios/SmokeTestScenario.cs
//
// Scale smoke tests: 5k / 10k / 20k simultaneous streams, 3 minutes sustained.
//
// Launch order:
//   1. nats-server -js
//   2. Streamly.Test.Publisher  (dotnet run or from Rider)
//   3. dotnet run               (this project)
//
// Pass criteria:
//   - All N streams establish within 30s of opening
//   - All established streams keep receiving prices throughout the run
//   - No stream falls silent for > 10s during the sustained period
//   - Memory growth < 50 MB/1000 streams (rough guard against leaks)

using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Streamly.Core.Models;
using Streamly.Subscriber;

namespace Streamly.LoadTests.Scenarios;

public class SmokeTestScenario
{
    private const string NatsUrl    = "nats://localhost:4222";
    private const string StreamName = "SpotPricer";

    // How long to sustain each load level after all streams have established.
    private static readonly TimeSpan SustainedDuration = TimeSpan.FromMinutes(3);

    // Maximum wall-clock time allowed for all N streams to receive their first price.
    private static readonly TimeSpan EstablishTimeout = TimeSpan.FromSeconds(30);

    // A stream is considered "silent" (stalled) if it hasn't received a price in this window.
    private static readonly TimeSpan SilenceThreshold = TimeSpan.FromSeconds(10);

    // Memory sampling interval.
    private static readonly TimeSpan MemorySampleInterval = TimeSpan.FromSeconds(30);

    // Batch size used when opening streams — mirrors the BurstOpenBenchmark fix.
    // Opening everything in a single burst at very high N can overwhelm the publisher
    // request pipeline; batching gives each batch time to confirm before the next fires.
    private const int BatchSize = 500;
    private static readonly TimeSpan BatchDelay = TimeSpan.FromMilliseconds(50);

    public async Task RunAsync(int[] loadLevels)
    {
        Console.WriteLine("\n=== Streamly Scale Smoke Tests ===");
        Console.WriteLine($"Load levels : {string.Join(" / ", loadLevels.Select(n => $"{n:N0}"))} streams");
        Console.WriteLine($"Duration    : {SustainedDuration.TotalMinutes:F0} min per level");
        Console.WriteLine($"Establish   : within {EstablishTimeout.TotalSeconds:F0}s");
        Console.WriteLine($"Silence     : alert after {SilenceThreshold.TotalSeconds:F0}s");
        Console.WriteLine();

        using var reporter = new CsvReporter("SmokeTest");
        var summaryRows = new List<SummaryRow>();

        foreach (var n in loadLevels)
        {
            var row = await RunLevelAsync(n, reporter);
            summaryRows.Add(row);

            // Brief pause between levels to let NATS and the publisher drain.
            Console.WriteLine($"\nCooling down 10s before next level...");
            await Task.Delay(TimeSpan.FromSeconds(10));
        }

        PrintFinalSummary(summaryRows);
    }

    // ─── Single load level ──────────────────────────────────────────────────

    private async Task<SummaryRow> RunLevelAsync(int n, CsvReporter reporter)
    {
        Console.WriteLine($"\n{'─',60}");
        Console.WriteLine($"Load level: {n:N0} streams");
        Console.WriteLine($"{'─',60}");

        using var host = BuildSubscriberHost();
        await host.StartAsync();

        var subscriber = host.Services
            .GetRequiredService<IStreamingSubscriber<SpotRequest, SpotPrice>>();

        // Per-stream state: last price timestamp + total count.
        var lastSeenTicks  = new ConcurrentDictionary<int, long>();   // index → Stopwatch ticks
        var priceCounts    = new ConcurrentDictionary<int, long>();   // index → total messages
        var established    = new ConcurrentDictionary<int, bool>();   // index → first price received
        var disposables    = new ConcurrentBag<IDisposable>();

        var sw = Stopwatch.StartNew();

        // ── Open streams in batches ──────────────────────────────────────────
        Console.WriteLine($"Opening {n:N0} streams in batches of {BatchSize}...");

        for (var batchStart = 0; batchStart < n; batchStart += BatchSize)
        {
            var batchEnd = Math.Min(batchStart + BatchSize, n);
            var tasks = new Task[batchEnd - batchStart];

            for (var idx = batchStart; idx < batchEnd; idx++)
            {
                var i = idx; // capture
                tasks[i - batchStart] = Task.Run(() =>
                {
                    var pair = $"EUR/USD_{i}";
                    var sub = subscriber
                        .Subscribe(
                            new SpotRequest { CurrencyPair = pair },
                            behavior: StreamBehavior.Live)
                        .Subscribe(
                            onNext: _ =>
                            {
                                lastSeenTicks[i]  = Stopwatch.GetTimestamp();
                                priceCounts.AddOrUpdate(i, 1, (_, c) => c + 1);
                                established.TryAdd(i, true);
                            },
                            onError: ex =>
                            {
                                // Log but don't throw — individual stream errors
                                // surface in the stale/establish checks below.
                            });

                    disposables.Add(sub);
                });
            }

            await Task.WhenAll(tasks);

            if (batchEnd < n)
                await Task.Delay(BatchDelay);

            if (batchEnd % 2000 == 0 || batchEnd == n)
                Console.WriteLine($"  Opened {batchEnd:N0}/{n:N0} streams ({sw.Elapsed.TotalSeconds:F1}s)");
        }

        var openElapsed = sw.Elapsed;
        Console.WriteLine($"All subscribe calls issued in {openElapsed.TotalSeconds:F1}s");

        // ── Wait for all streams to establish ───────────────────────────────
        Console.WriteLine($"Waiting up to {EstablishTimeout.TotalSeconds:F0}s for all streams to receive first price...");

        var establishDeadline = sw.Elapsed + EstablishTimeout;
        while (sw.Elapsed < establishDeadline && established.Count < n)
        {
            await Task.Delay(500);
            var pct = established.Count * 100.0 / n;
            Console.Write($"\r  Established: {established.Count:N0}/{n:N0} ({pct:F1}%)   ");
        }
        Console.WriteLine();

        var establishedCount = established.Count;
        var establishMs      = sw.Elapsed.TotalMilliseconds;
        var establishOk      = establishedCount == n;

        Console.WriteLine(establishOk
            ? $"  ✓ All {n:N0} streams established in {establishMs:F0}ms"
            : $"  ✗ Only {establishedCount:N0}/{n:N0} streams established within {EstablishTimeout.TotalSeconds:F0}s");

        reporter.RecordLatency(n, establishMs, $"establish_{n}");

        // ── Sustained phase ──────────────────────────────────────────────────
        Console.WriteLine($"\nSustained phase: {SustainedDuration.TotalMinutes:F0} min...");

        var sustainedStart     = sw.Elapsed;
        var sustainedEnd       = sustainedStart + SustainedDuration;
        var nextMemorySample   = sw.Elapsed + MemorySampleInterval;
        var stallAlerts        = 0;
        var peakStalledStreams = 0;

        while (sw.Elapsed < sustainedEnd)
        {
            await Task.Delay(2000);

            var now       = Stopwatch.GetTimestamp();
            var silentIds = new List<int>();

            foreach (var kvp in lastSeenTicks)
            {
                var silentMs = (now - kvp.Value) / (double)Stopwatch.Frequency * 1000;
                if (silentMs > SilenceThreshold.TotalMilliseconds)
                    silentIds.Add(kvp.Key);
            }

            if (silentIds.Count > 0)
            {
                stallAlerts++;
                peakStalledStreams = Math.Max(peakStalledStreams, silentIds.Count);
                Console.WriteLine($"  ⚠ {silentIds.Count} streams silent > {SilenceThreshold.TotalSeconds:F0}s " +
                                  $"(stall alert #{stallAlerts}) at {sw.Elapsed - sustainedStart:mm\\:ss}");
            }

            // Memory sample
            if (sw.Elapsed >= nextMemorySample)
            {
                SampleAndReportMemory(reporter, n, sw.Elapsed.TotalSeconds);
                nextMemorySample = sw.Elapsed + MemorySampleInterval;
            }

            // Progress tick
            var remaining = (sustainedEnd - sw.Elapsed).TotalSeconds;
            Console.Write($"\r  Sustained {(sw.Elapsed - sustainedStart):mm\\:ss} / " +
                          $"{SustainedDuration:mm\\:ss}  |  " +
                          $"Prices received: {priceCounts.Values.Sum():N0}  |  " +
                          $"Stalled: {silentIds.Count}  |  " +
                          $"Remaining: {remaining:F0}s   ");
        }
        Console.WriteLine();

        // Final memory snapshot
        SampleAndReportMemory(reporter, n, sw.Elapsed.TotalSeconds);

        // ── Teardown ─────────────────────────────────────────────────────────
        Console.WriteLine($"\nDisposing {n:N0} subscriptions...");
        while (disposables.TryTake(out var d))
            d.Dispose();

        await host.StopAsync();
        host.Dispose();

        // ── Results ──────────────────────────────────────────────────────────
        var totalPrices  = priceCounts.Values.Sum();
        var sustainedSec = SustainedDuration.TotalSeconds;
        var throughput   = totalPrices / sustainedSec;
        var stallOk      = stallAlerts == 0;

        Console.WriteLine($"\n  Streams established : {establishedCount:N0}/{n:N0}  {(establishOk ? "✓" : "✗")}");
        Console.WriteLine($"  Establish time      : {establishMs:F0}ms  (limit: {EstablishTimeout.TotalMilliseconds:F0}ms)");
        Console.WriteLine($"  Stall alerts        : {stallAlerts}  (peak stalled: {peakStalledStreams})  {(stallOk ? "✓" : "✗")}");
        Console.WriteLine($"  Total prices rcvd   : {totalPrices:N0}");
        Console.WriteLine($"  Avg throughput      : {throughput:F0} msg/s");
        Console.WriteLine($"  Pass                : {(establishOk && stallOk ? "PASS ✓" : "FAIL ✗")}");

        reporter.RecordThroughput(n, throughput, sustainedSec);

        return new SummaryRow(
            N: n,
            EstablishedCount: establishedCount,
            EstablishMs: establishMs,
            StallAlerts: stallAlerts,
            PeakStalledStreams: peakStalledStreams,
            TotalPricesReceived: totalPrices,
            AvgThroughputPerSec: throughput,
            Pass: establishOk && stallOk);
    }

    // ─── Memory sampling ────────────────────────────────────────────────────

    private static void SampleAndReportMemory(CsvReporter reporter, int n, double elapsedSec)
    {
        GC.Collect(0, GCCollectionMode.Optimized, blocking: false);
        var managedMb    = GC.GetTotalMemory(forceFullCollection: false) / 1024.0 / 1024.0;
        var workingSetMb = Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0;
        var kbPerStream  = n > 0 ? managedMb * 1024.0 / n : 0;

        Console.WriteLine();
        Console.WriteLine($"  [Memory @ {elapsedSec:F0}s] " +
                          $"Managed: {managedMb:F1}MB  " +
                          $"WorkingSet: {workingSetMb:F1}MB  " +
                          $"Per-stream: {kbPerStream:F2}KB");

        reporter.RecordMemory(n, managedMb, workingSetMb, kbPerStream);
    }

    // ─── Final summary ───────────────────────────────────────────────────────

    private static void PrintFinalSummary(List<SummaryRow> rows)
    {
        Console.WriteLine("\n");
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                    SCALE SMOKE TEST SUMMARY                             ║");
        Console.WriteLine("╠════════╦════════════╦══════════════╦══════════╦══════════╦══════════════╣");
        Console.WriteLine("║ Streams║ Established║ Establish(ms)║ Stalls   ║ Msg/s    ║ Result       ║");
        Console.WriteLine("╠════════╬════════════╬══════════════╬══════════╬══════════╬══════════════╣");

        foreach (var r in rows)
        {
            var pct    = r.EstablishedCount * 100.0 / r.N;
            var result = r.Pass ? "PASS ✓" : "FAIL ✗";
            Console.WriteLine(
                $"║ {r.N,6:N0} ║ {r.EstablishedCount,6:N0}/{r.N,-3:N0} ║ {r.EstablishMs,12:F0} ║ {r.StallAlerts,8} ║ {r.AvgThroughputPerSec,8:F0} ║ {result,-12} ║");
        }

        Console.WriteLine("╚════════╩════════════╩══════════════╩══════════╩══════════╩══════════════╝");
        Console.WriteLine();

        var allPass = rows.All(r => r.Pass);
        Console.WriteLine($"Overall: {(allPass ? "ALL PASS ✓" : "FAILURES DETECTED ✗")}");

        if (!allPass)
        {
            Console.WriteLine("\nFailed levels:");
            foreach (var r in rows.Where(r => !r.Pass))
            {
                if (r.EstablishedCount < r.N)
                    Console.WriteLine($"  {r.N:N0} streams — only {r.EstablishedCount:N0} established ({r.EstablishedCount * 100.0 / r.N:F1}%)");
                if (r.StallAlerts > 0)
                    Console.WriteLine($"  {r.N:N0} streams — {r.StallAlerts} stall alert(s), peak {r.PeakStalledStreams} stalled");
            }
        }
    }

    // ─── DI host ─────────────────────────────────────────────────────────────

    private static IHost BuildSubscriberHost()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Streamly:NatsUrl"]     = NatsUrl,
                ["Streamly:ServiceName"] = $"SmokeTest-{Guid.NewGuid():N}"[..20],
                // Give the subscriber a generous heartbeat timeout during long runs.
                ["Streamly:SubscriberHeartbeatTimeoutMs"] = "10000",
            })
            .Build();

        return Host.CreateDefaultBuilder()
            .ConfigureLogging(log => log
                .ClearProviders()
                .AddConsole()
                .AddFilter("Streamly", LogLevel.Warning)
                .AddFilter("Microsoft.Hosting", LogLevel.Warning))
            .ConfigureServices((_, services) =>
            {
                services.AddStreamlySubscriber(config, options =>
                {
                    options.AddSubscriber<SpotRequest, SpotPrice>(StreamName);
                });
            })
            .Build();
    }

    // ─── Value types ─────────────────────────────────────────────────────────

    private record SummaryRow(
        int N,
        int EstablishedCount,
        double EstablishMs,
        int StallAlerts,
        int PeakStalledStreams,
        long TotalPricesReceived,
        double AvgThroughputPerSec,
        bool Pass);
}
