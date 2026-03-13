// FILE: Streamly.Benchmarks/Program.cs
// Entry point for Streamly.Benchmarks.
//
// Must run in Release mode - BenchmarkDotNet enforces this.
// Launch command: dotnet run -c Release

using BenchmarkDotNet.Running;
using Streamly.Benchmarks;

#if DEBUG
Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine("┌─────────────────────────────────────────────────────┐");
Console.WriteLine("│  BenchmarkDotNet requires Release mode.             │");
Console.WriteLine("│  Run: dotnet run -c Release                         │");
Console.WriteLine("└─────────────────────────────────────────────────────┘");
Console.ResetColor();
Environment.Exit(1);
#endif

Console.WriteLine("Streamly Benchmarks");
Console.WriteLine("===================");
Console.WriteLine("Requires: NATS server running on nats://localhost:4222");
Console.WriteLine();
Console.WriteLine("  1 - Latency    (P50/P95/P99, background load: 0 / 100 / 1k / 5k streams)");
Console.WriteLine("  2 - BurstOpen  (time-to-last-first-price: 1k / 2k / 5k / 10k streams)");
Console.WriteLine("  3 - Throughput (msg/sec, 100 / 1k / 5k / 10k / 20k streams)");
Console.WriteLine("  4 - Memory     (KB/stream, 100 / 1k / 5k / 10k / 20k streams)");
Console.WriteLine("  5 - All");
Console.WriteLine();
Console.Write("Choice [1-5]: ");

var choice = Console.ReadLine()?.Trim();

switch (choice)
{
    case "1":
        BenchmarkRunner.Run<LatencyBenchmark>();
        break;

    case "2":
        BenchmarkRunner.Run<BurstOpenBenchmark>();
        break;

    case "3":
        BenchmarkRunner.Run<ThroughputBenchmark>();
        break;

    case "4":
        BenchmarkRunner.Run<MemoryBenchmark>();
        break;

    default:
        BenchmarkSwitcher
            .FromAssembly(typeof(LatencyBenchmark).Assembly)
            .RunAll();
        break;
}