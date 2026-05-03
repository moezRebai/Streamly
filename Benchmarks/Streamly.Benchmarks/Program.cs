// FILE: Streamly.Benchmarks/Program.cs

using BenchmarkDotNet.Running;
using Streamly.Benchmarks;
using Streamly.Benchmarks.TestCases;

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
Console.WriteLine("Requires: Streamly.Test.Publisher running (SpotPricer + IrsPricer)");
Console.WriteLine();
Console.WriteLine("  1 - Latency        (Spot, P50/P95/P99, background load: 0/100/1k/5k)");
Console.WriteLine("  2 - BurstOpen      (Spot, time-to-last-first-price: 1k/5k)");
Console.WriteLine("  3 - IrsBurstOpen   (IRS,  time-to-last-first-price: 1k/5k/10k)");
Console.WriteLine("  4 - Throughput     (Spot, msg/sec, 100/1k/5k/10k/20k streams)");
Console.WriteLine("  5 - Memory         (Spot, KB/stream, 100/1k/5k/10k/20k streams)");
Console.WriteLine("  6 - All");
Console.WriteLine();
Console.Write("Choice [1-6]: ");

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
        BenchmarkRunner.Run<IrsBurstOpenBenchmark>();
        break;
    case "4":
        BenchmarkRunner.Run<ThroughputBenchmark>();
        break;
    case "5":
        BenchmarkRunner.Run<MemoryBenchmark>();
        break;
    default:
        BenchmarkSwitcher
            .FromAssembly(typeof(LatencyBenchmark).Assembly)
            .RunAll();
        break;
}