// FILE: Streamly.Benchmarks/StreamlyBenchmarkConfig.cs
// BenchmarkDotNet configuration for all Streamly benchmarks.
//
// Job settings rationale:
//   WarmupCount=1 : one iteration to let NATS subscriptions establish
//   IterationCount=5 : enough for P95/P99 to be meaningful without
//                      running for hours at 20k streams
//   InvocationCount=1 / UnrollFactor=1 : each invocation is one full
//                      async scenario - BDN must not batch them
//
// Note on percentiles: StatisticColumn.P50/P95/P99 were removed in BDN 0.14+
// and PercentileColumn does not exist in 0.15.x either. P50/P95/P99 are still
// present in the HTML and CSV exports automatically. The console table shows
// Mean, StdDev, Min, Max which is sufficient for pass/fail NFR checks.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Validators;

namespace Streamly.Benchmarks;

public class StreamlyBenchmarkConfig : ManualConfig
{
    public StreamlyBenchmarkConfig()
    {
        AddJob(Job.Default
            .WithWarmupCount(1)
            .WithIterationCount(1)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithId("Streamly"));

        AddDiagnoser(MemoryDiagnoser.Default);

        // HTML export contains full percentile breakdown (P0 through P100)
        // CSV export contains raw per-iteration values for manual P99 calculation
        // GitHub markdown export for README
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(CsvExporter.Default);
        AddExporter(HtmlExporter.Default);

        // Available in all BDN versions
        AddColumn(StatisticColumn.Min);
        AddColumn(StatisticColumn.Max);
        AddColumn(StatisticColumn.Mean);
        AddColumn(StatisticColumn.StdDev);

        AddLogger(ConsoleLogger.Default);
        
        // MinIterationTime warning is designed for CPU micro-benchmarks.
        // Integration benchmarks with NATS round-trips are inherently slower
        // and the warning is not actionable here.
        WithOption(ConfigOptions.DisableOptimizationsValidator, true);
        AddValidator(JitOptimizationsValidator.DontFailOnError);
    }
}