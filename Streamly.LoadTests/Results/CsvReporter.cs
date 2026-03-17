using System.Diagnostics;
using System.Globalization;

namespace Streamly.LoadTests;

/// <summary>
/// Écrit les résultats de test dans un fichier CSV horodaté.
/// Format compatible Excel / pandas pour analyse post-test.
/// </summary>
public class CsvReporter : IDisposable
{
    private readonly string _filePath;
    private readonly StreamWriter _writer;
    private readonly Stopwatch _elapsed = Stopwatch.StartNew();
    private bool _headerWritten;

    public CsvReporter(string scenarioName)
    {
        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var dir = Path.Combine(
            AppContext.BaseDirectory, "Results", scenarioName);
        Directory.CreateDirectory(dir);

        _filePath = Path.Combine(dir, $"{scenarioName}_{timestamp}.csv");
        _writer = new StreamWriter(_filePath, append: false);

        Console.WriteLine($"[CsvReporter] Output → {_filePath}");
    }

    /// <summary>
    /// Enregistre une mesure de latence end-to-end.
    /// </summary>
    public void RecordLatency(
        int streamCount,
        double latencyMs,
        string currencyPair = "")
    {
        EnsureHeader("ScenarioElapsedMs,StreamCount,LatencyMs,CurrencyPair");
        _writer.WriteLine(string.Join(",",
            _elapsed.ElapsedMilliseconds,
            streamCount,
            latencyMs.ToString("F3", CultureInfo.InvariantCulture),
            currencyPair));
        _writer.Flush();
    }

    /// <summary>
    /// Enregistre une mesure de throughput.
    /// </summary>
    public void RecordThroughput(
        int streamCount,
        double messagesPerSec,
        double windowSeconds)
    {
        EnsureHeader("ScenarioElapsedMs,StreamCount,MessagesPerSec,WindowSeconds");
        _writer.WriteLine(string.Join(",",
            _elapsed.ElapsedMilliseconds,
            streamCount,
            messagesPerSec.ToString("F1", CultureInfo.InvariantCulture),
            windowSeconds.ToString("F1", CultureInfo.InvariantCulture)));
        _writer.Flush();
    }

    /// <summary>
    /// Enregistre un événement de failover.
    /// </summary>
    public void RecordFailover(
        string eventType,        // "LeaderKilled" | "NewLeaderElected" | "FirstMessageReceived"
        string instanceId,
        long elapsedSinceKillMs)
    {
        EnsureHeader("ScenarioElapsedMs,EventType,InstanceId,ElapsedSinceKillMs");
        _writer.WriteLine(string.Join(",",
            _elapsed.ElapsedMilliseconds,
            eventType,
            instanceId,
            elapsedSinceKillMs));
        _writer.Flush();
    }

    /// <summary>
    /// Enregistre un snapshot mémoire.
    /// </summary>
    public void RecordMemory(
        int streamCount,
        double managedMb,
        double workingSetMb,
        double kbPerStream)
    {
        EnsureHeader("ScenarioElapsedMs,StreamCount,ManagedMb,WorkingSetMb,KbPerStream");
        _writer.WriteLine(string.Join(",",
            _elapsed.ElapsedMilliseconds,
            streamCount,
            managedMb.ToString("F1", CultureInfo.InvariantCulture),
            workingSetMb.ToString("F1", CultureInfo.InvariantCulture),
            kbPerStream.ToString("F2", CultureInfo.InvariantCulture)));
        _writer.Flush();
    }

    public void PrintSummary(string label, IEnumerable<double> values)
    {
        var list = values.OrderBy(x => x).ToList();
        if (!list.Any()) return;

        Console.WriteLine($"\n[{label}]");
        Console.WriteLine($"  Count  : {list.Count}");
        Console.WriteLine($"  Min    : {list.First():F2}");
        Console.WriteLine($"  P50    : {Percentile(list, 50):F2}");
        Console.WriteLine($"  P95    : {Percentile(list, 95):F2}");
        Console.WriteLine($"  P99    : {Percentile(list, 99):F2}");
        Console.WriteLine($"  Max    : {list.Last():F2}");
        Console.WriteLine($"  File   : {_filePath}");
    }

    private static double Percentile(List<double> sorted, int p)
    {
        var index = (int)Math.Ceiling(p / 100.0 * sorted.Count) - 1;
        return sorted[Math.Max(0, Math.Min(index, sorted.Count - 1))];
    }

    private void EnsureHeader(string header)
    {
        if (_headerWritten) return;
        _writer.WriteLine(header);
        _headerWritten = true;
    }

    public void Dispose()
    {
        _writer.Flush();
        _writer.Dispose();
    }
}
