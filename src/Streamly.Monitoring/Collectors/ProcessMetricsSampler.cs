using System.Diagnostics;
using Microsoft.Extensions.Hosting;

namespace Streamly.Monitoring.Collectors;

/// <summary>
/// Background service that samples process-level CPU and memory metrics every 5 seconds.
/// CPU % cannot be derived from a single reading — it requires a delta between two
/// TotalProcessorTime samples divided by elapsed wall time and processor count.
/// Memory is also refreshed here so all process readings share the same Refresh() call.
///
/// Exposes the latest readings as volatile fields read by InMemoryMetricsCollector.GetSnapshot().
/// </summary>
public sealed class ProcessMetricsSampler : BackgroundService
{
    // Cached once — same process for the lifetime of the app.
    // Process.GetCurrentProcess() allocates; we reuse the handle and call Refresh().
    private static readonly Process Process = Process.GetCurrentProcess();

    private static readonly int ProcessorCount = Environment.ProcessorCount;
    private const int SampleIntervalMs = 5_000;

    // ── Latest readings ───────────────────────────────────────────────────────────
    // volatile is not supported for double/long on all platforms.
    // long fields use Interlocked.Read/Exchange for safe cross-thread access.
    // double is stored as its raw bits in a long via BitConverter so we can use Interlocked.

    private long _cpuPercentBits;    // BitConverter.DoubleToInt64Bits(value)
    private long _workingSetBytes;
    private long _gcHeapBytes;

    /// <summary>CPU usage % across all logical cores. 0 until the first sample completes.</summary>
    public double CpuPercent
    {
        get => BitConverter.Int64BitsToDouble(Interlocked.Read(ref _cpuPercentBits));
        private set => Interlocked.Exchange(ref _cpuPercentBits, BitConverter.DoubleToInt64Bits(value));
    }

    /// <summary>OS working set in bytes (total RAM footprint of the process).</summary>
    public long WorkingSetBytes
    {
        get => Interlocked.Read(ref _workingSetBytes);
        private set => Interlocked.Exchange(ref _workingSetBytes, value);
    }

    /// <summary>Managed GC heap in bytes (excludes native/unmanaged allocations).</summary>
    public long GcHeapBytes
    {
        get => Interlocked.Read(ref _gcHeapBytes);
        private set => Interlocked.Exchange(ref _gcHeapBytes, value);
    }

    // ── Sampling state ────────────────────────────────────────────────────────────

    private DateTime _lastSampleTime = DateTime.UtcNow;
    private TimeSpan _lastCpuTime;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Prime the baseline so the first delta is meaningful
        Process.Refresh();
        _lastCpuTime    = Process.TotalProcessorTime;
        _lastSampleTime = DateTime.UtcNow;

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(SampleIntervalMs, stoppingToken);

            try
            {
                Process.Refresh();

                var now        = DateTime.UtcNow;
                var cpuTime    = Process.TotalProcessorTime;
                var wallSeconds = (now - _lastSampleTime).TotalSeconds;

                if (wallSeconds > 0)
                {
                    var cpuSeconds = (cpuTime - _lastCpuTime).TotalSeconds;
                    CpuPercent = Math.Round(cpuSeconds / (wallSeconds * ProcessorCount) * 100, 1);
                }

                _lastSampleTime = now;
                _lastCpuTime    = cpuTime;

                WorkingSetBytes = Process.WorkingSet64;
                GcHeapBytes     = GC.GetTotalMemory(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Swallow — metric sampling must never crash the host.
                // Next iteration will retry automatically.
            }
        }
    }
}
