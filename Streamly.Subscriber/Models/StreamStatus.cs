namespace Streamly.Subscriber.Models;

public class StreamStatus
{
    public StreamState State { get; init; }
    public string StreamName { get; init; } = string.Empty;
    public string? Message { get; init; }
    public int RetryAttempt { get; init; }      // 0 when Active
    public int MaxRetryAttempts { get; init; }  // 0 when Active
    public Exception? Exception { get; init; }  // populated on Failed

    public static StreamStatus Active(string streamName) => new()
    {
        State = StreamState.Active,
        StreamName = streamName,
        Message = "Stream active"
    };

    public static StreamStatus Reconnecting(string streamName, int attempt, int max, Exception ex) => new()
    {
        State = StreamState.Reconnecting,
        StreamName = streamName,
        Message = $"Stream lost, reconnecting... (attempt {attempt}/{max})",
        RetryAttempt = attempt,
        MaxRetryAttempts = max,
        Exception = ex
    };

    public static StreamStatus Failed(string streamName, int maxAttempts, Exception ex) => new()
    {
        State = StreamState.Failed,
        StreamName = streamName,
        Message = $"Stream permanently lost after {maxAttempts} attempts",
        RetryAttempt = maxAttempts,
        MaxRetryAttempts = maxAttempts,
        Exception = ex
    };
}