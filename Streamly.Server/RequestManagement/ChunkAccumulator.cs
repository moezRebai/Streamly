namespace Streamly.Server.RequestManagement;

internal sealed class ChunkAccumulator(long epoch, int totalChunks)
{
    private readonly List<ActiveRequestSnapshot>[] _slots = new List<ActiveRequestSnapshot>[totalChunks];
    private int _received;

    public long Epoch       { get; } = epoch;
    public int  TotalChunks { get; } = totalChunks;

    public bool TryComplete(int chunkIndex, List<ActiveRequestSnapshot> snapshots)
    {
        _slots[chunkIndex] = snapshots;
        return Interlocked.Increment(ref _received) == TotalChunks;
    }

    public List<ActiveRequestSnapshot> Flatten()
    {
        var result = new List<ActiveRequestSnapshot>();
        foreach (var slot in _slots)
            if (slot is not null)
                result.AddRange(slot);
        return result;
    }
}
