namespace Streamly.Core.Runtime.RequestManagement;

internal sealed class ChunkAccumulator(long epoch, int totalChunks)
{
    private readonly List<ActiveRequestSnapshot>[] _slots = new List<ActiveRequestSnapshot>[totalChunks];
    private int _received;

    public long Epoch       { get; } = epoch;
    public int  TotalChunks { get; } = totalChunks;

    /// <summary>
    /// Store this chunk's snapshots.
    /// Returns true when every chunk in the group has arrived.
    /// </summary>
    public bool TryComplete(int chunkIndex, List<ActiveRequestSnapshot> snapshots)
    {
        _slots[chunkIndex] = snapshots;
        return Interlocked.Increment(ref _received) == TotalChunks;
    }

    /// <summary>Flatten all slots into a single ordered list.</summary>
    public List<ActiveRequestSnapshot> Flatten()
    {
        var result = new List<ActiveRequestSnapshot>();
        foreach (var slot in _slots)
            if (slot is not null)
                result.AddRange(slot);
        return result;
    }
}