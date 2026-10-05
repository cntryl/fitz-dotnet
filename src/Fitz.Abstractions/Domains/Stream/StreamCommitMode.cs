namespace Cntryl.Fitz;

/// <summary>Durability policy selected when committing a stream session.</summary>
public enum StreamCommitMode
{
    /// <summary>Use the broker's buffered write policy; recent commits may be lost on a crash.</summary>
    Buffered = 0,

    /// <summary>Use the broker's synchronous write policy before reporting success.</summary>
    Sync = 1,
}
