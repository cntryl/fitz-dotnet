namespace Cntryl.Fitz;

/// <summary>One broker page and its durable position in the selected stream.</summary>
public sealed class StreamReadBatch
{
    /// <summary>Creates a batch from its tagged items and selector positions.</summary>
    /// <param name="items">Events and filtered markers in broker order.</param>
    /// <param name="fromOffset">Inclusive position requested.</param>
    /// <param name="nextOffset">Position to resume from after processing this batch.</param>
    /// <param name="caughtUp">Whether the snapshot has reached its tail.</param>
    public StreamReadBatch(IReadOnlyList<StreamReadItem> items, ulong fromOffset, ulong nextOffset, bool caughtUp)
    {
        Items = items ?? throw new ArgumentNullException(nameof(items));
        Records = items.Where(item => item.Kind == StreamReadItemKind.Event && item.Record is not null).Select(item => item.Record!).ToArray();
        FromOffset = fromOffset;
        NextOffset = nextOffset;
        CaughtUp = caughtUp;
    }

    /// <summary>Events and filtered markers in broker order.</summary>
    public IReadOnlyList<StreamReadItem> Items { get; }

    /// <summary>Visible events in broker order.</summary>
    public IReadOnlyList<StreamRecord> Records { get; }

    /// <summary>Inclusive position requested on the selector's axis.</summary>
    public ulong FromOffset { get; }

    /// <summary>Durable position for the next read on this selector.</summary>
    public ulong NextOffset { get; }

    /// <summary>Whether this is the last page of the current snapshot.</summary>
    public bool CaughtUp { get; }
}
