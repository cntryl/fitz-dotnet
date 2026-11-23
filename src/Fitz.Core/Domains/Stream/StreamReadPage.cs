using System.Collections.Generic;

namespace Cntryl.Fitz.Domains.Stream;

/// <summary>
/// One page of stream read results and the cursor that continues it.
/// </summary>
/// <param name="Items">
/// The page's items, in sequence order. An item may be a record or a filtered marker.
/// </param>
/// <param name="Cursor">Continuation state, including whether more records follow.</param>
sealed record StreamReadPage(
    IReadOnlyList<StreamReadItem> Items,
    StreamReadCursor Cursor)
{
    internal ulong NextOffset(StreamSelectorScope scope, ulong from)
    {
        var last = scope switch
        {
            StreamSelectorScope.Resource => Cursor.LastResourceOffset,
            StreamSelectorScope.Area => Cursor.LastAreaOffset,
            StreamSelectorScope.Realm => Cursor.LastRealmOffset,
            StreamSelectorScope.Global => Cursor.LastGlobalOffset,
            _ => throw new ArgumentOutOfRangeException(nameof(scope)),
        };

        if (last is null || last < from ||
            (scope != StreamSelectorScope.Global && Items.Count == 0 && !Cursor.HasMore && last == from))
            return from;


        if (last == ulong.MaxValue)
            throw new StreamException("READ cursor exceeds the resumable offset range.", "READ_INVALID_RESPONSE");

        return last.Value + 1;
    }
}
