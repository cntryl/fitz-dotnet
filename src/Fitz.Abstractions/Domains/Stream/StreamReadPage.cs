using System.Collections.Generic;

namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// One page of stream read results and the cursor that continues it.
/// </summary>
/// <param name="Items">
/// The page's items, in sequence order. An item may be a record or a filtered marker.
/// </param>
/// <param name="Cursor">Continuation state, including whether more records follow.</param>
public sealed record StreamReadPage(
    IReadOnlyList<StreamReadItem> Items,
    StreamReadCursor Cursor);
