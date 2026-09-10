using System.Collections.Generic;

namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamReadPage(
    IReadOnlyList<StreamReadItem> Items,
    StreamReadCursor Cursor);
