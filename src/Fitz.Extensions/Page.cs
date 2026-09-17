namespace Cntryl.Fitz.Extensions;

/// <summary>
/// One page of a list query. <see cref="NextCursor"/> is opaque and only meaningful passed back
/// as the next request's cursor; its absence means there is no further page.
/// </summary>
/// <remarks>
/// <see cref="NextCursor"/>'s consistency guarantee depends on whether the query was sorted.
/// Unsorted (key-order) listing uses a key-based cursor that is robust to concurrent writes
/// elsewhere in the range. Sorted listing uses an offset-based cursor that can skip or repeat
/// items if the underlying data changes between page fetches, because it re-sorts the whole
/// range on every call.
/// </remarks>
/// <typeparam name="T">The item type.</typeparam>
/// <param name="Items">The items in this page, in the query's effective order.</param>
/// <param name="NextCursor">An opaque cursor resuming after this page, or <see langword="null"/> when this is the last page.</param>
public sealed record Page<T>(IReadOnlyList<T> Items, string? NextCursor);
