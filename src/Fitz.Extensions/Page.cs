namespace Cntryl.Fitz.Extensions;

/// <summary>
/// One keyset-ordered directory page. <see cref="NextCursor"/> is opaque and bound to the route,
/// directory, and primary or index query shape that produced it.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
/// <param name="Items">The items in this page, in the query's effective order.</param>
/// <param name="NextCursor">An opaque cursor resuming after this page, or <see langword="null"/> when this is the last page.</param>
public sealed record Page<T>(IReadOnlyList<T> Items, string? NextCursor);
