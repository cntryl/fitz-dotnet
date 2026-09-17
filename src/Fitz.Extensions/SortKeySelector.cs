namespace Cntryl.Fitz.Extensions;

/// <summary>Extracts one sortable field from a directory's value type.</summary>
/// <typeparam name="T">The directory's value type.</typeparam>
/// <param name="value">The value to extract a sort key from.</param>
/// <returns>The comparable sort key.</returns>
public delegate IComparable SortKeySelector<in T>(T value);
