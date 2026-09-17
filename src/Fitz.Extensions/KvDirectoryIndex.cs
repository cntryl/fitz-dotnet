using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions;

/// <summary>Declares one covering, byte-ordered secondary index generation.</summary>
/// <typeparam name="T">Directory entity type.</typeparam>
public sealed class KvDirectoryIndex<T>
{
    readonly Func<T, IReadOnlyList<LexKeyPart[]>> _keys;

    /// <summary>Creates an index generation.</summary>
    public KvDirectoryIndex(string name, uint generation, Func<T, LexKeyPart[]> key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(key);
        if (generation == 0)
            throw new ArgumentOutOfRangeException(nameof(generation), "An index generation must be positive.");
        Name = name;
        Generation = generation;
        _keys = value => [key(value)];
    }

    internal KvDirectoryIndex(string name, uint generation, Func<T, IReadOnlyList<LexKeyPart[]>> keys)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(keys);
        if (generation == 0)
            throw new ArgumentOutOfRangeException(nameof(generation), "An index generation must be positive.");
        Name = name;
        Generation = generation;
        _keys = keys;
    }

    /// <summary>Gets the stable logical index name.</summary>
    public string Name { get; }

    /// <summary>Gets this index's independently migratable generation.</summary>
    public uint Generation { get; }

    /// <summary>Starts a forward query against this generation.</summary>
    public KvDirectoryQuery<T> Query() => new(this);

    internal IReadOnlyList<LexKeyPart[]> Select(T value) =>
        _keys(value) ?? throw new InvalidOperationException($"Index '{Name}' returned null keys.");
}

/// <summary>Creates specialized directory-index definitions.</summary>
public static class KvDirectoryIndex
{
    /// <summary>Creates an index generation in which one entity may produce several rows.</summary>
    public static KvDirectoryIndex<T> Many<T>(
        string name,
        uint generation,
        Func<T, IReadOnlyList<LexKeyPart[]>> keys) => new(name, generation, keys);
}
