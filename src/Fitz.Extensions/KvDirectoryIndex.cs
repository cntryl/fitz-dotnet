using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions;

/// <summary>Declares one covering, byte-ordered secondary index generation.</summary>
/// <typeparam name="T">Directory entity type.</typeparam>
public sealed class KvDirectoryIndex<T>
{
    readonly Func<T, LexKeyPart[]> _key;

    /// <summary>Creates an index generation.</summary>
    public KvDirectoryIndex(string name, uint generation, Func<T, LexKeyPart[]> key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(key);
        if (generation == 0)
            throw new ArgumentOutOfRangeException(nameof(generation), "An index generation must be positive.");
        Name = name;
        Generation = generation;
        _key = key;
    }

    /// <summary>Gets the stable logical index name.</summary>
    public string Name { get; }

    /// <summary>Gets this index's independently migratable generation.</summary>
    public uint Generation { get; }

    /// <summary>Starts a forward query against this generation.</summary>
    public KvDirectoryQuery<T> Query() => new(this);

    internal LexKeyPart[] Select(T value) =>
        _key(value) ?? throw new InvalidOperationException($"Index '{Name}' returned a null key.");
}
