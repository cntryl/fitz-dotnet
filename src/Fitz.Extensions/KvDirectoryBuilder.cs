using System.Text.Json.Serialization.Metadata;
using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions;

/// <summary>Starts the fluent definition of a <see cref="KvDirectory{T, TKey}"/>.</summary>
public static class KvDirectory
{
    /// <summary>
    /// Begins defining a directory. The builder collects the same inputs as the
    /// <see cref="KvDirectory{T, TKey}"/> constructor and calls it from
    /// <see cref="KvDirectoryBuilder{T, TKey}.Build"/>, so the two paths cannot disagree.
    /// </summary>
    /// <typeparam name="T">Stored entity type.</typeparam>
    /// <typeparam name="TKey">Strongly typed entity identity.</typeparam>
    /// <param name="name">Stable directory name. It is part of every key.</param>
    /// <param name="valueTypeInfo">Source-generated JSON metadata used to store and read entities.</param>
    /// <param name="identity">Selects an entity's identity.</param>
    /// <param name="identityKey">Maps an identity to its ordered key parts; must return at least one part.</param>
    /// <returns>A builder to add indexes and options to.</returns>
    public static KvDirectoryBuilder<T, TKey> Define<T, TKey>(
        string name,
        JsonTypeInfo<T> valueTypeInfo,
        Func<T, TKey> identity,
        Func<TKey, LexKeyPart[]> identityKey) => new(name, valueTypeInfo, identity, identityKey);
}

/// <summary>Collects the parts of a <see cref="KvDirectory{T, TKey}"/> definition.</summary>
/// <typeparam name="T">Stored entity type.</typeparam>
/// <typeparam name="TKey">Strongly typed entity identity.</typeparam>
public sealed class KvDirectoryBuilder<T, TKey>
{
    readonly string _name;
    readonly JsonTypeInfo<T> _valueTypeInfo;
    readonly Func<T, TKey> _identity;
    readonly Func<TKey, LexKeyPart[]> _identityKey;
    readonly List<KvDirectoryIndex<T>> _indexes = [];
    KvDirectoryOptions? _options;

    internal KvDirectoryBuilder(
        string name,
        JsonTypeInfo<T> valueTypeInfo,
        Func<T, TKey> identity,
        Func<TKey, LexKeyPart[]> identityKey)
    {
        _name = name;
        _valueTypeInfo = valueTypeInfo;
        _identity = identity;
        _identityKey = identityKey;
    }

    /// <summary>Adds an index generation that writes must maintain.</summary>
    /// <param name="index">The index to add.</param>
    /// <returns>This builder.</returns>
    public KvDirectoryBuilder<T, TKey> WithIndex(KvDirectoryIndex<T> index)
    {
        ArgumentNullException.ThrowIfNull(index);
        _indexes.Add(index);
        return this;
    }

    /// <summary>Sets the resource bounds.</summary>
    /// <param name="options">The bounds to use.</param>
    /// <returns>This builder.</returns>
    public KvDirectoryBuilder<T, TKey> WithOptions(KvDirectoryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        return this;
    }

    /// <summary>Creates the directory, validating the definition.</summary>
    /// <returns>The immutable directory.</returns>
    public KvDirectory<T, TKey> Build() =>
        new(_name, _valueTypeInfo, _identity, _identityKey, _indexes, _options);
}
