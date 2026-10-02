using System.Runtime.CompilerServices;

namespace Cntryl.Fitz;

/// <summary>Provides complete-scan helpers for KV transactions.</summary>
public static class KvTransactionExtensions
{
    /// <summary>
    /// Enumerates every pair in a range while owning forward or reverse page continuation.
    /// </summary>
    /// <param name="transaction">Transaction used for every page.</param>
    /// <param name="query">Range, direction, and per-request limit.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The pairs in byte-lexicographic scan order.</returns>
    /// <exception cref="InvalidOperationException">
    /// A scan reports more data but returns no continuation key, or the broker does
    /// not advertise exclusive resume support for a reverse scan.
    /// </exception>
    /// <remarks>
    /// Pages are streamed as they arrive. If a later page fails, pairs from earlier pages
    /// have already been yielded to the caller.
    /// </remarks>
    public static async IAsyncEnumerable<KvPair> ScanAllAsync(
        this IKvTransaction transaction,
        KvScanQuery? query = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        query ??= new KvScanQuery();
        var next = query;

        while (true)
        {
            var page = await transaction.ScanAsync(next, ct).ConfigureAwait(false);
            foreach (var pair in page.Pairs)
            {
                ct.ThrowIfCancellationRequested();
                yield return pair;
            }

            if (!page.HasMore)
            {
                yield break;
            }
            if (page.Pairs.Count == 0)
            {
                throw new InvalidOperationException("KV SCAN returned an empty page with HasMore set.");
            }
            if (transaction.SupportsExclusiveScan)
            {
                next = query with { StartKey = page.Pairs[^1].Key, StartExclusive = true };
            }
            else if (query.Reverse)
            {
                throw new InvalidOperationException(
                    "This client cannot safely continue a reverse KV SCAN across pages.");
            }
            else
            {
                next = query with
                {
                    StartKey = After(page.Pairs[^1].Key.Span),
                    StartExclusive = false,
                };
            }
        }
    }

    static byte[] After(ReadOnlySpan<byte> key)
    {
        var result = new byte[key.Length + 1];
        key.CopyTo(result);
        return result;
    }
}
