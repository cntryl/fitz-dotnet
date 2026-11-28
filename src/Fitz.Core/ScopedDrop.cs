using System.Text;

namespace Cntryl.Fitz;

static class ScopedDrop
{
    internal static void Validate(string selector, string scheme)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var prefix = scheme + "://";
        var hasPrefix = selector.StartsWith(prefix, StringComparison.Ordinal);
        var path = hasPrefix ? selector[prefix.Length..] : selector;
        var segments = path.Split('/');
        var validScope = segments switch
        {
            [var realm, "*"] => IsLiteral(realm),
            [var realm, var area, var resource] =>
                IsLiteral(realm) && IsLiteral(area) && (resource == "*" || IsLiteral(resource)) &&
                (scheme != "stream" || (area != "__realm__" && resource != "__area__")),
            _ => false,
        };

        // Realm shorthand expands to a three-segment authorization route on the broker.
        var routeBytes = Encoding.UTF8.GetByteCount(path) + prefix.Length + (segments.Length == 2 ? 2 : 0);

        if (!validScope || routeBytes > 4096 || selector.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Expected a {scheme} DROP selector in realm/*, realm/area/*, or realm/area/resource form.",
                nameof(selector));
        }
    }

    static bool IsLiteral(string segment) => segment.Length > 0 && !segment.Contains('*', StringComparison.Ordinal);
}
