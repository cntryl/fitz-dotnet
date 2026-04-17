using System;
using System.Linq;

namespace Cntryl.Fitz.Core;

internal static class RouteValidation
{
    internal static bool IsConcreteRoute(string route, string scheme)
    {
        return TryParseSegments(route, scheme, out var segments) && !ContainsWildcard(segments);
    }

    internal static bool IsFixedRoute(string route, string scheme, int segmentCount)
    {
        return TryParseSegments(route, scheme, out var segments) && segments.Length == segmentCount && !ContainsWildcard(segments);
    }

    internal static bool IsSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard)
    {
        if (!TryParseSegments(route, scheme, out var segments) || segments.Length != segmentCount)
        {
            return false;
        }

        if (!ContainsWildcard(segments))
        {
            return true;
        }

        if (allowRealmWildcard && segmentCount == 3 && IsConcreteSegment(segments[0]) && segments[1] == "*" && segments[2] == "*")
        {
            return true;
        }

        if (segments[^1] != "*")
        {
            return false;
        }

        return segments[..^1].All(IsConcreteSegment);
    }

    private static bool TryParseSegments(string route, string scheme, out string[] segments)
    {
        segments = Array.Empty<string>();

        if (string.IsNullOrWhiteSpace(route) || string.IsNullOrWhiteSpace(scheme))
        {
            return false;
        }

        var prefix = scheme + "://";
        if (!route.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var path = route.Substring(prefix.Length);
        if (path.Length == 0)
        {
            return false;
        }

        segments = path.Split('/');
        if (segments.Length == 0)
        {
            return false;
        }

        foreach (var segment in segments)
        {
            if (segment.Length == 0)
            {
                return false;
            }
        }

        return true;
    }

    private static bool ContainsWildcard(string[] segments)
    {
        return segments.Any(segment => !IsConcreteSegment(segment));
    }

    private static bool IsConcreteSegment(string segment)
    {
        return segment != "*" && segment != "**";
    }
}