using System;
using System.Text;

namespace Cntryl.Fitz.Core;

internal enum RouteValidationFailure
{
    InvalidScheme,
    EmptySegment,
    InvalidShape,
    ContainsWildcard,
}

internal static class RouteValidation
{
    internal static bool IsConcreteRoute(string route, string scheme)
    {
        return TryValidateConcreteRoute(route, scheme, out _);
    }

    internal static bool IsFixedRoute(string route, string scheme, int segmentCount)
    {
        return TryValidateFixedRoute(route, scheme, segmentCount, out _);
    }

    internal static bool IsSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard)
    {
        return TryValidateSelectorRoute(route, scheme, segmentCount, allowRealmWildcard, out _);
    }

    internal static bool TryValidateConcreteRoute(string route, string scheme, out RouteValidationFailure failure)
    {
        return TryScan(route, scheme, expectedSegments: 0, allowWildcards: false, allowRealmWildcard: false, out _, out failure);
    }

    internal static bool TryValidateFixedRoute(string route, string scheme, int segmentCount, out RouteValidationFailure failure)
    {
        return TryScan(route, scheme, segmentCount, allowWildcards: false, allowRealmWildcard: false, out _, out failure);
    }

    internal static bool TryValidateSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard, out RouteValidationFailure failure)
    {
        return TryScan(route, scheme, segmentCount, allowWildcards: true, allowRealmWildcard, out _, out failure);
    }

    private static bool TryScan(
        string route,
        string scheme,
        int expectedSegments,
        bool allowWildcards,
        bool allowRealmWildcard,
        out int segmentCount,
        out RouteValidationFailure failure)
    {
        segmentCount = 0;
        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var firstWildcard = -1;
        var wildcardSuffix = true;
        var segmentStart = pathStart;
        for (var index = pathStart; index <= route.Length; index++)
        {
            if (index != route.Length && route[index] != '/')
            {
                continue;
            }

            var length = index - segmentStart;
            if (length == 0)
            {
                failure = RouteValidationFailure.EmptySegment;
                return false;
            }

            var wildcard = IsWildcardSegment(route, segmentStart, length);
            if (wildcard)
            {
                if (!allowWildcards || IsDoubleWildcard(route, segmentStart, length))
                {
                    failure = RouteValidationFailure.ContainsWildcard;
                    return false;
                }
                firstWildcard = firstWildcard < 0 ? segmentCount : firstWildcard;
            }
            else
            {
                for (var cursor = segmentStart; cursor < index; cursor++)
                {
                    if (route[cursor] == '*')
                    {
                        failure = RouteValidationFailure.ContainsWildcard;
                        return false;
                    }
                }
                wildcardSuffix &= firstWildcard < 0;
            }

            segmentCount++;
            segmentStart = index + 1;
        }

        if (expectedSegments > 0 && segmentCount != expectedSegments)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }
        if (firstWildcard == 0 || !wildcardSuffix)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }
        if (firstWildcard >= 0 && firstWildcard != segmentCount - 1 && !(allowRealmWildcard && firstWildcard == 1))
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        failure = default;
        return true;
    }

    private static bool TryGetPathStart(string route, string scheme, out int pathStart, out RouteValidationFailure failure)
    {
        pathStart = 0;
        failure = RouteValidationFailure.InvalidScheme;

        if (string.IsNullOrWhiteSpace(route) || string.IsNullOrWhiteSpace(scheme))
        {
            return false;
        }
        if (route.Length > ushort.MaxValue || (!IsAscii(route) && Encoding.UTF8.GetByteCount(route) > ushort.MaxValue))
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        var schemeLength = scheme.Length;
        if (route.Length <= schemeLength + 2)
        {
            return false;
        }

        if (!route.AsSpan(0, schemeLength).SequenceEqual(scheme))
        {
            return false;
        }

        if (route[schemeLength] != ':' || route[schemeLength + 1] != '/' || route[schemeLength + 2] != '/')
        {
            return false;
        }

        pathStart = schemeLength + 3;
        if (route.Length <= pathStart)
        {
            failure = RouteValidationFailure.EmptySegment;
            return false;
        }

        return true;
    }

    private static bool IsConcreteSegment(string route, int start, int length)
    {
        return !IsWildcardSegment(route, start, length);
    }

    private static bool IsAscii(string value)
    {
        foreach (var character in value)
        {
            if (character > 0x7f)
            {
                return false;
            }
        }
        return true;
    }

    private static bool IsWildcardSegment(string route, int start, int length)
    {
        return IsSingleWildcard(route, start, length) || IsDoubleWildcard(route, start, length);
    }

    private static bool IsSingleWildcard(string route, int start, int length)
    {
        return length == 1 && route[start] == '*';
    }

    private static bool IsDoubleWildcard(string route, int start, int length)
    {
        return length == 2 && route[start] == '*' && route[start + 1] == '*';
    }
}
