using System;
using System.Text;

// This file is compiled into more than one assembly (Cntryl.Fitz.Core and the analyzer, which
// targets netstandard2.0). It is the single definition of the Fitz route grammar, so it must
// stay a pure function of strings and use only APIs that exist on netstandard2.0.
namespace Cntryl.Fitz;

enum RouteValidationFailure
{
    InvalidScheme,
    EmptySegment,
    InvalidShape,
    ContainsWildcard,
}

enum StreamSelectorScope
{
    Resource,
    Area,
    Realm,
    Global,
}

/// <summary>
/// The Fitz route grammar: <c>scheme://segment/segment/...</c>, where <c>*</c> matches one
/// segment and <c>**</c> matches any number of them.
/// </summary>
static class RouteGrammar
{
    const string SchemeSeparator = "://";

    internal static string[] Segments(string route)
    {
        var marker = route.IndexOf(SchemeSeparator, StringComparison.Ordinal);
        return marker < 1 ? [] : route.Substring(marker + SchemeSeparator.Length).Split('/');
    }

    internal static bool TryValidateConcreteRoute(string route, string scheme, out RouteValidationFailure failure) =>
        TryScan(route, scheme, expectedSegments: 0, allowWildcards: false, allowRealmWildcard: false, out failure);

    internal static bool TryValidateFixedRoute(string route, string scheme, int segmentCount, out RouteValidationFailure failure) =>
        TryScan(route, scheme, segmentCount, allowWildcards: false, allowRealmWildcard: false, out failure);

    internal static bool TryValidateSelectorRoute(
        string route,
        string scheme,
        int segmentCount,
        bool allowRealmWildcard,
        out RouteValidationFailure failure) =>
        TryScan(route, scheme, segmentCount, allowWildcards: true, allowRealmWildcard, out failure);

    internal static bool TryGetStreamSelectorScope(string route, out StreamSelectorScope scope)
    {
        scope = default;
        if (string.Equals(route, "stream://**", StringComparison.Ordinal))
        {
            scope = StreamSelectorScope.Global;
            return true;
        }

        if (!route.StartsWith("stream://", StringComparison.Ordinal))
        {
            return false;
        }

        var segments = Segments(route);
        foreach (var segment in segments)
        {
            if (segment.Length == 0)
            {
                return false;
            }
        }

        if (segments.Length == 2)
        {
            if (segments[1] != "**" || ContainsWildcard(segments[0], 0, segments[0].Length))
            {
                return false;
            }

            scope = StreamSelectorScope.Realm;
            return true;
        }

        if (segments.Length != 3)
        {
            return false;
        }

        foreach (var segment in segments)
        {
            if (segment != "*" && ContainsWildcard(segment, 0, segment.Length))
            {
                return false;
            }
        }

        scope = segments[0] == "*"
            ? StreamSelectorScope.Global
            : segments[1] == "*"
                ? StreamSelectorScope.Realm
                : segments[2] == "*"
                    ? StreamSelectorScope.Area
                    : StreamSelectorScope.Resource;
        return true;
    }

    internal static bool TryValidateRegistrationPattern(
        string route,
        string scheme,
        int requiredSegments,
        out RouteValidationFailure failure)
    {
        if (requiredSegments < 0)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var segmentCount = 0;
        var doubleWildcardCount = 0;
        var previousWasDoubleWildcard = false;
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

            var doubleWildcard = IsDoubleWildcard(route, segmentStart, length);
            if (!IsSingleWildcard(route, segmentStart, length) && !doubleWildcard &&
                ContainsWildcard(route, segmentStart, index))
            {
                failure = RouteValidationFailure.ContainsWildcard;
                return false;
            }

            if (doubleWildcard)
            {
                if (previousWasDoubleWildcard)
                {
                    failure = RouteValidationFailure.InvalidShape;
                    return false;
                }

                doubleWildcardCount++;
            }

            previousWasDoubleWildcard = doubleWildcard;
            segmentCount++;
            segmentStart = index + 1;
        }

        var canMatchRequiredDepth = requiredSegments == 0 ||
            (doubleWildcardCount == 0
                ? segmentCount == requiredSegments
                : segmentCount - doubleWildcardCount <= requiredSegments);
        failure = canMatchRequiredDepth ? default : RouteValidationFailure.InvalidShape;
        return canMatchRequiredDepth;
    }

    static bool TryScan(
        string route,
        string scheme,
        int expectedSegments,
        bool allowWildcards,
        bool allowRealmWildcard,
        out RouteValidationFailure failure)
    {
        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var segmentCount = 0;
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

            if (IsSingleWildcard(route, segmentStart, length) || IsDoubleWildcard(route, segmentStart, length))
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
                if (ContainsWildcard(route, segmentStart, index))
                {
                    failure = RouteValidationFailure.ContainsWildcard;
                    return false;
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

    static bool TryGetPathStart(string route, string scheme, out int pathStart, out RouteValidationFailure failure)
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
        if (route.Length <= schemeLength + 2 ||
            string.CompareOrdinal(route, 0, scheme, 0, schemeLength) != 0 ||
            route[schemeLength] != ':' || route[schemeLength + 1] != '/' || route[schemeLength + 2] != '/')
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

    static bool ContainsWildcard(string route, int start, int end)
    {
        for (var cursor = start; cursor < end; cursor++)
        {
            if (route[cursor] == '*')
            {
                return true;
            }
        }

        return false;
    }

    static bool IsAscii(string value)
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

    static bool IsSingleWildcard(string route, int start, int length) => length == 1 && route[start] == '*';

    static bool IsDoubleWildcard(string route, int start, int length) => length == 2 && route[start] == '*' && route[start + 1] == '*';
}
