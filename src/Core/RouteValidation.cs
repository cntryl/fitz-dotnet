using System;

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
        failure = RouteValidationFailure.InvalidScheme;

        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var index = pathStart;
        while (true)
        {
            var segmentEnd = route.IndexOf('/', index);
            if (segmentEnd < 0)
            {
                segmentEnd = route.Length;
            }

            if (segmentEnd == index)
            {
                failure = RouteValidationFailure.EmptySegment;
                return false;
            }

            if (IsWildcardSegment(route, index, segmentEnd - index))
            {
                failure = RouteValidationFailure.ContainsWildcard;
                return false;
            }

            if (segmentEnd == route.Length)
            {
                return true;
            }

            index = segmentEnd + 1;
        }
    }

    internal static bool TryValidateFixedRoute(string route, string scheme, int segmentCount, out RouteValidationFailure failure)
    {
        failure = RouteValidationFailure.InvalidScheme;

        if (segmentCount <= 0)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var index = pathStart;
        var seenSegments = 0;
        while (true)
        {
            var segmentEnd = route.IndexOf('/', index);
            if (segmentEnd < 0)
            {
                segmentEnd = route.Length;
            }

            if (segmentEnd == index)
            {
                failure = RouteValidationFailure.EmptySegment;
                return false;
            }

            if (IsWildcardSegment(route, index, segmentEnd - index))
            {
                failure = RouteValidationFailure.ContainsWildcard;
                return false;
            }

            seenSegments++;
            if (segmentEnd == route.Length)
            {
                break;
            }

            index = segmentEnd + 1;
        }

        if (seenSegments != segmentCount)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        return true;
    }

    internal static bool TryValidateSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard, out RouteValidationFailure failure)
    {
        failure = RouteValidationFailure.InvalidScheme;

        if (segmentCount <= 0)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var index = pathStart;
        var seenSegments = 0;
        var hasWildcard = false;
        var allPreviousSegmentsConcrete = true;
        var firstSegmentConcrete = false;
        var secondSegmentSingleWildcard = false;
        var thirdSegmentSingleWildcard = false;
        var lastSegmentSingleWildcard = false;

        while (true)
        {
            var segmentEnd = route.IndexOf('/', index);
            if (segmentEnd < 0)
            {
                segmentEnd = route.Length;
            }

            if (segmentEnd == index)
            {
                failure = RouteValidationFailure.EmptySegment;
                return false;
            }

            var segmentLength = segmentEnd - index;
            var isConcrete = IsConcreteSegment(route, index, segmentLength);
            var isSingleWildcard = IsSingleWildcard(route, index, segmentLength);
            var isDoubleWildcard = IsDoubleWildcard(route, index, segmentLength);

            if (seenSegments == 0)
            {
                firstSegmentConcrete = isConcrete;
            }
            else if (seenSegments == 1)
            {
                secondSegmentSingleWildcard = isSingleWildcard;
            }
            else if (seenSegments == 2)
            {
                thirdSegmentSingleWildcard = isSingleWildcard;
            }

            if (!isConcrete)
            {
                hasWildcard = true;
                if (seenSegments < segmentCount - 1)
                {
                    allPreviousSegmentsConcrete = false;
                }
            }

            if (isDoubleWildcard)
            {
                failure = RouteValidationFailure.InvalidShape;
                return false;
            }

            seenSegments++;
            lastSegmentSingleWildcard = isSingleWildcard;

            if (segmentEnd == route.Length)
            {
                break;
            }

            index = segmentEnd + 1;
        }

        if (seenSegments != segmentCount)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        if (!hasWildcard)
        {
            return true;
        }

        if (allowRealmWildcard &&
            segmentCount == 3 &&
            firstSegmentConcrete &&
            secondSegmentSingleWildcard &&
            thirdSegmentSingleWildcard)
        {
            return true;
        }

        if (!lastSegmentSingleWildcard || !allPreviousSegmentsConcrete)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

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