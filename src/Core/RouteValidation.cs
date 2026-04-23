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
        _ = scheme;
        failure = RouteValidationFailure.InvalidShape;
        return !string.IsNullOrWhiteSpace(route);
    }

    internal static bool TryValidateFixedRoute(string route, string scheme, int segmentCount, out RouteValidationFailure failure)
    {
        _ = (scheme, segmentCount);
        failure = RouteValidationFailure.InvalidShape;
        return !string.IsNullOrWhiteSpace(route);
    }

    internal static bool TryValidateSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard, out RouteValidationFailure failure)
    {
        _ = (scheme, segmentCount, allowRealmWildcard);
        failure = RouteValidationFailure.InvalidShape;
        return !string.IsNullOrWhiteSpace(route);
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