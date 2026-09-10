using Cntryl.Fitz;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RouteValidationTests
{
    [Theory]
    [InlineData("queue://realm/area/resource")]
    [InlineData("queue://opaque/shape/value")]
    public void ShouldAcceptFixedRouteShapesWithoutCheckingPermissions(string route) => Assert.True(RouteValidation.IsFixedRoute(route, "queue", 3));

    [Theory]
    [InlineData("notice://realm/area/resource")]
    [InlineData("queue://realm//resource")]
    [InlineData("queue://realm/area/*")]
    [InlineData("queue://realm/area/resource/extra")]
    public void ShouldRejectInvalidFixedRouteShapes(string route) => Assert.False(RouteValidation.IsFixedRoute(route, "queue", 3));

    [Theory]
    [InlineData("stream://realm/area/resource")]
    [InlineData("stream://realm/area/*")]
    [InlineData("stream://realm/*/*")]
    public void ShouldAcceptSupportedSelectorShapes(string route) => Assert.True(RouteValidation.IsSelectorRoute(route, "stream", 3, true));

    [Fact]
    public void ShouldRejectNonTerminalSelectorWildcards() => Assert.False(RouteValidation.IsSelectorRoute("stream://realm/*/resource", "stream", 3, true));

    [Fact]
    public void ShouldAcceptConcreteRouteGivenValidSchemeAndSegmentsWhenValidating() => Assert.True(RouteValidation.IsFixedRoute("queue://realm/area/resource", "queue", 3));

    [Fact]
    public void ShouldRejectWrongSchemeGivenDomainRouteWhenValidating() => Assert.False(RouteValidation.IsFixedRoute("notice://realm/area/resource", "queue", 3));

    [Fact]
    public void ShouldRejectEmptySegmentGivenEmptyRouteComponentWhenValidating() => Assert.False(RouteValidation.IsFixedRoute("queue://realm//resource", "queue", 3));

    [Fact]
    public void ShouldRejectIllegalWildcardPlacementGivenNonterminalWildcardWhenValidating() => Assert.False(RouteValidation.IsSelectorRoute("stream://realm/*/resource", "stream", 3, true));

    [Fact]
    public void ShouldRejectScheduleRouteWithObsoleteArityGivenThreeSegmentsWhenValidating() => Assert.False(RouteValidation.IsFixedRoute("schedule://realm/area/resource", "schedule", 4));

    [Theory]
    [InlineData("queue://realm/area/resource")]
    [InlineData("queue://realm/area/*")]
    [InlineData("queue://realm/**")]
    [InlineData("queue://*/area/resource")]
    [InlineData("queue://**/resource")]
    [InlineData("queue://**")]
    [InlineData("queue://**/renderers/**")]
    public void ShouldAcceptSharedRegistrationPatternsCapableOfMatchingDomainDepth(string pattern) => Assert.True(RouteValidation.IsRegistrationPattern(pattern, "queue", 3));

    [Theory]
    [InlineData("stream://realm/area/resource")]
    [InlineData("queue://realm//resource")]
    [InlineData("queue://realm/area/res*")]
    [InlineData("queue://realm/area")]
    [InlineData("queue://realm/area/resource/extra/**")]
    [InlineData("queue://realm/**/**")]
    [InlineData("queue://**/**")]
    [InlineData("queue://**/**/resource")]
    public void ShouldRejectInvalidSharedRegistrationPatterns(string pattern) => Assert.False(RouteValidation.IsRegistrationPattern(pattern, "queue", 3));

    [Fact]
    public void ShouldRejectAdjacentDoubleWildcardSegments() => Assert.False(RouteValidation.IsRegistrationPattern("lease://acme/**/**", "lease", 3));

    [Fact]
    public void ShouldAcceptDoubleWildcardsSeparatedByALiteralSegment() => Assert.True(RouteValidation.IsRegistrationPattern("lease://**/renderers/**", "lease", 3));

    [Theory]
    [InlineData("rpc://acme/orders/v1/create", "rpc://*/orders/**", true)]
    [InlineData("rpc://acme/orders/create", "rpc://acme/**/**", true)]
    [InlineData("rpc://acme/create", "rpc://acme/**/orders", false)]
    [InlineData("queue://acme/app/jobs", "stream://**", false)]
    public void ShouldMatchSharedRegistrationPatternsWithoutCrossingSchemes(
        string route,
        string pattern,
        bool expected) => Assert.Equal(expected, RouteValidation.MatchesPattern(route, pattern));
}
