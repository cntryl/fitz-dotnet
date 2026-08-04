using Cntryl.Fitz.Core;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RouteValidationTests
{
    [Theory]
    [InlineData("queue://realm/area/resource")]
    [InlineData("queue://opaque/shape/value")]
    public void should_accept_fixed_route_shapes_without_checking_permissions(string route)
    {
        Assert.True(RouteValidation.IsFixedRoute(route, "queue", 3));
    }

    [Theory]
    [InlineData("notice://realm/area/resource")]
    [InlineData("queue://realm//resource")]
    [InlineData("queue://realm/area/*")]
    [InlineData("queue://realm/area/resource/extra")]
    public void should_reject_invalid_fixed_route_shapes(string route)
    {
        Assert.False(RouteValidation.IsFixedRoute(route, "queue", 3));
    }

    [Theory]
    [InlineData("stream://realm/area/resource")]
    [InlineData("stream://realm/area/*")]
    [InlineData("stream://realm/*/*")]
    public void should_accept_supported_selector_shapes(string route)
    {
        Assert.True(RouteValidation.IsSelectorRoute(route, "stream", 3, true));
    }

    [Fact]
    public void should_reject_non_terminal_selector_wildcards()
    {
        Assert.False(RouteValidation.IsSelectorRoute("stream://realm/*/resource", "stream", 3, true));
    }

    [Fact]
    public void should_accept_concrete_route_given_valid_scheme_and_segments_when_validating()
    {
        Assert.True(RouteValidation.IsFixedRoute("queue://realm/area/resource", "queue", 3));
    }

    [Fact]
    public void should_reject_wrong_scheme_given_domain_route_when_validating()
    {
        Assert.False(RouteValidation.IsFixedRoute("notice://realm/area/resource", "queue", 3));
    }

    [Fact]
    public void should_reject_empty_segment_given_empty_route_component_when_validating()
    {
        Assert.False(RouteValidation.IsFixedRoute("queue://realm//resource", "queue", 3));
    }

    [Fact]
    public void should_reject_illegal_wildcard_placement_given_nonterminal_wildcard_when_validating()
    {
        Assert.False(RouteValidation.IsSelectorRoute("stream://realm/*/resource", "stream", 3, true));
    }

    [Fact]
    public void should_reject_schedule_route_with_obsolete_arity_given_three_segments_when_validating()
    {
        Assert.False(RouteValidation.IsFixedRoute("schedule://realm/area/resource", "schedule", 4));
    }

    [Theory]
    [InlineData("queue://realm/area/resource")]
    [InlineData("queue://realm/area/*")]
    [InlineData("queue://realm/**")]
    [InlineData("queue://*/area/resource")]
    [InlineData("queue://**/resource")]
    [InlineData("queue://realm/**/**")]
    public void should_accept_shared_registration_patterns_capable_of_matching_domain_depth(string pattern)
    {
        Assert.True(RouteValidation.IsRegistrationPattern(pattern, "queue", 3));
    }

    [Theory]
    [InlineData("stream://realm/area/resource")]
    [InlineData("queue://realm//resource")]
    [InlineData("queue://realm/area/res*")]
    [InlineData("queue://realm/area")]
    [InlineData("queue://realm/area/resource/extra/**")]
    public void should_reject_invalid_shared_registration_patterns(string pattern)
    {
        Assert.False(RouteValidation.IsRegistrationPattern(pattern, "queue", 3));
    }

    [Theory]
    [InlineData("rpc://acme/orders/v1/create", "rpc://*/orders/**", true)]
    [InlineData("rpc://acme/orders/create", "rpc://acme/**/**", true)]
    [InlineData("rpc://acme/create", "rpc://acme/**/orders", false)]
    [InlineData("queue://acme/app/jobs", "stream://**", false)]
    public void should_match_shared_registration_patterns_without_crossing_schemes(
        string route,
        string pattern,
        bool expected)
    {
        Assert.Equal(expected, RouteValidation.MatchesPattern(route, pattern));
    }
}
