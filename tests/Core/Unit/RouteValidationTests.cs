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
}
