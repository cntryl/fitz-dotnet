namespace Cntryl.Fitz.Analyzers.Tests;

/// <summary>
/// The analyzer and the runtime client each validate routes through the shared grammar. These
/// tests pin that they hand back the same verdict for every scheme, so a change to one wrapper
/// cannot quietly drift from the other.
/// </summary>
public sealed class RouteGrammarParityTests
{
    // scheme, segments a route needs, segments a pattern needs (0 means any depth)
    static readonly (string Scheme, int RouteSegments, int PatternSegments)[] Schemes =
    [
        ("kv", 3, 3),
        ("queue", 3, 3),
        ("lease", 3, 3),
        ("notice", 3, 0),
        ("rpc", 0, 0),
        ("schedule", 4, 4),
        ("stream", 3, 3),
    ];

    static readonly string[] Templates =
    [
        "{s}://a",
        "{s}://a/b",
        "{s}://a/b/c",
        "{s}://a/b/c/d",
        "{s}://a/b/c/d/e",
        "{s}://*",
        "{s}://**",
        "{s}://a/*",
        "{s}://a/**",
        "{s}://*/b/c",
        "{s}://a/b/*",
        "{s}://a/*/c",
        "{s}://a/b/c/*",
        "{s}://a/*/*/d",
        "{s}://**/b",
        "{s}://a/**/c",
        "{s}://**/b/**",
        "{s}://**/**",
        "{s}://a/**/**",
        "{s}://**/**/c",
        "{s}://a//c",
        "{s}://a/b/",
        "{s}:///a",
        "{s}://",
        "{s}:/a/b/c",
        "{s}:a/b/c",
        "{s}//a/b/c",
        "{s}://a/b*/c",
        "{s}://a/*b/c",
        "{s}://a/b/c*",
        "{s}://a/***/c",
        "{s}://a/b/c/d/e/f/g/h",
        "{s}://caf\u00e9/b/c",
        "other://a/b/c",
        "{s}x://a/b/c",
        "a/b/c",
        "/a/b/c",
        "",
        " ",
    ];

    static readonly string[] StreamSelectors =
    [
        "stream://**",
        "stream://*",
        "stream://realm",
        "stream://realm/**",
        "stream://*/**",
        "stream://re*lm/**",
        "stream://realm/area",
        "stream://realm/area/resource",
        "stream://realm/area/*",
        "stream://realm/*/*",
        "stream://realm/*/resource",
        "stream://*/*/*",
        "stream://*/area/resource",
        "stream://realm/**/resource",
        "stream://realm/area/**",
        "stream://realm//*",
        "stream://realm/area/",
        "stream://realm/ar*a/*",
        "stream://realm/area/res*",
        "stream://a/b/c/d",
        "queue://realm/area/*",
        "stream:/realm/area/*",
        "",
    ];

    [Fact]
    public void ShouldAgreeWithCoreOnConcreteRoutesGivenCorpusWhenValidatingEveryScheme()
    {
        // Arrange
        var mismatches = new List<string>();
        var accepted = 0;
        var rejected = 0;

        // Act
        foreach (var (scheme, segments, _) in Schemes)
        {
            foreach (var route in Corpus(scheme))
            {
                var analyzer = FitzRouteRules.IsValidConcrete(route, scheme, segments);
                var core = segments == 0
                    ? RouteValidation.IsConcreteRoute(route, scheme)
                    : RouteValidation.IsFixedRoute(route, scheme, segments);
                Record(analyzer, core, $"route {scheme}/{segments}: '{route}'", mismatches, ref accepted, ref rejected);
            }
        }

        // Assert
        Assert.Empty(mismatches);
        Assert.True(accepted > 0 && rejected > 0, "The corpus must contain both valid and invalid routes.");
    }

    [Fact]
    public void ShouldAgreeWithCoreOnPatternsGivenCorpusWhenValidatingEveryScheme()
    {
        // Arrange
        var mismatches = new List<string>();
        var accepted = 0;
        var rejected = 0;

        // Act
        foreach (var (scheme, _, segments) in Schemes)
        {
            foreach (var pattern in Corpus(scheme))
            {
                var analyzer = FitzRouteRules.IsValidPattern(pattern, scheme, segments, streamSelector: false);
                var core = RouteValidation.IsRegistrationPattern(pattern, scheme, segments);
                Record(analyzer, core, $"pattern {scheme}/{segments}: '{pattern}'", mismatches, ref accepted, ref rejected);
            }
        }

        // Assert
        Assert.Empty(mismatches);
        Assert.True(accepted > 0 && rejected > 0, "The corpus must contain both valid and invalid patterns.");
    }

    [Fact]
    public void ShouldAgreeWithCoreOnStreamSelectorsGivenCorpusWhenValidating()
    {
        // Arrange
        var mismatches = new List<string>();
        var accepted = 0;
        var rejected = 0;

        // Act
        foreach (var selector in StreamSelectors)
        {
            var analyzer = FitzRouteRules.IsValidPattern(selector, "stream", 3, streamSelector: true);
            var core = RouteValidation.IsStreamSelector(selector);
            Record(analyzer, core, $"selector: '{selector}'", mismatches, ref accepted, ref rejected);
        }

        // Assert
        Assert.Empty(mismatches);
        Assert.True(accepted > 0 && rejected > 0, "The corpus must contain both valid and invalid selectors.");
    }

    [Theory]
    [InlineData("stream://realm//*")]
    [InlineData("stream://realm/area/")]
    [InlineData("stream://realm/ar*a/*")]
    public void ShouldRejectMalformedStreamSelectorGivenSegmentDefectWhenValidating(string selector)
    {
        // Arrange
        // Act
        var core = RouteValidation.IsStreamSelector(selector);
        var analyzer = FitzRouteRules.IsValidPattern(selector, "stream", 3, streamSelector: true);

        // Assert
        Assert.False(core);
        Assert.False(analyzer);
    }

    [Fact]
    public void ShouldPreserveSpecificFailureGivenInvalidRegistrationPatternWhenValidating()
    {
        // Arrange
        // Act
        var wrongScheme = RouteValidation.TryValidateRegistrationPattern("notice://a/b/c", "queue", 3, out var schemeFailure);
        var emptySegment = RouteValidation.TryValidateRegistrationPattern("queue://a//c", "queue", 3, out var emptyFailure);
        var partialWildcard = RouteValidation.TryValidateRegistrationPattern("queue://a/b*/c", "queue", 3, out var wildcardFailure);
        var wrongDepth = RouteValidation.TryValidateRegistrationPattern("queue://a/b", "queue", 3, out var depthFailure);

        // Assert
        Assert.False(wrongScheme);
        Assert.Equal(RouteValidationFailure.InvalidScheme, schemeFailure);
        Assert.False(emptySegment);
        Assert.Equal(RouteValidationFailure.EmptySegment, emptyFailure);
        Assert.False(partialWildcard);
        Assert.Equal(RouteValidationFailure.ContainsWildcard, wildcardFailure);
        Assert.False(wrongDepth);
        Assert.Equal(RouteValidationFailure.InvalidShape, depthFailure);
    }

    static IEnumerable<string> Corpus(string scheme) =>
        Templates.Select(template => template.Replace("{s}", scheme, StringComparison.Ordinal))
            .Append(scheme + "://" + new string('a', 70_000));

    static void Record(
        bool analyzer,
        bool core,
        string description,
        List<string> mismatches,
        ref int accepted,
        ref int rejected)
    {
        if (analyzer != core)
        {
            mismatches.Add($"{description} analyzer={analyzer} core={core}");
        }

        if (core)
        {
            accepted++;
        }
        else
        {
            rejected++;
        }
    }
}
