using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Integration;

internal static class IntegrationFixture
{
    internal static ConformanceRunConfig GetConformanceRunConfig()
    {
        var transport = GetConformanceTransport();
        var authMode = GetConformanceAuthMode();
        return new ConformanceRunConfig(
            GetConformanceSuitePath(),
            GetConformanceClientName(),
            transport,
            authMode,
            GetConformanceBrokerAddress(transport, authMode),
            GetOutputPath(),
            GetConformanceTimeoutScale(),
            GetConformanceReconnectEnabledOverride(),
            GetConformanceSeed()
        );
    }

    internal static string GetConformanceTransport()
    {
        var configured = Environment.GetEnvironmentVariable("CONFORMANCE_TRANSPORT");
        return string.Equals(configured, "tcp", StringComparison.OrdinalIgnoreCase) ? "tcp" : "websocket";
    }

    internal static string GetConformanceAuthMode()
    {
        return (Environment.GetEnvironmentVariable("CONFORMANCE_AUTH_MODE") ?? "anonymous").ToLowerInvariant();
    }

    internal static string GetBrokerUrl(string transport, string authMode)
    {
        var configured = Environment.GetEnvironmentVariable("CONFORMANCE_BROKER_ADDR") ?? Environment.GetEnvironmentVariable("CONFORMANCE_BROKER_ADDRESS");
        if (!string.IsNullOrWhiteSpace(configured))
        {
            return configured;
        }

        var normalizedTransport = NormalizeTransport(transport);
        var normalizedAuthMode = authMode.ToLowerInvariant();

        return (normalizedTransport, normalizedAuthMode) switch
        {
            ("ws", "valid_jwt") => Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_WS_ADDR") ?? "ws://localhost:4090/ws",
            ("ws", "invalid_jwt") => Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_WS_ADDR") ?? "ws://localhost:4090/ws",
            ("ws", _) => Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_WS_ADDR") ?? "ws://localhost:4190/ws",
            ("websocket", "valid_jwt") => Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_WS_ADDR") ?? "ws://localhost:4090/ws",
            ("websocket", "invalid_jwt") => Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_WS_ADDR") ?? "ws://localhost:4090/ws",
            ("websocket", _) => Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_WS_ADDR") ?? "ws://localhost:4190/ws",
            ("tcp", "valid_jwt") => Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_TCP_ADDR") ?? "localhost:4091",
            ("tcp", "invalid_jwt") => Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_TCP_ADDR") ?? "localhost:4091",
            ("tcp", _) => Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_TCP_ADDR") ?? "localhost:4191",
            _ => throw new NotSupportedException($"Unsupported transport '{transport}'.")
        };
    }

    internal static string GetAnonymousWebSocketUrl() => GetBrokerUrl("websocket", "anonymous");

    internal static string GetAuthWebSocketUrl() => GetBrokerUrl("websocket", "valid_jwt");

    internal static string GetConformanceSuitePath()
    {
        var configured = Environment.GetEnvironmentVariable("CONFORMANCE_SUITE_PATH");
        if (!string.IsNullOrWhiteSpace(configured))
        {
            return Path.IsPathRooted(configured)
                ? configured
                : Path.GetFullPath(Path.Combine(GetRepositoryRoot(), configured));
        }

        return Path.GetFullPath(Path.Combine(GetRepositoryRoot(), "..", "fitz", "docs", "clients", "cross-language-conformance-suite.yaml"));
    }

    internal static string GetConformanceClientName()
    {
        return Environment.GetEnvironmentVariable("CONFORMANCE_CLIENT_NAME") ?? "fitz-dotnet";
    }

    internal static double GetConformanceTimeoutScale()
    {
        var configured = Environment.GetEnvironmentVariable("CONFORMANCE_TIMEOUT_SCALE");
        return double.TryParse(configured, out var parsed) && parsed > 0 ? parsed : 1.0;
    }

    internal static bool? GetConformanceReconnectEnabledOverride()
    {
        var configured = Environment.GetEnvironmentVariable("CONFORMANCE_RECONNECT_ENABLED_OVERRIDE");
        if (string.IsNullOrWhiteSpace(configured))
        {
            return null;
        }

        return bool.TryParse(configured, out var parsed) ? parsed : null;
    }

    internal static int? GetConformanceSeed()
    {
        var configured = Environment.GetEnvironmentVariable("CONFORMANCE_SEED");
        return int.TryParse(configured, out var parsed) ? parsed : null;
    }

    internal static string GetConformanceBrokerAddress(string transport, string authMode) => GetBrokerUrl(transport, authMode);

    internal static string GetOutputPath()
    {
        var outputPath = Environment.GetEnvironmentVariable("CONFORMANCE_OUTPUT");
        if (!string.IsNullOrWhiteSpace(outputPath))
        {
            return Path.IsPathRooted(outputPath)
                ? outputPath
                : Path.Combine(GetRepositoryRoot(), outputPath);
        }

        return Path.Combine(GetRepositoryRoot(), "artifacts", "conformance-results.json");
    }

    internal static Client CreateAnonymousClient(
        string url,
        string? transport = null,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null)
    {
        return CreateClient(url, transport, timeout, reconnect, tokenProvider: null, maxInFlightRequests);
    }

    internal static Client CreateValidJwtClient(
        string url,
        string? transport = null,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null)
    {
        var secret = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") ?? "test-secret-key";
        var audience = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_AUDIENCE") ?? "fitz";
        var validToken = CreateTestJwt(secret, audience, DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds());

        return CreateClient(
            url,
            transport,
            timeout,
            reconnect,
            _ => ValueTask.FromResult(validToken),
            maxInFlightRequests);
    }

    internal static Client CreateInvalidJwtClient(
        string url,
        string? transport = null,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null)
    {
        var secret = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") ?? "test-secret-key";
        var audience = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_AUDIENCE") ?? "fitz";
        var invalidToken = CreateInvalidSignatureJwt(secret, audience);

        return CreateClient(
            url,
            transport,
            timeout,
            reconnect,
            _ => ValueTask.FromResult(invalidToken),
            maxInFlightRequests);
    }

    internal static Client CreateClientForMode(
        string transport,
        string authMode,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null)
    {
        var url = GetBrokerUrl(transport, authMode);
        return authMode.ToLowerInvariant() switch
        {
            "valid_jwt" => CreateValidJwtClient(url, transport, timeout, reconnect, maxInFlightRequests),
            "invalid_jwt" => CreateInvalidJwtClient(url, transport, timeout, reconnect, maxInFlightRequests),
            _ => CreateAnonymousClient(url, transport, timeout, reconnect, maxInFlightRequests)
        };
    }

    internal static string CreateUniqueRoute(string prefix)
    {
        var resource = $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}";

        return prefix == "schedule"
            ? $"{prefix}://conformance-realm/integration/{resource}/run"
            : $"{prefix}://conformance-realm/integration/{resource}";
    }

    internal static async Task WriteAggregateAsync(AggregateResult aggregate, string? outputPath = null)
    {
        outputPath ??= GetOutputPath();
        var outputDir = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrWhiteSpace(outputDir))
        {
            Directory.CreateDirectory(outputDir);
        }

        await File.WriteAllTextAsync(
            outputPath,
            JsonSerializer.Serialize(aggregate, new JsonSerializerOptions { WriteIndented = true })
        );
    }

    internal static string DescribeAuthFailure(Exception ex)
    {
        return ex is AuthenticationException
            ? "error is typed AuthenticationException (ideal)"
            : "auth failure surfaced as non-typed exception";
    }

    private static Client CreateClient(
        string url,
        string? transport,
        TimeSpan? timeout,
        ReconnectOptions? reconnect,
        Func<CancellationToken, ValueTask<string>>? tokenProvider,
        int? maxInFlightRequests = null)
    {
        var normalizedTransport = NormalizeTransport(transport);
        var timeoutScale = GetConformanceTimeoutScale();
        TimeSpan? scaledTimeout = timeout is { } timeoutValue
            ? TimeSpan.FromMilliseconds(timeoutValue.TotalMilliseconds * timeoutScale)
            : null;

        var reconnectOverride = GetConformanceReconnectEnabledOverride();
        if (reconnectOverride is { } enabled)
        {
            reconnect = reconnect is null ? new ReconnectOptions(enabled) : reconnect with { Enabled = enabled };
        }

        return new Client(
            new ClientConfig(
                url,
                Transport: normalizedTransport,
                Timeout: scaledTimeout,
                AuthSettleDelay: TimeSpan.FromSeconds(5),
                MaxInFlightRequests: maxInFlightRequests ?? 256,
                TokenProvider: tokenProvider,
                Reconnect: reconnect
            )
        );
    }

    private static string NormalizeTransport(string? transport)
    {
        return string.Equals(transport, "tcp", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
    }

    private static string CreateInvalidSignatureJwt(string secret, string audience)
    {
        return CreateTestJwt($"{secret}-invalid", audience, DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds());
    }

    private static string CreateTestJwt(string secret, string audience, long expiresAtSeconds)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var header = Base64UrlEncode(
            JsonSerializer.SerializeToUtf8Bytes(
                new
                {
                    alg = "HS256",
                    typ = "JWT",
                }
            )
        );

        var payload = Base64UrlEncode(
            JsonSerializer.SerializeToUtf8Bytes(
                new
                {
                    iss = string.Empty,
                    aud = audience,
                    sub = "fitz-dotnet-tests",
                    tid = "fitz-dotnet-tests",
                    exp = expiresAtSeconds,
                    iat = now,
                    fitz = new
                    {
                        permissions = new[]
                        {
                            "kv://**#*",
                            "queue://**#*",
                            "notice://**#*",
                            "stream://**#*",
                            "rpc://**#*",
                            "lease://**#*",
                            "schedule://**#*",
                        },
                    },
                }
            )
        );

        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var signature = Base64UrlEncode(hmac.ComputeHash(Encoding.UTF8.GetBytes($"{header}.{payload}")));
        return $"{header}.{payload}.{signature}";
    }

    private static string Base64UrlEncode(byte[] value)
    {
        return Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    private static string GetRepositoryRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Fitz.sln")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return Directory.GetCurrentDirectory();
    }
}
