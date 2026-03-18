using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Integration;

internal static class IntegrationFixture
{
    internal static bool IsEnabled()
    {
        return string.Equals(
            Environment.GetEnvironmentVariable("FITZ_DOTNET_RUN_INTEGRATION"),
            "1",
            StringComparison.Ordinal
        );
    }

    internal static string GetAnonymousWebSocketUrl()
    {
        return Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_WS_ADDR") ?? "ws://localhost:4190/ws";
    }

    internal static string GetAuthWebSocketUrl()
    {
        return Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_WS_ADDR") ?? "ws://localhost:4090/ws";
    }

    internal static string GetOutputPath()
    {
        var outputPath = Environment.GetEnvironmentVariable("CONFORMANCE_OUTPUT");
        if (!string.IsNullOrWhiteSpace(outputPath))
        {
            return outputPath;
        }

        return Path.Combine(AppContext.BaseDirectory, "conformance-results.json");
    }

    internal static Client CreateAnonymousClient(string url, TimeSpan? timeout = null)
    {
        return new Client(
            new ClientConfig(
                url,
                Timeout: timeout,
                AuthSettleDelay: TimeSpan.FromMilliseconds(100)
            )
        );
    }

    internal static Client CreateInvalidJwtClient(string url)
    {
        var secret = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") ?? "test-secret-key";
        var audience = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_AUDIENCE") ?? "fitz";
        var invalidToken = CreateInvalidSignatureJwt(secret, audience);

        return new Client(
            new ClientConfig(
                url,
                AuthSettleDelay: TimeSpan.FromMilliseconds(100),
                TokenProvider: _ => ValueTask.FromResult(invalidToken)
            )
        );
    }

    internal static string CreateUniqueRoute(string prefix)
    {
        return $"{prefix}://conformance-realm/{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}";
    }

    internal static async Task WriteAggregateAsync(AggregateResult aggregate)
    {
        var outputPath = GetOutputPath();
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
}
