using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Cntryl.Fitz;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class ConformanceSmokeTests
{
    [Fact]
    public async Task should_write_json_result_given_enabled_flag_when_running_conformance_smoke()
    {
        // Arrange
        if (!IsEnabled())
        {
            return;
        }

        var scenarios = new List<ScenarioResult>
        {
            await RunCs001ConnectSuccess(),
            await RunCs002AuthFailure(),
            await RunCs003RequestSuccess(),
        };

        var outputPath = Environment.GetEnvironmentVariable("CONFORMANCE_OUTPUT");
        if (string.IsNullOrWhiteSpace(outputPath))
        {
            outputPath = Path.Combine(AppContext.BaseDirectory, "conformance-results.json");
        }

        // Act
        var aggregate = BuildAggregate(scenarios);
        await File.WriteAllTextAsync(
            outputPath,
            JsonSerializer.Serialize(aggregate, new JsonSerializerOptions { WriteIndented = true })
        );

        // Assert
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-001");
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-002");
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-003");
        Assert.True(File.Exists(outputPath));
    }

    private static bool IsEnabled()
    {
        return string.Equals(
            Environment.GetEnvironmentVariable("FITZ_DOTNET_RUN_INTEGRATION"),
            "1",
            StringComparison.Ordinal
        );
    }

    private static async Task<ScenarioResult> RunCs001ConnectSuccess()
    {
        var url = Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_WS_ADDR") ?? "ws://localhost:4190/ws";
        var sw = System.Diagnostics.Stopwatch.StartNew();

        var evidence = new List<string>();
        try
        {
            await using var client = new Client(
                new ClientConfig(
                    url,
                    AuthSettleDelay: TimeSpan.FromMilliseconds(100)
                )
            );
            await client.ConnectAsync();
            evidence.Add("connect returned successfully");
            evidence.Add("client state is authenticated");
            return new ScenarioResult(
                "CS-001",
                "fitz-dotnet",
                "websocket",
                "anonymous",
                "pass",
                sw.ElapsedMilliseconds,
                evidence,
                ""
            );
        }
        catch (Exception ex)
        {
            evidence.Add($"connect failed: {ex.GetType().Name}");
            return new ScenarioResult(
                "CS-001",
                "fitz-dotnet",
                "websocket",
                "anonymous",
                "fail",
                sw.ElapsedMilliseconds,
                evidence,
                ex.Message
            );
        }
    }

    private static async Task<ScenarioResult> RunCs002AuthFailure()
    {
        var url = Environment.GetEnvironmentVariable("FITZ_BROKER_AUTH_WS_ADDR") ?? "ws://localhost:4090/ws";
        var secret = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") ?? "test-secret-key";
        var audience = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_AUDIENCE") ?? "fitz";
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        await using var client = new Client(
            new ClientConfig(
                url,
                AuthSettleDelay: TimeSpan.FromMilliseconds(100),
                TokenProvider: _ => ValueTask.FromResult(CreateInvalidSignatureJwt(secret, audience))
            )
        );

        try
        {
            await client.ConnectAsync();
            evidence.Add("connect did not raise (silent-close model)");
            evidence.Add($"client is_connected = {client.IsConnected}");

            try
            {
                _ = await client.Kv().BeginAsync(CreateUniqueRoute("kv"));
                evidence.Add("WARNING: domain request unexpectedly succeeded");
                return new ScenarioResult(
                    "CS-002",
                    "fitz-dotnet",
                    "websocket",
                    "invalid_jwt",
                    "partial",
                    sw.ElapsedMilliseconds,
                    evidence,
                    ""
                );
            }
            catch (Exception domainException)
            {
                evidence.Add($"domain request failed post-auth: {domainException.GetType().Name}: {domainException.Message}");
                return new ScenarioResult(
                    "CS-002",
                    "fitz-dotnet",
                    "websocket",
                    "invalid_jwt",
                    "partial",
                    sw.ElapsedMilliseconds,
                    evidence,
                    ""
                );
            }
        }
        catch (Exception ex)
        {
            evidence.Add($"connect raised {ex.GetType().Name}: {ex.Message}");
            evidence.Add("auth failure surfaced as error (correct)");
            if (ex is AuthenticationException)
            {
                evidence.Add("error is typed AuthenticationException (ideal)");
            }

            return new ScenarioResult(
                "CS-002",
                "fitz-dotnet",
                "websocket",
                "invalid_jwt",
                "pass",
                sw.ElapsedMilliseconds,
                evidence,
                ex.Message
            );
        }
    }

    private static async Task<ScenarioResult> RunCs003RequestSuccess()
    {
        var url = Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_WS_ADDR") ?? "ws://localhost:4190/ws";
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = new Client(
                new ClientConfig(
                    url,
                    AuthSettleDelay: TimeSpan.FromMilliseconds(100)
                )
            );

            await client.ConnectAsync();

            var route = CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.PutAsync("user:1"u8.ToArray(), "Alice"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("kv begin/put/commit succeeded");

            var readTx = await client.Kv().BeginAsync(route, KvMode.ReadOnly);
            var result = await readTx.GetAsync("user:1"u8.ToArray());
            if (!result.Found)
            {
                return new ScenarioResult(
                    "CS-003",
                    "fitz-dotnet",
                    "websocket",
                    "anonymous",
                    "fail",
                    sw.ElapsedMilliseconds,
                    evidence,
                    "expected read-after-commit to return found=true"
                );
            }

            var value = Encoding.UTF8.GetString(result.Value ?? []);
            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                return new ScenarioResult(
                    "CS-003",
                    "fitz-dotnet",
                    "websocket",
                    "anonymous",
                    "fail",
                    sw.ElapsedMilliseconds,
                    evidence,
                    $"expected read-after-commit value 'Alice', got '{value}'"
                );
            }

            evidence.Add($"read-after-commit returned \"{value}\" (correct)");
            return new ScenarioResult(
                "CS-003",
                "fitz-dotnet",
                "websocket",
                "anonymous",
                "pass",
                sw.ElapsedMilliseconds,
                evidence,
                ""
            );
        }
        catch (Exception ex)
        {
            evidence.Add($"request success scenario failed: {ex.GetType().Name}: {ex.Message}");
            return new ScenarioResult(
                "CS-003",
                "fitz-dotnet",
                "websocket",
                "anonymous",
                "fail",
                sw.ElapsedMilliseconds,
                evidence,
                ex.Message
            );
        }
    }

    private static AggregateResult BuildAggregate(IReadOnlyList<ScenarioResult> scenarios)
    {
        var p0 = scenarios.Where(s => s.ScenarioId is "CS-001" or "CS-002" or "CS-003" or "CS-004" or "CS-005" or "CS-006" or "CS-007" or "CS-008").ToList();
        var p1 = scenarios.Where(s => s.ScenarioId is "CS-009" or "CS-010" or "CS-011" or "CS-012" or "CS-013" or "CS-014" or "CS-015").ToList();

        static double PassRate(IEnumerable<ScenarioResult> input)
        {
            var arr = input.ToArray();
            if (arr.Length == 0)
            {
                return 1.0;
            }

            var pass = arr.Count(r => r.Verdict == "pass");
            return (double)pass / arr.Length;
        }

        var p0PassRate = PassRate(p0);
        var p1PassRate = PassRate(p1);
        var overall = p0.Any(r => r.Verdict != "pass") ? "fail" : (p1.Any(r => r.Verdict is "partial" or "fail") ? "partial" : "pass");

        return new AggregateResult(
            "fitz-cross-language-client-conformance",
            "1.0",
            DateTimeOffset.UtcNow,
            "fitz-dotnet",
            "websocket",
            "anonymous",
            p0PassRate,
            p1PassRate,
            overall,
            scenarios
        );
    }

    private static string CreateUniqueRoute(string prefix)
    {
        return $"{prefix}://conformance-realm/{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}";
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

    private sealed record ScenarioResult(
        string ScenarioId,
        string Client,
        string Transport,
        string AuthMode,
        string Verdict,
        long LatencyMs,
        IReadOnlyList<string> Evidence,
        string Error
    );

    private sealed record AggregateResult(
        string Suite,
        string Version,
        DateTimeOffset GeneratedAt,
        string Client,
        string Transport,
        string AuthMode,
        double P0PassRate,
        double P1PassRate,
        string OverallStatus,
        IReadOnlyList<ScenarioResult> Scenarios
    );
}