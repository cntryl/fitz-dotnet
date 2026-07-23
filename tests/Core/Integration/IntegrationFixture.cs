using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Observability;
using System.Diagnostics;

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

        return Path.GetFullPath(Path.Combine(GetRepositoryRoot(), "conformance", "cross-language-conformance-suite.yaml"));
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

    internal static string GetBrokerComposePath()
    {
        var configured = Environment.GetEnvironmentVariable("FITZ_BROKER_COMPOSE_PATH");
        if (!string.IsNullOrWhiteSpace(configured))
        {
            return Path.IsPathRooted(configured)
                ? configured
                : Path.GetFullPath(Path.Combine(GetRepositoryRoot(), configured));
        }

        return Path.GetFullPath(Path.Combine(GetRepositoryRoot(), "compose.yml"));
    }

    internal static async Task RestartBrokerForModeAsync(string transport, string authMode, CancellationToken cancellationToken = default)
    {
        var serviceName = GetBrokerServiceName(authMode);
        await RunProcessAsync("docker", $"compose -f \"{GetBrokerComposePath()}\" restart {serviceName}", cancellationToken).ConfigureAwait(false);
        await WaitForBrokerReadyAsync(transport, authMode, TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
    }

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
        int? maxInFlightRequests = null,
        FitzObservabilityOptions? observability = null,
        AsyncHandlerOptions? asyncHandlers = null)
    {
        return CreateClient(url, transport, timeout, reconnect, tokenProvider: null, maxInFlightRequests, observability, asyncHandlers);
    }

    internal static Client CreateValidJwtClient(
        string url,
        string? transport = null,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null,
        FitzObservabilityOptions? observability = null,
        AsyncHandlerOptions? asyncHandlers = null)
    {
        var secret = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") ?? "dev-test-secret";
        var audience = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_AUDIENCE") ?? "fitz";
        var validToken = CreateTestJwt(secret, audience, DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds());

        return CreateClient(
            url,
            transport,
            timeout,
            reconnect,
            _ => ValueTask.FromResult(validToken),
            maxInFlightRequests,
            observability,
            asyncHandlers);
    }

    internal static Client CreateInvalidJwtClient(
        string url,
        string? transport = null,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null,
        FitzObservabilityOptions? observability = null,
        AsyncHandlerOptions? asyncHandlers = null)
    {
        var secret = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") ?? "dev-test-secret";
        var audience = Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_AUDIENCE") ?? "fitz";
        var invalidToken = CreateInvalidSignatureJwt(secret, audience);

        return CreateClient(
            url,
            transport,
            timeout,
            reconnect,
            _ => ValueTask.FromResult(invalidToken),
            maxInFlightRequests,
            observability,
            asyncHandlers);
    }

    internal static Client CreateClientForMode(
        string transport,
        string authMode,
        TimeSpan? timeout = null,
        ReconnectOptions? reconnect = null,
        int? maxInFlightRequests = null,
        FitzObservabilityOptions? observability = null,
        AsyncHandlerOptions? asyncHandlers = null)
    {
        var url = GetBrokerUrl(transport, authMode);
        return authMode.ToLowerInvariant() switch
        {
            "valid_jwt" => CreateValidJwtClient(url, transport, timeout, reconnect, maxInFlightRequests, observability, asyncHandlers),
            "invalid_jwt" => CreateInvalidJwtClient(url, transport, timeout, reconnect, maxInFlightRequests, observability, asyncHandlers),
            _ => CreateAnonymousClient(url, transport, timeout, reconnect, maxInFlightRequests, observability, asyncHandlers)
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
        int? maxInFlightRequests = null,
        FitzObservabilityOptions? observability = null,
        AsyncHandlerOptions? asyncHandlers = null)
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
                MaxInFlightRequests: maxInFlightRequests ?? 256,
                TokenProvider: tokenProvider,
                Reconnect: reconnect,
                Observability: observability,
                AsyncHandlers: asyncHandlers
            )
        );
    }

    private static string NormalizeTransport(string? transport)
    {
        return string.Equals(transport, "tcp", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
    }

    private static string CreateInvalidSignatureJwt(string secret, string audience)
    {
        return CreateTestJwt($"{secret}-invalid", audience, GetJwtTenant(), DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds());
    }

    private static string CreateTestJwt(string secret, string audience, long expiresAtSeconds)
    {
        return CreateTestJwt(secret, audience, GetJwtTenant(), expiresAtSeconds);
    }

    private static string CreateTestJwt(string secret, string audience, string tenant, long expiresAtSeconds)
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
                    sub = tenant,
                    tid = tenant,
                    exp = expiresAtSeconds,
                    iat = now,
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

    private static string GetJwtTenant()
    {
        return Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_TENANT") ?? "dev";
    }

    private static string GetBrokerServiceName(string authMode)
    {
        return string.Equals(authMode, "valid_jwt", StringComparison.OrdinalIgnoreCase)
            || string.Equals(authMode, "invalid_jwt", StringComparison.OrdinalIgnoreCase)
            ? "fitz-auth"
            : "fitz-anon";
    }

    private static async Task WaitForBrokerReadyAsync(string transport, string authMode, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        Exception? lastError = null;

        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await using var probe = CreateClientForMode(
                    transport,
                    authMode,
                    timeout: TimeSpan.FromSeconds(2),
                    reconnect: new ReconnectOptions(false));
                await probe.ConnectWhenReadyAsync(
                    new ConnectWhenReadyOptions(
                        Timeout: TimeSpan.FromSeconds(2),
                        Backoff: TimeSpan.FromMilliseconds(100),
                        MaxBackoff: TimeSpan.FromMilliseconds(250)),
                    cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                await Task.Delay(250, cancellationToken).ConfigureAwait(false);
            }
        }

        throw new TimeoutException($"Broker '{GetBrokerServiceName(authMode)}' did not become ready within {timeout.TotalSeconds:F0}s. Last error: {lastError?.Message}");
    }

    private static async Task RunProcessAsync(string fileName, string arguments, CancellationToken cancellationToken)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            }
        };

        if (!process.Start())
        {
            throw new InvalidOperationException($"Failed to start process '{fileName} {arguments}'.");
        }

        var stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);

        await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

        var stdout = await stdoutTask.ConfigureAwait(false);
        var stderr = await stderrTask.ConfigureAwait(false);
        if (process.ExitCode == 0)
        {
            return;
        }

        var details = string.Join(Environment.NewLine, new[] { stdout.Trim(), stderr.Trim() }.Where(value => !string.IsNullOrWhiteSpace(value)));
        throw new InvalidOperationException($"Process '{fileName} {arguments}' failed with exit code {process.ExitCode}.{Environment.NewLine}{details}".TrimEnd());
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
