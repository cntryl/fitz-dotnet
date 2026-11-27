using System.Net.WebSockets;
using Amazon;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Azure.Core;
using Azure.Identity;
using Cntryl.Fitz;
using Cntryl.Fitz.Observability;
using AwsClientException = Amazon.Runtime.AmazonClientException;
using AwsServiceException = Amazon.Runtime.AmazonServiceException;

if (args.Length < 3 || !Uri.TryCreate(args[1], UriKind.Absolute, out var endpoint)
    || endpoint.Scheme != "wss" || endpoint.UserInfo.Length != 0)
{
    return Usage();
}

if (args is ["aws", _, var region, var audience]
    && !string.IsNullOrWhiteSpace(region) && !region.Equals("aws-global", StringComparison.OrdinalIgnoreCase)
    && !string.IsNullOrWhiteSpace(audience))
{
    using var sts = new AmazonSecurityTokenServiceClient(RegionEndpoint.GetBySystemName(region));
    return await RunAsync(endpoint, ct => GetAwsTokenAsync(sts, audience, ct)).ConfigureAwait(false);
}

if (args[0] == "azure" && args.Length is 3 or 4
    && args[2].EndsWith("/.default", StringComparison.Ordinal)
    && (args.Length == 3 || Guid.TryParse(args[3], out _)))
{
    var identity = args.Length == 3 ? ManagedIdentityId.SystemAssigned : ManagedIdentityId.FromUserAssignedClientId(args[3]);
    var credential = new ManagedIdentityCredential(identity);
    var request = new TokenRequestContext([args[2]]);
    return await RunAsync(endpoint, ct => GetAzureTokenAsync(credential, request, ct)).ConfigureAwait(false);
}

if (args is ["kubernetes", _, var path] && Path.IsPathFullyQualified(path))
{
    return await RunAsync(endpoint, ct => ReadProjectedTokenAsync(path, ct)).ConfigureAwait(false);
}

return Usage();

static int Usage()
{
    Console.Error.WriteLine("""
        Usage:
          aws wss://broker/ws <region> <audience>
          azure wss://broker/ws <broker-app-id-uri>/.default [managed-identity-client-id]
          kubernetes wss://broker/ws <absolute-projected-token-file>
        """);
    return 2;
}

static async Task<int> RunAsync(Uri endpoint, Func<CancellationToken, ValueTask<string>> provider)
{
    using var shutdown = new CancellationTokenSource();
    Console.CancelKeyPress += Cancel;

    try
    {
        var client = new FitzClient(new ClientConfig(endpoint)
        {
            TokenProvider = provider,
            Observability = new FitzObservabilityOptions(OnLifecycleEvent: update =>
            {
                if (update.Event == "reconnect_failed")
                    Console.Error.WriteLine("Fitz reconnect attempt failed.");
            }),
        });
        await using var clientLifetime = client.ConfigureAwait(false);
        await client.ConnectAsync(shutdown.Token).ConfigureAwait(false);
        Console.WriteLine("Connected to Fitz. Press Ctrl+C to close.");
        await Task.Delay(Timeout.InfiniteTimeSpan, shutdown.Token).ConfigureAwait(false);
        return 0;
    }
    catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
    {
        return 0;
    }
    catch (Exception error) when (error is FitzException or InvalidOperationException or WebSocketException or OperationCanceledException
        or IOException or UnauthorizedAccessException or AuthenticationFailedException
        or AwsServiceException or AwsClientException)
    {
        await Console.Error.WriteLineAsync("Fitz connection failed. Check the endpoint, identity configuration, and broker grant.")
            .ConfigureAwait(false);
        return 1;
    }
    finally
    {
        Console.CancelKeyPress -= Cancel;
    }

    void Cancel(object? sender, ConsoleCancelEventArgs args)
    {
        args.Cancel = true;
        shutdown.Cancel();
    }
}

static async ValueTask<string> GetAwsTokenAsync(IAmazonSecurityTokenService sts, string audience, CancellationToken ct)
{
    var response = await sts.GetWebIdentityTokenAsync(new GetWebIdentityTokenRequest
    {
        Audience = [audience],
        SigningAlgorithm = "RS256",
        DurationSeconds = 300,
    }, ct).ConfigureAwait(false);

    return RequireFreshToken(response.WebIdentityToken, response.Expiration);
}

static async ValueTask<string> GetAzureTokenAsync(ManagedIdentityCredential credential, TokenRequestContext request, CancellationToken ct)
{
    var response = await credential.GetTokenAsync(request, ct).ConfigureAwait(false);
    return RequireFreshToken(response.Token, response.ExpiresOn);
}

static async ValueTask<string> ReadProjectedTokenAsync(string path, CancellationToken ct)
{
    var token = (await File.ReadAllTextAsync(path, ct).ConfigureAwait(false)).Trim();
    return string.IsNullOrEmpty(token)
        ? throw new InvalidOperationException("The projected ServiceAccount token file is empty.")
        : token;
}

static string RequireFreshToken(string? token, DateTimeOffset? expiresOn)
{
    if (string.IsNullOrWhiteSpace(token) || expiresOn is null
        || expiresOn <= TimeProvider.System.GetUtcNow().AddSeconds(30))
    {
        throw new InvalidOperationException("The identity provider returned no usable token.");
    }

    return token;
}
