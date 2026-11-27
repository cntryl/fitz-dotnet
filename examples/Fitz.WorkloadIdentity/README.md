# Fitz workload identity example

This .NET 11 application opens a Fitz connection using the selected workload identity and keeps
it open until Ctrl+C. Copy the selected `ClientConfig.TokenProvider` callback into your worker.
The callback runs on initial CONNECT and every reconnect. Provider dependencies live in this
example project. The Fitz SDK uses the same CONNECT format for these tokens as for other JWTs.
Each acquisition receives the connection attempt's cancellation token. Closing the client cancels
the underlying attempt. Canceling one caller's `ConnectAsync` wait leaves the shared attempt active.

## Broker prerequisite

Run this example against a broker configured to verify the provider's signature, issuer, dedicated
Fitz audience, and token claims. Bind the specific workload identity to the provisioned Fitz
route family and permissions it needs. Configure `realm` separately where applicable. AWS and
Entra provider verification and grant binding are tracked in
[Fitz #541](https://github.com/cntryl/fitz/issues/541). This example exercises client token
acquisition. Full federation depends on broker support and provider-backed qualification.

Use a `wss://` endpoint with a certificate trusted by the worker. The built-in `tcp://`
transport sends plaintext. Deployments using TCP for workload tokens need a separately
configured protected transport path. Treat tokens as secrets and keep them out of logs.

## AWS

Enable [IAM outbound identity federation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_outbound.html)
on the account and attach an IAM role to the worker. Grant that role
`sts:GetWebIdentityToken` for the dedicated Fitz audience. Use the assigned role through the
AWS SDK credential chain and keep static credentials or developer profiles out of the deployment.
The broker must trust the account's issuer and bind the exact role and principal claims.

```sh
dotnet run --project examples/Fitz.WorkloadIdentity -- aws \
  wss://fitz.example.com/ws us-east-1 https://fitz.example.com/worker
```

The AWS SDK v4 client uses the specified [regional STS endpoint](https://docs.aws.amazon.com/sdkref/latest/guide/feature-sts-regionalized-endpoints.html).
Each callback calls [GetWebIdentityToken](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetWebIdentityToken.html)
with that audience, RS256, and a 300-second lifetime. The callback passes Fitz's cancellation
token to STS and requires more than 30 seconds of remaining validity.

## Microsoft Entra

Assign a system-assigned or user-assigned managed identity to the worker. Configure a dedicated
broker API application in the trusted Entra tenant and grant the managed identity any required
application permissions. The broker must validate the app-only token and match the identity's
tenant and service principal object ID to the configured grant. Configure the broker audience
for the API's [access token version](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference).
The `/.default` suffix belongs to the scope requested by the client.

```sh
dotnet run --project examples/Fitz.WorkloadIdentity -- azure \
  wss://fitz.example.com/ws api://11111111-1111-1111-1111-111111111111/.default
```

Replace the example ID with the broker API application's ID. The callback requests the
broker API scope via [ManagedIdentityCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.identity.managedidentitycredential).
Pass the client ID of a user-assigned managed identity as a fourth argument to select it.
Reuse that credential for the worker's lifetime so the Azure SDK can manage its token cache and
refresh. The callback passes through cancellation and acquisition failures. It refuses a token
with 30 seconds or less remaining instead of sending it to Fitz.

## Kubernetes

Configure broker trust for this cluster's ServiceAccount token issuer and bind the intended
namespace and ServiceAccount to Fitz permissions before using this mode. Ask Kubernetes to
project a token specifically for the broker audience. For example, place the following fields
under your Pod's `spec` and add the mount to the worker container:

```yaml
serviceAccountName: fitz-worker
automountServiceAccountToken: false
volumes:
  - name: fitz-identity
    projected:
      sources:
        - serviceAccountToken:
            audience: https://fitz.example.com/worker
            expirationSeconds: 600
            path: token
# Under spec.containers[].volumeMounts:
# - name: fitz-identity
#   mountPath: /var/run/fitz/identity
#   readOnly: true
```

```sh
dotnet run --project examples/Fitz.WorkloadIdentity -- kubernetes \
  wss://fitz.example.com/ws /var/run/fitz/identity/token
```

Mount the whole projected directory without a `subPath` so that kubelet can rotate the token. The callback opens the file
on every attempt and rejects an empty projection. See the
[ServiceAccount token projection guide](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#serviceaccount-token-volume-projection).

## Local evidence

`WorkloadIdentityTests` sends synthetic callback tokens over the real WebSocket and TCP
transports to local peers. It checks the initial and rotated CONNECT payloads and checks that a
callback failure prevents CONNECT. The tests use no cloud credentials. Cloud acceptance requires
a broker deployment and actual issuer tokens.
