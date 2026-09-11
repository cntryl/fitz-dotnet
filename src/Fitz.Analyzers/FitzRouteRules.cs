using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace Cntryl.Fitz.Analyzers;

static class FitzRouteRules
{
    internal sealed class ApiSymbols
    {
        readonly ImmutableArray<ClientSymbol> _clients;
        readonly ImmutableArray<INamedTypeSymbol> _lifecycleHandles;
        readonly INamedTypeSymbol? _subscriptionHandle;

        internal ApiSymbols(Compilation compilation)
        {
            var clients = ImmutableArray.CreateBuilder<ClientSymbol>();
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Kv.IKvClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Queue.IQueueClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Lease.ILeaseClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Notice.INoticeClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Rpc.IRpcClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Schedule.IScheduleClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Abstractions.Domains.Stream.IStreamClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Kv.KvClient", "IKvClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Queue.QueueClient", "IQueueClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Lease.LeaseClient", "ILeaseClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Notice.NoticeClient", "INoticeClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Rpc.RpcClient", "IRpcClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Schedule.ScheduleClient", "IScheduleClient");
            AddClient(clients, compilation, "Cntryl.Fitz.Domains.Stream.StreamClient", "IStreamClient");
            _clients = clients.ToImmutable();

            var lifecycleHandles = ImmutableArray.CreateBuilder<INamedTypeSymbol>();
            AddType(lifecycleHandles, compilation, "Cntryl.Fitz.Abstractions.Domains.Kv.IKvTransaction");
            AddType(lifecycleHandles, compilation, "Cntryl.Fitz.Abstractions.Domains.Lease.ILease");
            AddType(lifecycleHandles, compilation, "Cntryl.Fitz.Abstractions.Domains.Lease.ILeaseInventoryObserver");
            AddType(lifecycleHandles, compilation, "Cntryl.Fitz.Abstractions.Domains.Rpc.RpcWorkerRegistration");
            AddType(lifecycleHandles, compilation, "Cntryl.Fitz.Abstractions.Domains.Stream.IStreamSession");
            _lifecycleHandles = lifecycleHandles.ToImmutable();
            _subscriptionHandle = compilation.GetTypeByMetadataName("Cntryl.Fitz.Runtime.SubscriptionHandle");
        }

        internal bool TryGetClientName(IMethodSymbol method, out string clientName)
        {
            var containingType = method.ContainingType.OriginalDefinition;
            foreach (var client in _clients)
            {
                if (SymbolEqualityComparer.Default.Equals(containingType, client.Symbol))
                {
                    clientName = client.ClientName;
                    return true;
                }
            }

            clientName = string.Empty;
            return false;
        }

        internal bool IsLifecycleHandle(ITypeSymbol type)
        {
            foreach (var lifecycleHandle in _lifecycleHandles)
            {
                if (SymbolEqualityComparer.Default.Equals(type.OriginalDefinition, lifecycleHandle))
                {
                    return true;
                }
            }

            if (_subscriptionHandle is null)
            {
                return false;
            }

            for (var current = type.BaseType; current is not null; current = current.BaseType)
            {
                if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, _subscriptionHandle))
                {
                    return true;
                }
            }

            return false;
        }

        static void AddClient(
            ImmutableArray<ClientSymbol>.Builder clients,
            Compilation compilation,
            string metadataName,
            string? clientName = null)
        {
            if (compilation.GetTypeByMetadataName(metadataName) is { } symbol)
            {
                clients.Add(new ClientSymbol(symbol, clientName ?? symbol.Name));
            }
        }

        static void AddType(
            ImmutableArray<INamedTypeSymbol>.Builder symbols,
            Compilation compilation,
            string metadataName)
        {
            if (compilation.GetTypeByMetadataName(metadataName) is { } symbol)
            {
                symbols.Add(symbol);
            }
        }

        readonly struct ClientSymbol
        {
            internal ClientSymbol(INamedTypeSymbol symbol, string clientName)
            {
                Symbol = symbol;
                ClientName = clientName;
            }

            internal INamedTypeSymbol Symbol { get; }

            internal string ClientName { get; }
        }
    }

    internal readonly struct Rule
    {
        internal Rule(string parameterName, string scheme, int requiredSegments, bool isPattern, bool isStreamSelector = false)
        {
            ParameterName = parameterName;
            Scheme = scheme;
            RequiredSegments = requiredSegments;
            IsPattern = isPattern;
            IsStreamSelector = isStreamSelector;
        }

        internal string? ParameterName { get; }
        internal string? Scheme { get; }
        internal int RequiredSegments { get; }
        internal bool IsPattern { get; }
        internal bool IsStreamSelector { get; }
    }

    internal static ApiSymbols ResolveApiSymbols(Compilation compilation) => new(compilation);

    internal static bool IsFitzMethod(IMethodSymbol method, ApiSymbols apiSymbols) =>
        apiSymbols.TryGetClientName(method, out _);

    internal static bool TryGetRule(IMethodSymbol method, ApiSymbols apiSymbols, out Rule rule)
    {
        rule = default;
        if (!apiSymbols.TryGetClientName(method, out var type))
        {
            return false;
        }

        var name = method.Name;
        rule = (type, name) switch
        {
            ("IKvClient", "BeginAsync") => new("route", "kv", 3, false),
            ("IKvClient", "SubscribeAsync") => new("pattern", "kv", 3, true),
            ("IQueueClient", "EnqueueAsync") => new("route", "queue", 3, false),
            ("IQueueClient", "ReserveAsync") => new("route", "queue", 3, true),
            ("IQueueClient", "SubscribeAsync") => new("pattern", "queue", 3, true),
            ("ILeaseClient", "AcquireAsync" or "WithLeaseAsync" or "QueryAsync") => new("route", "lease", 3, false),
            ("ILeaseClient", "SubscribeAsync" or "ListAsync" or "ObserveAsync") => new("pattern", "lease", 3, true),
            ("INoticeClient", "PublishAsync") => new("route", "notice", 3, false),
            ("INoticeClient", "SubscribeAsync") => new("pattern", "notice", 0, true),
            ("IRpcClient", "CallAsync") => new("route", "rpc", 0, false),
            ("IRpcClient", "RegisterWorkerAsync") => new("pattern", "rpc", 0, true),
            ("IScheduleClient", "CreateAsync" or "CancelAsync") => new("route", "schedule", 4, false),
            ("IScheduleClient", "ListBySelectorAsync") => new("selector", "schedule", 4, true),
            ("IScheduleClient", "SubscribeAsync") => new("pattern", "schedule", 4, true),
            ("IStreamClient", "BeginAsync" or "PeekAsync" or "MetadataAsync") => new("route", "stream", 3, false),
            ("IStreamClient", "ReadAsync" or "ReadPageAsync") => new("route", "stream", 3, true, true),
            ("IStreamClient", "SubscribeAsync") => new("pattern", "stream", 3, true, true),
            _ => default,
        };
        return rule.Scheme is not null;
    }

    internal static bool IsValidConcrete(string value, string scheme, int requiredSegments)
    {
        if (!TrySegments(value, scheme, out var segments) || segments.Any(segment => segment.Contains('*')))
        {
            return false;
        }

        return requiredSegments == 0 || segments.Length == requiredSegments;
    }

    internal static bool IsValidPattern(string value, string scheme, int requiredSegments, bool streamSelector)
    {
        if (!TrySegments(value, scheme, out var segments))
        {
            return false;
        }

        if (segments.Any(segment => segment.Contains('*') && segment is not "*" and not "**"))
        {
            return false;
        }

        if (streamSelector)
        {
            if (segments.Length == 1 && segments[0] == "**")
            {
                return true;
            }

            if (segments.Length == 2 && segments[1] == "**")
            {
                return segments[0] is not "*" and not "**";
            }

            if (segments.Length != 3 || segments.Any(segment => segment == "**"))
            {
                return false;
            }

            return true;
        }

        var doubleWildcards = segments.Count(segment => segment == "**");
        if (segments.Zip(segments.Skip(1), (left, right) => left == "**" && right == "**").Any(consecutive => consecutive))
        {
            return false;
        }
        return requiredSegments == 0 || (doubleWildcards == 0
            ? segments.Length == requiredSegments
            : segments.Length - doubleWildcards <= requiredSegments);
    }

    static bool TrySegments(string value, string scheme, out string[] segments)
    {
        var prefix = scheme + "://";
        if (!value.StartsWith(prefix, StringComparison.Ordinal) || value.Length == prefix.Length)
        {
            segments = [];
            return false;
        }

        segments = value.Substring(prefix.Length).Split('/');
        return segments.All(segment => segment.Length > 0);
    }
}
