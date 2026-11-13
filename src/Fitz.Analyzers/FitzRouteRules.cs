using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Operations;

namespace Cntryl.Fitz.Analyzers;

static class FitzRouteRules
{
    internal sealed class ApiSymbols
    {
        readonly ImmutableArray<INamedTypeSymbol> _clients;
        readonly ImmutableArray<HandleSymbol> _lifecycleHandles;
        readonly INamedTypeSymbol? _subscriptionHandle;
        readonly INamedTypeSymbol? _task;
        readonly INamedTypeSymbol? _valueTask;

        internal ApiSymbols(Compilation compilation)
        {
            var clients = ImmutableArray.CreateBuilder<INamedTypeSymbol>();
            AddType(clients, compilation, "Cntryl.Fitz.IKvClient");
            AddType(clients, compilation, "Cntryl.Fitz.IQueueClient");
            AddType(clients, compilation, "Cntryl.Fitz.ILeaseClient");
            AddType(clients, compilation, "Cntryl.Fitz.INoticeClient");
            AddType(clients, compilation, "Cntryl.Fitz.IRpcClient");
            AddType(clients, compilation, "Cntryl.Fitz.IScheduleClient");
            AddType(clients, compilation, "Cntryl.Fitz.IStreamClient");
            _clients = clients.ToImmutable();

            var lifecycleHandles = ImmutableArray.CreateBuilder<HandleSymbol>();
            AddHandle(lifecycleHandles, compilation, "Cntryl.Fitz.IKvTransaction", "transaction");
            AddHandle(lifecycleHandles, compilation, "Cntryl.Fitz.ILease", "lease");
            AddHandle(lifecycleHandles, compilation, "Cntryl.Fitz.ILeaseInventoryObserver", "observer");
            AddHandle(lifecycleHandles, compilation, "Cntryl.Fitz.RpcWorkerRegistration", "registration");
            AddHandle(lifecycleHandles, compilation, "Cntryl.Fitz.IStreamSession", "session");
            _lifecycleHandles = lifecycleHandles.ToImmutable();
            _subscriptionHandle = compilation.GetTypeByMetadataName("Cntryl.Fitz.Runtime.SubscriptionHandle");
            _task = compilation.GetTypeByMetadataName("System.Threading.Tasks.Task`1");
            _valueTask = compilation.GetTypeByMetadataName("System.Threading.Tasks.ValueTask`1");
        }

        /// <summary>
        /// Resolves the Fitz client interface that <paramref name="method"/> belongs to, whether it
        /// is declared on the interface itself or on a class (shipped or user-written) that
        /// implements the interface member, directly or through an override chain.
        /// </summary>
        internal bool TryGetClientName(IInvocationOperation invocation, out string clientName)
        {
            var method = invocation.TargetMethod;
            var containingType = method.ContainingType;
            var receiverType = invocation.Instance?.Type as INamedTypeSymbol;
            foreach (var client in _clients)
            {
                // The receiver matters when a base class declares the method and only the derived
                // class lists the Fitz interface.
                if (SymbolEqualityComparer.Default.Equals(containingType.OriginalDefinition, client) ||
                    ImplementsClientMember(containingType, client, method) ||
                    (receiverType is not null && ImplementsClientMember(receiverType, client, method)))
                {
                    clientName = client.Name;
                    return true;
                }
            }

            clientName = string.Empty;
            return false;
        }

        /// <summary>
        /// Whether the awaited <paramref name="type"/> is a <c>Task&lt;T&gt;</c> or
        /// <c>ValueTask&lt;T&gt;</c>, returning the <c>T</c>.
        /// </summary>
        internal bool TryGetAwaitedResultType(ITypeSymbol? type, out ITypeSymbol result)
        {
            if (type is INamedTypeSymbol { TypeArguments.Length: 1 } named &&
                (SymbolEqualityComparer.Default.Equals(named.OriginalDefinition, _task) ||
                 SymbolEqualityComparer.Default.Equals(named.OriginalDefinition, _valueTask)))
            {
                result = named.TypeArguments[0];
                return true;
            }

            result = null!;
            return false;
        }

        /// <summary>
        /// Whether <paramref name="type"/> is a Fitz handle that must be retained and disposed,
        /// returning the noun used to describe it.
        /// </summary>
        internal bool TryGetLifecycleHandleName(ITypeSymbol type, out string handleName)
        {
            foreach (var lifecycleHandle in _lifecycleHandles)
            {
                if (SymbolEqualityComparer.Default.Equals(type.OriginalDefinition, lifecycleHandle.Symbol))
                {
                    handleName = lifecycleHandle.Name;
                    return true;
                }
            }

            handleName = "subscription";
            if (_subscriptionHandle is null)
            {
                return false;
            }

            for (var current = type; current is not null; current = current.BaseType)
            {
                if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, _subscriptionHandle))
                {
                    return true;
                }
            }

            return false;
        }

        static bool ImplementsClientMember(INamedTypeSymbol containingType, INamedTypeSymbol client, IMethodSymbol method)
        {
            if (containingType.TypeKind == TypeKind.Interface || method.IsStatic)
            {
                return false;
            }

            foreach (var candidate in containingType.AllInterfaces)
            {
                if (!SymbolEqualityComparer.Default.Equals(candidate.OriginalDefinition, client))
                {
                    continue;
                }

                foreach (var member in candidate.GetMembers(method.Name))
                {
                    if (member is IMethodSymbol interfaceMethod &&
                        containingType.FindImplementationForInterfaceMember(interfaceMethod) is IMethodSymbol implementation &&
                        SharesOverrideChain(implementation, method))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        static bool SharesOverrideChain(IMethodSymbol implementation, IMethodSymbol method)
        {
            for (var current = implementation; current is not null; current = current.OverriddenMethod)
            {
                if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, method.OriginalDefinition))
                {
                    return true;
                }
            }

            for (var current = method; current is not null; current = current.OverriddenMethod)
            {
                if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, implementation.OriginalDefinition))
                {
                    return true;
                }
            }

            return false;
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

        static void AddHandle(
            ImmutableArray<HandleSymbol>.Builder handles,
            Compilation compilation,
            string metadataName,
            string name)
        {
            if (compilation.GetTypeByMetadataName(metadataName) is { } symbol)
            {
                handles.Add(new HandleSymbol(symbol, name));
            }
        }

        readonly struct HandleSymbol
        {
            internal HandleSymbol(INamedTypeSymbol symbol, string name)
            {
                Symbol = symbol;
                Name = name;
            }

            internal INamedTypeSymbol Symbol { get; }

            internal string Name { get; }
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

    internal static bool IsFitzMethod(IInvocationOperation invocation, ApiSymbols apiSymbols) =>
        apiSymbols.TryGetClientName(invocation, out _);

    internal static bool TryGetRule(IInvocationOperation invocation, ApiSymbols apiSymbols, out Rule rule)
    {
        rule = default;
        if (!apiSymbols.TryGetClientName(invocation, out var type))
        {
            return false;
        }

        var name = invocation.TargetMethod.Name;
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

    internal static bool IsValidConcrete(string value, string scheme, int requiredSegments) =>
        RouteGrammar.TryValidateFixedRoute(value, scheme, requiredSegments, out _);

    internal static bool IsValidPattern(string value, string scheme, int requiredSegments, bool streamSelector) =>
        streamSelector
            ? RouteGrammar.TryGetStreamSelectorScope(value, out _)
            : RouteGrammar.TryValidateRegistrationPattern(value, scheme, requiredSegments, out _);
}
