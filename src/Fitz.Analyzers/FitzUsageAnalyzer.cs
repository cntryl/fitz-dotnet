using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Cntryl.Fitz.Analyzers;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class FitzUsageAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [FitzDiagnostics.InvalidRoute, FitzDiagnostics.InvalidPattern, FitzDiagnostics.DiscardedHandle, FitzDiagnostics.InvalidArgument];

    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(static startContext =>
        {
            var apiSymbols = FitzRouteRules.ResolveApiSymbols(startContext.Compilation);
            startContext.RegisterOperationAction(
                operationContext => AnalyzeInvocation(operationContext, apiSymbols),
                OperationKind.Invocation);
        });
    }

    static void AnalyzeInvocation(OperationAnalysisContext context, FitzRouteRules.ApiSymbols apiSymbols)
    {
        var invocation = (IInvocationOperation)context.Operation;
        AnalyzeAddress(context, invocation, apiSymbols);
        AnalyzeDiscard(context, invocation, apiSymbols);
        AnalyzeArguments(context, invocation, apiSymbols);
    }

    static void AnalyzeAddress(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        FitzRouteRules.ApiSymbols apiSymbols)
    {
        if (!FitzRouteRules.TryGetRule(invocation.TargetMethod, apiSymbols, out var rule))
        {
            return;
        }

        var argument = invocation.Arguments.FirstOrDefault(item => item.Parameter?.Name == rule.ParameterName);
        if (argument?.Value.ConstantValue is not { HasValue: true, Value: string value })
        {
            return;
        }

        var valid = rule.IsPattern
            ? FitzRouteRules.IsValidPattern(value, rule.Scheme!, rule.RequiredSegments, rule.IsStreamSelector)
            : FitzRouteRules.IsValidConcrete(value, rule.Scheme!, rule.RequiredSegments);
        if (valid)
        {
            return;
        }

        var descriptor = rule.IsPattern ? FitzDiagnostics.InvalidPattern : FitzDiagnostics.InvalidRoute;
        context.ReportDiagnostic(Diagnostic.Create(
            descriptor,
            argument.Syntax.GetLocation(),
            properties: ImmutableDictionary<string, string?>.Empty.Add("ExpectedScheme", rule.Scheme),
            value,
            rule.Scheme));
    }

    static void AnalyzeDiscard(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        FitzRouteRules.ApiSymbols apiSymbols)
    {
        if (invocation.Parent is not IAwaitOperation { Parent: IExpressionStatementOperation })
        {
            return;
        }

        if (invocation.Type is not INamedTypeSymbol { Name: "Task", TypeArguments.Length: 1 } taskType)
        {
            return;
        }

        var handleType = taskType.TypeArguments[0];
        if (!apiSymbols.IsLifecycleHandle(handleType))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            FitzDiagnostics.DiscardedHandle,
            invocation.Syntax.GetLocation(),
            properties: ImmutableDictionary<string, string?>.Empty.Add("HandleName", GetHandleName(handleType)),
            handleType.Name,
            invocation.TargetMethod.Name));
    }

    static string GetHandleName(ITypeSymbol type) => type.Name switch
    {
        "IKvTransaction" => "transaction",
        "ILease" => "lease",
        "ILeaseInventoryObserver" => "observer",
        "RpcWorkerRegistration" => "registration",
        "IStreamSession" => "session",
        _ => "subscription",
    };

    static void AnalyzeArguments(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        FitzRouteRules.ApiSymbols apiSymbols)
    {
        if (!FitzRouteRules.IsFitzMethod(invocation.TargetMethod, apiSymbols))
        {
            return;
        }

        foreach (var argument in invocation.Arguments)
        {
            if (argument.Parameter is null)
            {
                continue;
            }

            var constant = GetConstantValue(argument.Value);
            if (!IsInvalidConstant(argument.Parameter.Name, constant))
            {
                continue;
            }

            context.ReportDiagnostic(Diagnostic.Create(
                FitzDiagnostics.InvalidArgument,
                argument.Syntax.GetLocation(),
                argument.Parameter.Name,
                constant.Value));
        }
    }

    static Optional<object?> GetConstantValue(IOperation operation)
    {
        var constant = operation.ConstantValue;
        while (!constant.HasValue && operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
            constant = operation.ConstantValue;
        }

        return constant;
    }

    static bool IsInvalidConstant(string parameterName, Optional<object?> constant)
    {
        if (!constant.HasValue || constant.Value is null)
        {
            return false;
        }

        return parameterName switch
        {
            "ttlSecs" => ToUInt64(constant.Value) is 0 or > uint.MaxValue / 1000,
            "leaseSeconds" => ToUInt64(constant.Value) == 0,
            "batchSize" => ToInt64(constant.Value) <= 0,
            "delayMs" => IsInvalidDelay(ToInt64(constant.Value)),
            "waitSeconds" or "limit" => ToInt64(constant.Value) < 0,
            _ => false,
        };
    }

    static bool IsInvalidDelay(long delayMs) => delayMs < 0 || delayMs % 1000 != 0;

    static ulong ToUInt64(object value) => value switch
    {
        byte item => item,
        ushort item => item,
        uint item => item,
        ulong item => item,
        _ => ulong.MaxValue,
    };

    static long ToInt64(object value) => value switch
    {
        sbyte item => item,
        short item => item,
        int item => item,
        long item => item,
        byte item => item,
        ushort item => item,
        uint item => item,
        _ => long.MaxValue,
    };
}
