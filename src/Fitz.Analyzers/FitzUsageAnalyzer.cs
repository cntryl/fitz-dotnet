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
        var properties = ImmutableDictionary<string, string?>.Empty.Add("ExpectedScheme", rule.Scheme);
        var marker = value.IndexOf("://", StringComparison.Ordinal);
        if (marker >= 0)
        {
            var suggested = rule.Scheme + "://" + value.Substring(marker + 3);
            var suggestionIsValid = rule.IsPattern
                ? FitzRouteRules.IsValidPattern(suggested, rule.Scheme!, rule.RequiredSegments, rule.IsStreamSelector)
                : FitzRouteRules.IsValidConcrete(suggested, rule.Scheme!, rule.RequiredSegments);
            if (suggestionIsValid)
            {
                properties = properties.Add("SuggestedAddress", suggested);
            }
        }

        context.ReportDiagnostic(Diagnostic.Create(
            descriptor,
            argument.Syntax.GetLocation(),
            properties: properties,
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

            // Durations are TimeSpan on the public surface, so the literal the caller wrote is
            // an expression rather than a constant. Evaluate the recognisable TimeSpan factory
            // calls so these stay compile-time errors instead of runtime ones.
            object? reported;
            if (TryGetConstantTimeSpan(argument.Value, out var duration))
            {
                if (!IsInvalidDuration(argument.Parameter.Name, duration))
                {
                    continue;
                }

                reported = duration;
            }
            else
            {
                var constant = GetConstantValue(argument.Value);
                if (!IsInvalidConstant(invocation.TargetMethod, apiSymbols, argument.Parameter.Name, constant))
                {
                    continue;
                }

                reported = constant.Value;
            }

            context.ReportDiagnostic(Diagnostic.Create(
                FitzDiagnostics.InvalidArgument,
                argument.Syntax.GetLocation(),
                argument.Parameter.Name,
                reported));
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

    static bool IsInvalidConstant(
        IMethodSymbol method,
        FitzRouteRules.ApiSymbols apiSymbols,
        string parameterName,
        Optional<object?> constant)
    {
        if (!constant.HasValue || constant.Value is null)
        {
            return false;
        }

        if (parameterName == "limit" &&
            method.Name == "ListAsync" &&
            apiSymbols.TryGetClientName(method, out var clientName) &&
            clientName == "ILeaseClient")
        {
            return ToInt64(constant.Value) <= 0;
        }

        return parameterName switch
        {
            "batchSize" => ToInt64(constant.Value) <= 0,
            "limit" => ToInt64(constant.Value) < 0,
            _ => false,
        };
    }

    /// <summary>
    /// Whether a duration written as a literal is one the Fitz wire cannot carry.
    /// </summary>
    static bool IsInvalidDuration(string parameterName, TimeSpan value) => parameterName switch
    {
        "ttl" => value <= TimeSpan.Zero || !IsWholeSeconds(value) || value.TotalSeconds > uint.MaxValue / 1000,
        "lease" => value <= TimeSpan.Zero || !IsWholeSeconds(value),
        "delay" or "wait" => value < TimeSpan.Zero || !IsWholeSeconds(value),
        _ => false,
    };

    static bool IsWholeSeconds(TimeSpan value) => value.Ticks % TimeSpan.TicksPerSecond == 0;

    /// <summary>
    /// Evaluates <c>TimeSpan.Zero</c> and the <c>TimeSpan.From*</c> factories over a constant,
    /// which is how a caller writes a duration literal.
    /// </summary>
    static bool TryGetConstantTimeSpan(IOperation operation, out TimeSpan value)
    {
        value = default;
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }

        if (operation is IFieldReferenceOperation field &&
            IsTimeSpan(field.Field.ContainingType) &&
            field.Field.Name == nameof(TimeSpan.Zero))
        {
            value = TimeSpan.Zero;
            return true;
        }

        if (operation is not IInvocationOperation call ||
            !call.TargetMethod.IsStatic ||
            !IsTimeSpan(call.TargetMethod.ContainingType) ||
            call.Arguments.Length != 1)
        {
            return false;
        }

        var amountConstant = GetConstantValue(call.Arguments[0].Value);
        if (!amountConstant.HasValue || amountConstant.Value is null || !TryToDouble(amountConstant.Value, out var amount))
        {
            return false;
        }

        try
        {
            switch (call.TargetMethod.Name)
            {
                case nameof(TimeSpan.FromDays):
                    value = TimeSpan.FromDays(amount);
                    return true;
                case nameof(TimeSpan.FromHours):
                    value = TimeSpan.FromHours(amount);
                    return true;
                case nameof(TimeSpan.FromMinutes):
                    value = TimeSpan.FromMinutes(amount);
                    return true;
                case nameof(TimeSpan.FromSeconds):
                    value = TimeSpan.FromSeconds(amount);
                    return true;
                case nameof(TimeSpan.FromMilliseconds):
                    value = TimeSpan.FromMilliseconds(amount);
                    return true;
                default:
                    return false;
            }
        }
        catch (OverflowException)
        {
            // A literal the caller wrote that TimeSpan itself rejects is not ours to report.
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    static bool TryToDouble(object value, out double result)
    {
        switch (value)
        {
            case sbyte item:
                result = item;
                return true;
            case short item:
                result = item;
                return true;
            case int item:
                result = item;
                return true;
            case long item:
                result = item;
                return true;
            case byte item:
                result = item;
                return true;
            case ushort item:
                result = item;
                return true;
            case uint item:
                result = item;
                return true;
            case ulong item:
                result = item;
                return true;
            case float item:
                result = item;
                return true;
            case double item:
                result = item;
                return true;
            case decimal item:
                result = (double)item;
                return true;
            default:
                result = 0;
                return false;
        }
    }

    static bool IsTimeSpan(INamedTypeSymbol? type) =>
        type is { Name: nameof(TimeSpan), ContainingNamespace.Name: nameof(System) };

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
