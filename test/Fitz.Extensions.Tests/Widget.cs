using System.Text.Json.Serialization;

namespace Cntryl.Fitz.Extensions.Tests;

sealed record Widget(Guid Id, string Name, int Priority);

[JsonSerializable(typeof(Widget))]
sealed partial class WidgetJsonContext : JsonSerializerContext;
