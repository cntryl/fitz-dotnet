using BenchmarkDotNet.Running;

namespace Cntryl.Fitz.Benchmarks;

internal static class Program
{
    public static void Main(string[] args)
    {
        // Recommend running with: dotnet run -c Release
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
