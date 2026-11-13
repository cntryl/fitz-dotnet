using BenchmarkDotNet.Running;
using Cntryl.Fitz.Benchmarks;

namespace Cntryl.Fitz.Benchmarks.Runner;

static class Program
{
    public static void Main(string[] args) =>
        // Recommend running with: dotnet run -c Release
        BenchmarkSwitcher.FromAssembly(typeof(FrameCodecBenchmarks).Assembly).Run(args);
}
