using BenchmarkDotNet.Running;

namespace Cntryl.Fitz.Benchmarks;

class Program
{
    static void Main(string[] args)
    {
        // Recommend running with: dotnet run -c Release
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
