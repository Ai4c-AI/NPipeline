using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Running;

namespace NPipeline.Connectors.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        // The default config already exports GitHub markdown, used for the committed baselines. Full JSON is added
        // so later runs can be diffed against them.
        var config = DefaultConfig.Instance.AddExporter(JsonExporter.Full);

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
