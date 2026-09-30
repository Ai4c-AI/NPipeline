using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Sql;
using NPipeline.Nodes;

namespace NPipeline.Connectors.SqlServer.Analyzers.Tests;

public sealed class SqlServerCheckpointOrderingAnalyzerTests
{
    private const string Usings = """
                                  using NPipeline.Connectors.SqlServer;
                                  using NPipeline.Connectors.SqlServer.Configuration;
                                  using NPipeline.Connectors.SqlServer.DependencyInjection;
                                  using NPipeline.Connectors.Configuration;

                                  public class MyRecord { public int Id { get; set; } }

                                  """;

    [Theory]
    [InlineData("Offset")]
    [InlineData("InMemory")]
    public void Warns_when_a_checkpointing_factory_source_has_no_order_by(string strategy) =>
        Assert.True(Warns($$"""
                            public class P
                            {
                                public void Create() =>
                                    SqlServerConnector.Source<MyRecord>("cs", "SELECT id FROM t", o => o with { CheckpointStrategy = CheckpointStrategy.{{strategy}} });
                            }
                            """));

    [Fact]
    public void Does_not_warn_with_an_order_by_in_any_case()
    {
        Assert.False(Warns("""
                           public class P
                           {
                               public void Create() =>
                                   SqlServerConnector.Source<MyRecord>("cs", "SELECT id FROM t order  by id", o => o with { CheckpointStrategy = CheckpointStrategy.Offset });
                           }
                           """));
    }

    [Fact]
    public void Does_not_warn_without_checkpointing()
    {
        Assert.False(Warns("""
                           public class P
                           {
                               public void Create()
                               {
                                   SqlServerConnector.Source<MyRecord>("cs", "SELECT id FROM t");
                                   SqlServerConnector.Source<MyRecord>("cs", "SELECT id FROM t", o => o with { CheckpointStrategy = CheckpointStrategy.None });
                               }
                           }
                           """));
    }

    [Fact]
    public void Resolves_a_constant_query()
    {
        Assert.True(Warns("""
                          public class P
                          {
                              private const string Query = "SELECT id FROM t";

                              public void Create() =>
                                  SqlServerConnector.Source<MyRecord>("cs", Query, o => o with { CheckpointStrategy = CheckpointStrategy.Offset });
                          }
                          """));
    }

    [Fact]
    public void Skips_interpolated_queries()
    {
        Assert.False(Warns("""
                           public class P
                           {
                               public void Create(string table) =>
                                   SqlServerConnector.Source<MyRecord>("cs", $"SELECT id FROM {table}", o => o with { CheckpointStrategy = CheckpointStrategy.Offset });
                           }
                           """));
    }

    [Fact]
    public void Warns_for_options_built_directly()
    {
        Assert.True(Warns("""
                          public class P
                          {
                              public void Create() =>
                                  _ = new SqlServerReadOptions { ConnectionString = "cs", Query = "SELECT id FROM t", CheckpointStrategy = CheckpointStrategy.Offset };
                          }
                          """));
    }

    [Fact]
    public void Warns_for_the_dependency_injection_factory()
    {
        Assert.True(Warns("""
                          public class P
                          {
                              public void Create(ISqlServerSourceNodeFactory factory) =>
                                  factory.CreateSourceNode<MyRecord>("SELECT id FROM t", o => o with { CheckpointStrategy = CheckpointStrategy.InMemory });
                          }
                          """));
    }

    [Fact]
    public void Has_the_documented_id() => Assert.Equal("NP9502", SqlServerCheckpointOrderingAnalyzer.SqlServerCheckpointOrderingId);

    private static bool Warns(string code) =>
        Diagnostics(Usings + code).Any(d => d.Id == SqlServerCheckpointOrderingAnalyzer.SqlServerCheckpointOrderingId);

    private static IEnumerable<Diagnostic> Diagnostics(string code)
    {
        var runtime = Path.GetDirectoryName(typeof(object).Assembly.Location)!;

        MetadataReference[] references =
        [
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(Path.Combine(runtime, "System.Runtime.dll")),
            MetadataReference.CreateFromFile(Path.Combine(runtime, "System.Data.Common.dll")),
            MetadataReference.CreateFromFile(typeof(SqlServerConnector).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(SqlSourceOptions).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(SourceNode<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CheckpointStrategy).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(NPipeline.StorageProviders.Models.StorageUri).Assembly.Location),
        ];

        var compilation = CSharpCompilation.Create("Test", [CSharpSyntaxTree.ParseText(code)], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        return compilation.WithAnalyzers([new SqlServerCheckpointOrderingAnalyzer()]).GetAnalyzerDiagnosticsAsync().Result;
    }
}
