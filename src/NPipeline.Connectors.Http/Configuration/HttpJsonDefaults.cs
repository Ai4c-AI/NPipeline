using System.Text.Json;

namespace NPipeline.Connectors.Http.Configuration;

/// <summary>
///     The serializer options used when a configuration sets no <c>JsonOptions</c>. One shared instance keeps
///     System.Text.Json's per-type metadata cache warm; a new instance per request or batch rebuilds it every time.
/// </summary>
internal static class HttpJsonDefaults
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);
}
