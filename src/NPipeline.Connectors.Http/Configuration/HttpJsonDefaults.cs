using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace NPipeline.Connectors.Http.Configuration;

/// <summary>
///     The serializer options used when the options set no <c>JsonOptions</c>. One shared instance keeps
///     System.Text.Json's per-type metadata cache warm; a new instance per request or batch rebuilds it every time.
/// </summary>
internal static class HttpJsonDefaults
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    ///     The metadata for <typeparamref name="T" />. Options without a resolver get the reflection resolver, as they would
    ///     on their first use by the serializer, which also makes them read-only.
    /// </summary>
    public static JsonTypeInfo<T> TypeInfo<T>(JsonSerializerOptions? options)
    {
        options ??= Options;

        if (!options.IsReadOnly)
            options.MakeReadOnly(populateMissingResolver: true);

        return (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));
    }
}
