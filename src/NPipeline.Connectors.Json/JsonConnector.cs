using System.Text.Json.Serialization.Metadata;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json;

/// <summary>Creates JSON sources and sinks.</summary>
/// <example>
///     <code>
/// var source = JsonConnector.Source&lt;Order&gt;(StorageUri.Parse("s3://bucket/export.json"), o => o with { ItemsPath = "data.orders" });
/// var sink = JsonConnector.Sink&lt;Order&gt;(StorageUri.FromFilePath("orders.ndjson.gz"));
///     </code>
/// </example>
public static class JsonConnector
{
    /// <summary>A source that deserializes each record as <typeparamref name="T" />.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="configure">Adjusts the default options, typically with a <c>with</c> expression.</param>
    public static JsonSourceNode<T> Source<T>(StorageUri uri, Func<JsonReadOptions, JsonReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure));

    /// <summary>A source that deserializes each record with <paramref name="typeInfo" />, such as one from a source-generated <c>JsonSerializerContext</c>.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="typeInfo">The metadata to deserialize records with.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static JsonSourceNode<T> Source<T>(StorageUri uri, JsonTypeInfo<T> typeInfo, Func<JsonReadOptions, JsonReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure), typeInfo);

    /// <summary>A source that builds each record with <paramref name="map" />.</summary>
    /// <param name="uri">A file, a directory ending in <c>/</c>, or a glob.</param>
    /// <param name="map">Builds a record from the current one.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static JsonSourceNode<T> Source<T>(StorageUri uri, Func<JsonRow, T> map, Func<JsonReadOptions, JsonReadOptions>? configure = null) =>
        new(ReadOptions(uri, configure), map);

    /// <summary>A sink that serializes each record.</summary>
    /// <param name="uri">The file to write.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static JsonSinkNode<T> Sink<T>(StorageUri uri, Func<JsonWriteOptions, JsonWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure));

    /// <summary>A sink that serializes each record with <paramref name="typeInfo" />.</summary>
    /// <param name="uri">The file to write.</param>
    /// <param name="typeInfo">The metadata to serialize records with.</param>
    /// <param name="configure">Adjusts the default options.</param>
    public static JsonSinkNode<T> Sink<T>(StorageUri uri, JsonTypeInfo<T> typeInfo, Func<JsonWriteOptions, JsonWriteOptions>? configure = null) =>
        new(WriteOptions(uri, configure), typeInfo);

    private static JsonReadOptions ReadOptions(StorageUri uri, Func<JsonReadOptions, JsonReadOptions>? configure)
    {
        var options = new JsonReadOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }

    private static JsonWriteOptions WriteOptions(StorageUri uri, Func<JsonWriteOptions, JsonWriteOptions>? configure)
    {
        var options = new JsonWriteOptions { Uri = uri };
        return configure is null ? options : configure(options);
    }
}
