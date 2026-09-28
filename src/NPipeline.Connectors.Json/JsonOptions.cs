using System.Text.Json;
using NPipeline.Connectors.Files;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Json;

/// <summary>Options for <see cref="JsonSourceNode{T}" />. Create them with <see cref="JsonConnector.Source{T}(StorageUri, Func{JsonReadOptions, JsonReadOptions}?)" /> or directly.</summary>
public sealed record JsonReadOptions : FileSourceOptions
{
    /// <summary>How files hold their records. Defaults to <see cref="JsonFormat.Auto" />, which looks at each file's first character.</summary>
    public JsonFormat Format { get; init; } = JsonFormat.Auto;

    /// <summary>
    ///     The path to the array of records inside a root object, as property names separated by dots:
    ///     <c>data.items</c> (a leading <c>$.</c> is allowed). Property names match case-insensitively. When <c>null</c>
    ///     (the default), records are the root array's elements or the file's top-level values.
    /// </summary>
    public string? ItemsPath { get; init; }

    /// <summary>
    ///     The serializer options. When <c>null</c> (the default), the connector's defaults: System.Text.Json's web
    ///     defaults (camelCase, case-insensitive property matching, numbers from strings) with enums as names. Either way,
    ///     <c>[Column]</c> and <c>[IgnoreColumn]</c> are honoured.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; init; }

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();

        if (ItemsPath is not null && JsonItemsPath.Parse(ItemsPath).Count == 0)
            throw new ArgumentException("ItemsPath must name at least one property, such as 'data.items'.", nameof(ItemsPath));

        if (ItemsPath is not null && Format == JsonFormat.NewlineDelimited)
            throw new ArgumentException("ItemsPath needs a single root object, so it cannot be combined with NewlineDelimited.", nameof(ItemsPath));
    }
}

/// <summary>Options for <see cref="JsonSinkNode{T}" />. Create them with <see cref="JsonConnector.Sink{T}(StorageUri, Func{JsonWriteOptions, JsonWriteOptions}?)" /> or directly.</summary>
public sealed record JsonWriteOptions : FileSinkOptions
{
    /// <summary>How to write the records. Defaults to <see cref="JsonFormat.Auto" />: NDJSON for <c>.ndjson</c> and <c>.jsonl</c> files, an array otherwise.</summary>
    public JsonFormat Format { get; init; } = JsonFormat.Auto;

    /// <summary>Whether to indent an array's output. NDJSON is never indented. Defaults to <c>false</c>.</summary>
    public bool WriteIndented { get; init; }

    /// <summary>The serializer options. When <c>null</c> (the default), the connector's defaults, as for <see cref="JsonReadOptions.SerializerOptions" />.</summary>
    public JsonSerializerOptions? SerializerOptions { get; init; }
}

/// <summary>Parses <see cref="JsonReadOptions.ItemsPath" />.</summary>
internal static class JsonItemsPath
{
    public static IReadOnlyList<string> Parse(string path)
    {
        var trimmed = path.Trim();

        if (trimmed.StartsWith('$'))
            trimmed = trimmed[1..];

        return trimmed.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}
