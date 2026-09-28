using System.Text.Json;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>
///     A page's body: the bytes read once from the response, the items found in them with a forward-only reader, and a
///     parsed <see cref="JsonDocument" /> built only when something (a pagination strategy, an error message) needs it.
/// </summary>
internal sealed class HttpPageBody(byte[] bytes, JsonReaderOptions readerOptions) : IDisposable
{
    private readonly List<JsonDocument> _values = [];
    private JsonDocument? _document;

    public byte[] Bytes { get; } = bytes;

    /// <summary>The body parsed, on first use.</summary>
    /// <exception cref="JsonException">The body is not JSON.</exception>
    public JsonElement Root => (_document ??= JsonDocument.Parse(Bytes, new JsonDocumentOptions { AllowTrailingCommas = readerOptions.AllowTrailingCommas, CommentHandling = readerOptions.CommentHandling, MaxDepth = readerOptions.MaxDepth })).RootElement;

    public void Dispose()
    {
        _document?.Dispose();

        foreach (var value in _values)
        {
            value.Dispose();
        }
    }

    /// <summary>
    ///     The value at <paramref name="path" />, found with a forward-only reader and parsed on its own, so reading a total
    ///     or a cursor does not parse the whole page. Once the page has been parsed, the parsed page is used instead.
    /// </summary>
    public bool TryGetValue(IReadOnlyList<string> path, out JsonElement value)
    {
        if (_document is not null)
            return HttpJsonPath.TryResolve(_document.RootElement, path, out value);

        var reader = new Utf8JsonReader(Bytes, readerOptions);
        value = default;

        if (!reader.Read())
            return false;

        foreach (var segment in path)
        {
            if (reader.TokenType != JsonTokenType.StartObject || !MoveToProperty(ref reader, segment))
                return false;
        }

        var start = (int)reader.TokenStartIndex;
        reader.Skip();

        // A string token's BytesConsumed includes its closing quote, as the value's text does.
        var document = JsonDocument.Parse(Bytes.AsMemory(start, (int)reader.BytesConsumed - start));
        _values.Add(document);
        value = document.RootElement;
        return true;
    }

    /// <summary>
    ///     Positions <paramref name="reader" /> on the start of the items array: the root, or the array at
    ///     <paramref name="path" />. Returns <c>false</c> when the path is missing or not an array. Nothing after the array
    ///     is read, so a serializer can read the array straight from the reader.
    /// </summary>
    /// <exception cref="JsonException">The body is not JSON.</exception>
    public bool TryFindArray(string[]? path, out Utf8JsonReader reader)
    {
        reader = new Utf8JsonReader(Bytes, readerOptions);

        if (!reader.Read())
            throw new JsonException("The body is empty.");

        foreach (var segment in path ?? [])
        {
            if (reader.TokenType != JsonTokenType.StartObject || !MoveToProperty(ref reader, segment))
                return false;
        }

        return reader.TokenType == JsonTokenType.StartArray;
    }

    /// <summary>The positions of the elements of the array <paramref name="reader" /> is on.</summary>
    public static List<Range> FindElements(Utf8JsonReader reader)
    {
        var elements = new List<Range>();

        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            var start = (int)reader.TokenStartIndex;
            reader.Skip();
            elements.Add(new Range(start, (int)reader.BytesConsumed));
        }

        return elements;
    }

    /// <summary>
    ///     The bytes of the array <paramref name="reader" /> is on. A root array runs to the end of the body, so it needs no
    ///     scan; a nested one is skipped once to find its end. Deserializing from these bytes is much faster than from the
    ///     reader.
    /// </summary>
    public ReadOnlySpan<byte> ArrayBytes(Utf8JsonReader reader, bool isRoot)
    {
        var start = (int)reader.TokenStartIndex;

        if (isRoot)
            return Bytes.AsSpan(start);

        reader.Skip();
        return Bytes.AsSpan(start, (int)reader.BytesConsumed - start);
    }

    /// <summary>Moves from an object's start to the value of its property <paramref name="name" />: an exact match, else the first ignoring case.</summary>
    private static bool MoveToProperty(ref Utf8JsonReader reader, string name)
    {
        var objectStart = reader;
        var caseInsensitive = default(Utf8JsonReader);
        var hasCaseInsensitive = false;

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            if (reader.ValueTextEquals(name))
            {
                _ = reader.Read();
                return true;
            }

            if (!hasCaseInsensitive && string.Equals(reader.GetString(), name, StringComparison.OrdinalIgnoreCase))
            {
                caseInsensitive = reader;
                _ = caseInsensitive.Read();
                hasCaseInsensitive = true;
            }

            _ = reader.Read();
            reader.Skip();
        }

        if (hasCaseInsensitive)
        {
            reader = caseInsensitive;
            return true;
        }

        reader = objectStart;
        return false;
    }
}
