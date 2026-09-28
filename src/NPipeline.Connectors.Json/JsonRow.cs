using System.Text.Json;
using NPipeline.Connectors.Errors;

namespace NPipeline.Connectors.Json;

/// <summary>
///     The current record, for a manual mapper passed to <see cref="JsonSourceNode{T}" />. Its <see cref="Element" /> is
///     only valid during the mapper call; use <see cref="JsonElement.Clone" /> to keep it.
/// </summary>
/// <remarks>
///     Values are read with the source's serializer options, so any type the serializer supports can be read, including
///     nested objects and lists. <see cref="Get{T}(string)" /> is strict: a missing property or a value that does not
///     convert throws, naming the property. <see cref="TryGet{T}(string, out T)" /> is the lenient alternative.
/// </remarks>
public readonly struct JsonRow
{
    private readonly JsonSerializerOptions _options;

    internal JsonRow(JsonElement element, JsonSerializerOptions options, long recordNumber)
    {
        Element = element;
        _options = options;
        RecordNumber = recordNumber;
    }

    /// <summary>The record.</summary>
    public JsonElement Element { get; }

    /// <summary>The record's 1-based position in the file.</summary>
    public long RecordNumber { get; }

    /// <summary>Whether the record is an object with a property named <paramref name="name" />.</summary>
    public bool HasProperty(string name) => TryFind(name, out _);

    /// <summary>Reads the named property as <typeparamref name="T" />. A dotted name (<c>address.city</c>) reads a nested property.</summary>
    /// <exception cref="FieldMappingException">The property is missing, or its value does not convert.</exception>
    public T Get<T>(string name)
    {
        if (!TryFind(name, out var value))
            throw new FieldMappingException(name, name, new KeyNotFoundException($"The record has no property '{name}'."));

        try
        {
            return value.Deserialize<T>(_options)!;
        }
        catch (JsonException ex)
        {
            throw new FieldMappingException(name, name, ex);
        }
    }

    /// <summary>Reads the named property as <typeparamref name="T" />, returning <c>false</c> when it is missing or does not convert.</summary>
    public bool TryGet<T>(string name, out T value)
    {
        if (TryFind(name, out var element))
        {
            try
            {
                value = element.Deserialize<T>(_options)!;
                return true;
            }
            catch (JsonException)
            {
            }
            catch (InvalidOperationException)
            {
            }
        }

        value = default!;
        return false;
    }

    /// <summary>Reads the whole record as <typeparamref name="T" />.</summary>
    /// <exception cref="JsonException">The record does not convert.</exception>
    public T As<T>() => Element.Deserialize<T>(_options)!;

    private bool TryFind(string name, out JsonElement value)
    {
        value = Element;

        foreach (var segment in name.Split('.'))
        {
            if (value.ValueKind != JsonValueKind.Object)
                return false;

            if (value.TryGetProperty(segment, out var exact))
            {
                value = exact;
                continue;
            }

            if (!_options.PropertyNameCaseInsensitive || !TryFindIgnoringCase(value, segment, out value))
                return false;
        }

        return true;
    }

    private static bool TryFindIgnoringCase(JsonElement element, string name, out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
