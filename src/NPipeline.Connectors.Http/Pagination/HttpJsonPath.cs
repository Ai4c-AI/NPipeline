using System.Text.Json;

namespace NPipeline.Connectors.Http.Pagination;

/// <summary>The dotted property paths used by <c>ItemsJsonPath</c> and the pagination strategies, parsed one way everywhere.</summary>
internal static class HttpJsonPath
{
    /// <summary>Splits <c>$.data.items</c> or <c>data.items</c> into its property names; <c>$</c> or an empty path is the root.</summary>
    public static string[] Parse(string path)
    {
        var trimmed = path.Trim();

        if (trimmed.StartsWith('$'))
            trimmed = trimmed[1..];

        return trimmed.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    /// <summary>Follows <paramref name="segments" /> from <paramref name="root" />, matching each name exactly first and then ignoring case.</summary>
    public static bool TryResolve(JsonElement root, IReadOnlyList<string> segments, out JsonElement value)
    {
        value = root;

        foreach (var segment in segments)
        {
            if (value.ValueKind != JsonValueKind.Object)
                return false;

            if (value.TryGetProperty(segment, out var exact))
            {
                value = exact;
                continue;
            }

            var found = false;

            foreach (var property in value.EnumerateObject())
            {
                if (string.Equals(property.Name, segment, StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    found = true;
                    break;
                }
            }

            if (!found)
                return false;
        }

        return true;
    }

    /// <summary>The deepest object along the path and its property names, to say what was there when a path is wrong.</summary>
    public static string Describe(JsonElement root, IReadOnlyList<string> segments)
    {
        var current = root;
        var where = "the root";

        foreach (var segment in segments)
        {
            if (current.ValueKind != JsonValueKind.Object || !TryResolve(current, [segment], out var next))
                break;

            current = next;
            where = $"'{segment}'";
        }

        return current.ValueKind == JsonValueKind.Object
            ? $"{where} has properties: {string.Join(", ", current.EnumerateObject().Select(p => p.Name).Take(20))}"
            : $"{where} is {current.ValueKind}";
    }
}
