using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Json;

/// <summary>The connector's serializer defaults, and the resolver modifier that applies NPipeline's column attributes.</summary>
internal static class JsonSerialization
{
    // One copy per caller-supplied options instance, so STJ's metadata cache is shared by every node using it.
    private static readonly ConditionalWeakTable<JsonSerializerOptions, JsonSerializerOptions> WithAttributes = new();

    /// <summary>Web defaults (camelCase, case-insensitive reads, numbers from strings), enums as names, and column attributes.</summary>
    public static JsonSerializerOptions Default { get; } = CreateDefault();

    /// <summary>The options a node uses: the defaults, or a copy of the caller's with column attributes added.</summary>
    public static JsonSerializerOptions Resolve(JsonSerializerOptions? options) =>
        options is null
            ? Default
            : WithAttributes.GetValue(options, static o => AddColumnAttributes(o));

    public static JsonTypeInfo<T> TypeInfo<T>(JsonSerializerOptions options) => (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));

    private static JsonSerializerOptions CreateDefault()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            TypeInfoResolver = new DefaultJsonTypeInfoResolver().WithAddedModifier(ApplyColumnAttributes),
        };

        options.Converters.Add(new JsonStringEnumConverter());
        options.MakeReadOnly();
        return options;
    }

    private static JsonSerializerOptions AddColumnAttributes(JsonSerializerOptions options)
    {
        var copy = new JsonSerializerOptions(options);
        copy.TypeInfoResolver = (options.TypeInfoResolver ?? new DefaultJsonTypeInfoResolver()).WithAddedModifier(ApplyColumnAttributes);
        copy.MakeReadOnly();
        return copy;
    }

    /// <summary>
    ///     <c>[Column("name")]</c> renames a property unless <c>[JsonPropertyName]</c> already names it; <c>[IgnoreColumn]</c>
    ///     and <c>[Column(Ignore = true)]</c> remove it.
    /// </summary>
    private static void ApplyColumnAttributes(JsonTypeInfo typeInfo)
    {
        if (typeInfo.Kind != JsonTypeInfoKind.Object)
            return;

        for (var i = typeInfo.Properties.Count - 1; i >= 0; i--)
        {
            var property = typeInfo.Properties[i];

            if (property.AttributeProvider is not MemberInfo member)
                continue;

            var column = member.GetCustomAttribute<ColumnAttribute>(true);

            if (member.IsDefined(typeof(IgnoreColumnAttribute), true) || column?.Ignore == true)
            {
                typeInfo.Properties.RemoveAt(i);
                continue;
            }

            if (column is { Name.Length: > 0 } && !member.IsDefined(typeof(JsonPropertyNameAttribute), true))
                property.Name = column.Name;
        }
    }
}
