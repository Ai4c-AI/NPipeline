using System.Text.Json;

namespace NPipeline.Connectors.Mapping;

/// <summary>
///     Converts a CLR member name into the column (or property) name a connector writes. An explicit
///     <see cref="Attributes.ColumnAttribute" /> name always wins over the policy. Reads match column names
///     case-insensitively, so the policy matters for what is written.
/// </summary>
public abstract class ColumnNamingPolicy
{
    /// <summary>The member name unchanged: <c>FirstName</c>.</summary>
    public static ColumnNamingPolicy AsIs { get; } = new DelegatePolicy(static name => name);

    /// <summary>Lowercase: <c>firstname</c>.</summary>
    public static ColumnNamingPolicy LowerCase { get; } = new DelegatePolicy(static name => name.ToLowerInvariant());

    /// <summary>camelCase, as System.Text.Json writes it: <c>firstName</c>.</summary>
    public static ColumnNamingPolicy CamelCase { get; } = new JsonPolicy(JsonNamingPolicy.CamelCase);

    /// <summary>snake_case, as System.Text.Json writes it: <c>first_name</c>, <c>http_server</c>.</summary>
    public static ColumnNamingPolicy SnakeCaseLower { get; } = new JsonPolicy(JsonNamingPolicy.SnakeCaseLower);

    /// <summary>kebab-case, as System.Text.Json writes it: <c>first-name</c>.</summary>
    public static ColumnNamingPolicy KebabCaseLower { get; } = new JsonPolicy(JsonNamingPolicy.KebabCaseLower);

    /// <summary>Converts <paramref name="memberName" /> into a column name.</summary>
    public abstract string ConvertName(string memberName);

    /// <summary>A policy that applies <paramref name="convert" />.</summary>
    public static ColumnNamingPolicy Custom(Func<string, string> convert)
    {
        ArgumentNullException.ThrowIfNull(convert);
        return new DelegatePolicy(convert);
    }

    /// <summary>A policy that applies a System.Text.Json naming policy, so a connector can share one with its serializer.</summary>
    public static ColumnNamingPolicy FromJson(JsonNamingPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        return new JsonPolicy(policy);
    }

    private sealed class DelegatePolicy(Func<string, string> convert) : ColumnNamingPolicy
    {
        public override string ConvertName(string memberName) => convert(memberName);
    }

    private sealed class JsonPolicy(JsonNamingPolicy policy) : ColumnNamingPolicy
    {
        public override string ConvertName(string memberName) => policy.ConvertName(memberName);
    }
}
