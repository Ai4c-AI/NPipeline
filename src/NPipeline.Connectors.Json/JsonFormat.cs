namespace NPipeline.Connectors.Json;

/// <summary>How a JSON file holds its records.</summary>
public enum JsonFormat
{
    /// <summary>
    ///     Decided per file. A source reads a root array as its elements, and anything else as a sequence of top-level
    ///     values (NDJSON, or a single object). A sink writes NDJSON to files ending in <c>.ndjson</c> or <c>.jsonl</c>
    ///     (before any compression suffix), and an array otherwise.
    /// </summary>
    Auto,

    /// <summary>One JSON array of records: <c>[{"name":"Ada"},{"name":"Grace"}]</c>.</summary>
    Array,

    /// <summary>One record per line (NDJSON, JSON Lines): <c>{"name":"Ada"}\n{"name":"Grace"}</c>. A source also accepts records spanning lines.</summary>
    NewlineDelimited,
}
