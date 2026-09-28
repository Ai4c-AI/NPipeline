using NPipeline.Connectors.Parquet.Attributes;

namespace NPipeline.Connectors.RoundTrip.Tests.Models;

// Each model covers one family of types, so a failing round trip points at one bug rather than a wall of diffs.
// Values are chosen to survive every format's precision: whole seconds for dates (Excel stores OA doubles) and
// decimals within Parquet's DECIMAL(18, 4).

public sealed class ScalarRecord
{
    public int Id { get; set; }

    public long Big { get; set; }

    public double Ratio { get; set; }

    [ParquetDecimal(18, 4)]
    public decimal Amount { get; set; }

    public bool Active { get; set; }

    public string Name { get; set; } = string.Empty;

    public DateTime CreatedUtc { get; set; }

    public Guid Key { get; set; }

    public static ScalarRecord Create(int i) => new()
    {
        Id = i,
        Big = 5_000_000_000L + i,
        Ratio = i + 0.25,
        Amount = 1234.5678m + i,
        Active = i % 2 == 0,
        Name = $"name-{i}",
        CreatedUtc = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc).AddSeconds(i),
        Key = new Guid(i, 0, 0, [1, 2, 3, 4, 5, 6, 7, 8]),
    };
}

public sealed class UnsignedRecord
{
    public int Id { get; set; }

    public byte U8 { get; set; }

    public sbyte S8 { get; set; }

    public short S16 { get; set; }

    public ushort U16 { get; set; }

    public uint U32 { get; set; }

    public ulong U64 { get; set; }
}

public sealed class NullableRecord
{
    public int Id { get; set; }

    public int? Count { get; set; }

    [ParquetDecimal(18, 4)]
    public decimal? Price { get; set; }

    public double? Score { get; set; }

    public bool? Flag { get; set; }

    public DateTime? When { get; set; }
}

public enum Status
{
    Unknown,
    Active,
    Suspended,
}

public sealed class EnumRecord
{
    public int Id { get; set; }

    public Status Status { get; set; }

    public Status? Optional { get; set; }
}

public sealed class DateTimeOffsetRecord
{
    public int Id { get; set; }

    public DateTimeOffset At { get; set; }
}

public sealed class DateOnlyRecord
{
    public int Id { get; set; }

    public DateOnly Day { get; set; }
}

public sealed class TextRecord
{
    public int Id { get; set; }

    public string Text { get; set; } = string.Empty;

    /// <summary>Strings that break naive quoting, escaping, trimming or encoding.</summary>
    public static readonly string[] TrickyValues =
    [
        "plain",
        "comma, inside",
        "semi;colon",
        "quote \" inside",
        "line\nbreak",
        "crlf\r\nbreak",
        "tab\tchar",
        "  padded  ",
        "unicode ✓ 日本語 🚀",
        "=1+2",
    ];
}

public sealed class ControlCharacterRecord
{
    public int Id { get; set; }

    public string Text { get; set; } = string.Empty;
}

public sealed class BinaryRecord
{
    public int Id { get; set; }

    public byte[]? Data { get; set; }
}

public sealed class ListRecord
{
    public int Id { get; set; }

    public List<int>? Values { get; set; }
}

public sealed class Address
{
    public string Street { get; set; } = string.Empty;

    public string Postcode { get; set; } = string.Empty;
}

public sealed class NestedRecord
{
    public int Id { get; set; }

    public Address? Address { get; set; }
}

public sealed record PositionalRecord(int Id, string Name);
