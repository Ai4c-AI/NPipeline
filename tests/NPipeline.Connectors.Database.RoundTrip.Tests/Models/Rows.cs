using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Database.RoundTrip.Tests.Models;

public sealed record ScalarRow
{
    public int Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public long Big { get; init; }
    public decimal Amount { get; init; }
    public double Ratio { get; init; }
    public bool Active { get; init; }
    public DateTime CreatedAt { get; init; }

    public static readonly Column[] Columns =
    [
        new(nameof(Id), ColumnType.Int, Key: true),
        new(nameof(Name), ColumnType.Text),
        new(nameof(Big), ColumnType.BigInt),
        new(nameof(Amount), ColumnType.Decimal),
        new(nameof(Ratio), ColumnType.Double),
        new(nameof(Active), ColumnType.Bool),
        new(nameof(CreatedAt), ColumnType.DateTime),
    ];

    public static ScalarRow Create(int i) => new()
    {
        Id = i,
        Name = $"name {i}",
        Big = long.MaxValue - i,
        Amount = 1234.5678m + i,
        Ratio = i / 8.0,
        Active = i % 2 == 0,
        CreatedAt = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddSeconds(i),
    };
}

public sealed record NullableRow
{
    public int Id { get; init; }
    public int? Count { get; init; }
    public decimal? Price { get; init; }
    public double? Score { get; init; }
    public bool? Flag { get; init; }
    public DateTime? SeenAt { get; init; }
    public string? Note { get; init; }

    public static readonly Column[] Columns =
    [
        new(nameof(Id), ColumnType.Int, Key: true),
        new(nameof(Count), ColumnType.Int, true),
        new(nameof(Price), ColumnType.Decimal, true),
        new(nameof(Score), ColumnType.Double, true),
        new(nameof(Flag), ColumnType.Bool, true),
        new(nameof(SeenAt), ColumnType.DateTime, true),
        new(nameof(Note), ColumnType.Text, true),
    ];
}

public sealed record ExtendedRow
{
    public int Id { get; init; }
    public Guid Key { get; init; }
    public DateOnly Day { get; init; }
    public TimeOnly Time { get; init; }
    public byte[] Blob { get; init; } = [];

    public static readonly Column[] Columns =
    [
        new(nameof(Id), ColumnType.Int, Key: true),
        new(nameof(Key), ColumnType.Guid),
        new(nameof(Day), ColumnType.Date),
        new(nameof(Time), ColumnType.Time),
        new(nameof(Blob), ColumnType.Binary),
    ];

    public static ExtendedRow Create(int i) => new()
    {
        Id = i,
        Key = new Guid(i, 2, 3, [4, 5, 6, 7, 8, 9, 10, 11]),
        Day = new DateOnly(2026, 1, 2).AddDays(i),
        Time = new TimeOnly(13, 14, 15).Add(TimeSpan.FromMilliseconds(i)),
        Blob = [(byte)i, 0, 255],
    };
}

public sealed record OffsetRow
{
    public int Id { get; init; }
    public DateTimeOffset At { get; init; }

    public static readonly Column[] Columns = [new(nameof(Id), ColumnType.Int, Key: true), new(nameof(At), ColumnType.DateTimeOffset)];
}

public enum Status
{
    Pending,
    Active,
    Suspended,
}

/// <summary>Enums are stored as their underlying integer, as ADO.NET providers pass them.</summary>
public sealed record EnumRow
{
    public int Id { get; init; }
    public Status Status { get; init; }
    public Status? Optional { get; init; }

    public static readonly Column[] Columns =
    [
        new(nameof(Id), ColumnType.Int, Key: true),
        new(nameof(Status), ColumnType.Int),
        new(nameof(Optional), ColumnType.Int, true),
    ];
}

public sealed record TextRow
{
    public int Id { get; init; }
    public string Text { get; init; } = string.Empty;

    public static readonly Column[] Columns = [new(nameof(Id), ColumnType.Int, Key: true), new(nameof(Text), ColumnType.Text)];

    public static readonly string[] TrickyValues =
    [
        "",
        " padded ",
        "single ' and double \" quotes",
        "unicode ✓ 日本語 émoji 🚀",
        "line\nbreak and\r\ncarriage return",
        "tab\tseparated",
        "back\\slash",
        "semi;colon, comma",
        "-- not a comment",
        new string('x', 350),
    ];
}

public sealed record PositionalRow(int Id, string Name);

public sealed record ScoreRow
{
    public int Id { get; init; }
    public int Score { get; init; }
}

public sealed record StrictScoreRow
{
    public int Id { get; init; }
    public int Score { get; init; }
}

public sealed record CachedScoreRow
{
    public int Id { get; init; }
    public int Score { get; init; }
}

public sealed record AmountRow
{
    public int Id { get; init; }
    public decimal Amount { get; init; }
}

/// <summary>A column whose name needs every dialect's quote character escaped.</summary>
public sealed record HostileNameRow
{
    public const string ColumnName = "odd]name\"with`quotes";

    public int Id { get; init; }

    [Column(ColumnName)]
    public string Value { get; init; } = string.Empty;
}

#pragma warning disable CA1720 // The members name the SQL types they stand for.
public enum ColumnType
{
    Int,
    BigInt,
    Decimal,
    Double,
    Bool,
    Text,
    DateTime,
    DateTimeOffset,
    Date,
    Time,
    Guid,
    Binary,
}
#pragma warning restore CA1720

/// <summary>A column of a test table, named after a member; the harness applies the connector's naming convention.</summary>
public sealed record Column(string Member, ColumnType Type, bool Nullable = false, bool Key = false, bool Explicit = false);
