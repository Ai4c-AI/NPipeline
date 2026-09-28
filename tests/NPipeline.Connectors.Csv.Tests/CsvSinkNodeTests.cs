using System.Globalization;
using System.IO.Compression;
using System.Text;
using AwesomeAssertions;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Csv.Tests;

public sealed class CsvSinkNodeTests : CsvTestBase
{
    [Fact]
    public async Task Writes_a_header_with_member_names_as_they_are_and_one_row_per_item()
    {
        await WriteAsync(Sink<Person>(), new Person { Id = 1, FirstName = "Ada", Amount = 12.5m }, new Person { Id = 2, FirstName = "Grace", Amount = 7m });

        Text().Should().Be("Id,FirstName,Amount,Country\n1,Ada,12.5,AU\n2,Grace,7,AU\n");
    }

    [Fact]
    public async Task Applies_attributes_and_the_naming_policy()
    {
        await WriteAsync(Sink<AttributedPerson>(o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower }), new AttributedPerson { Id = 7, Name = "Ada" });

        Text().Should().Be("person_id,name\n7,Ada\n");
    }

    [Fact]
    public async Task Writes_values_that_read_back_unchanged_under_any_culture()
    {
        var row = new TypedRow
        {
            At = new DateTimeOffset(2026, 1, 2, 3, 4, 5, 123, TimeSpan.FromHours(10)),
            Day = new DateOnly(2026, 1, 2),
            Time = new TimeOnly(13, 14, 15, 500),
            Span = new TimeSpan(1, 2, 3, 4),
            Key = Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff"),
            Weekday = DayOfWeek.Friday,
            Ratio = 0.1,
            Flag = true,
        };

        await WriteAsync(Sink<TypedRow>(), row);

        Text().Should().Be(
            "At,Day,Time,Span,Key,Weekday,Ratio,Flag\n" +
            "2026-01-02T03:04:05.1230000+10:00,2026-01-02,13:14:15.5000000,1.02:03:04,6f9619ff-8b86-d011-b42d-00cf4fc964ff,Friday,0.1,true\n");

        (await ReadAsync(Source<TypedRow>())).Single().Should().BeEquivalentTo(row);
    }

    [Fact]
    public async Task Null_values_are_empty_fields()
    {
        await WriteAsync(Sink<NullableRow>(), new NullableRow { Id = 1 });

        Text().Should().Be("Id,Score,Seen\n1,,\n");
    }

    [Fact]
    public async Task Quotes_fields_that_need_it_and_keeps_surrounding_whitespace()
    {
        await WriteAsync(Sink<Person>(), new Person { Id = 1, FirstName = "a,\"b\"\nc", Country = "  padded  " });

        Text().Should().Be("Id,FirstName,Amount,Country\n1,\"a,\"\"b\"\"\nc\",0,\"  padded  \"\n");
        (await ReadAsync(Source<Person>())).Single().Should().BeEquivalentTo(new Person { Id = 1, FirstName = "a,\"b\"\nc", Country = "  padded  " });
    }

    [Fact]
    public async Task A_scalar_type_writes_one_column_without_a_header()
    {
        await WriteAsync(Sink<DateTime>(), new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), new DateTime(2026, 1, 3, 0, 0, 0, DateTimeKind.Utc));

        Text().Should().Be("2026-01-02T03:04:05.0000000Z\n2026-01-03T00:00:00.0000000Z\n");
    }

    [Fact]
    public async Task Headers_can_be_turned_off_or_on()
    {
        await WriteAsync(Sink<Person>(o => o with { HasHeader = false }), new Person { Id = 1 });
        await WriteAsync(Sink<int>(o => o with { HasHeader = true }, "ints.csv"), 5);

        Text().Should().Be("1,,0,AU\n");
        Text("ints.csv").Should().Be("Value\n5\n");
    }

    [Fact]
    public async Task Uses_the_configured_delimiter_culture_and_line_ending()
    {
        await WriteAsync(
            Sink<Person>(o => o with { Delimiter = ";", Culture = CultureInfo.GetCultureInfo("de-DE"), NewLine = "\r\n" }),
            new Person { Id = 1, FirstName = "Ada", Amount = 1234.5m });

        Text().Should().Be("Id;FirstName;Amount;Country\r\n1;Ada;1234,5;AU\r\n");
    }

    [Fact]
    public async Task Null_items_fail_by_default_and_can_be_skipped()
    {
        var fail = () => WriteAsync(Sink<Person?>(), new Person { Id = 1 }, null);

        await fail.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null item*");
        await WriteAsync(Sink<Person?>(o => o with { NullItems = NullItemHandling.Skip }), new Person { Id = 1 }, null, new Person { Id = 2 });

        Text().Should().Be("Id,FirstName,Amount,Country\n1,,0,AU\n2,,0,AU\n");
    }

    [Fact]
    public async Task Compresses_by_suffix()
    {
        await WriteAsync(Sink<int>(path: "out.csv.gz"), 1, 2);

        await using var zip = new GZipStream(new MemoryStream(Provider.Get(Uri("out.csv.gz"))), CompressionMode.Decompress);
        using var reader = new StreamReader(zip, Encoding.UTF8);
        (await reader.ReadToEndAsync()).Should().Be("1\n2\n");
        (await ReadAsync(Source<int>(path: "out.csv.gz"))).Should().Equal(1, 2);
    }

    [Fact]
    public async Task A_manual_writer_writes_the_given_columns()
    {
        var sink = CsvConnector.Sink<Person>(
            Uri(),
            ["id", "name"],
            (row, person) =>
            {
                row.Write(person.Id);
                row.WriteText(person.FirstName.ToUpperInvariant());
            },
            o => o with { Provider = Provider });

        await WriteAsync(sink, new Person { Id = 3, FirstName = "Ada" });

        Text().Should().Be("id,name\n3,ADA\n");
    }

    [Fact]
    public void A_member_that_is_not_a_single_value_fails_when_the_node_is_created()
    {
        var create = () => Sink<NestedRow>();

        create.Should().Throw<NotSupportedException>().WithMessage("*Tags*");
    }

    [Fact]
    public async Task Round_trips_positional_records()
    {
        await WriteAsync(Sink<PositionalPerson>(), new PositionalPerson(1, "Ada"), new PositionalPerson(2, "Grace"));

        (await ReadAsync(Source<PositionalPerson>())).Should().Equal(new PositionalPerson(1, "Ada"), new PositionalPerson(2, "Grace"));
    }
}
