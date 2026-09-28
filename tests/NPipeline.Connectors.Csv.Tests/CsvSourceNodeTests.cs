using System.Globalization;
using System.IO.Compression;
using System.Text;
using AwesomeAssertions;
using CsvHelper;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;

namespace NPipeline.Connectors.Csv.Tests;

public sealed class CsvSourceNodeTests : CsvTestBase
{
    [Fact]
    public async Task Maps_columns_to_members_by_name_ignoring_case_and_order()
    {
        Put("amount,FIRSTNAME,id\n12.5,Ada,1\n7,Grace,2\n");

        var rows = await ReadAsync(Source<Person>());

        rows.Should().BeEquivalentTo([
            new Person { Id = 1, FirstName = "Ada", Amount = 12.5m },
            new Person { Id = 2, FirstName = "Grace", Amount = 7m },
        ]);
    }

    [Fact]
    public async Task A_missing_optional_column_keeps_the_member_initialiser()
    {
        Put("Id,FirstName,Amount\n1,Ada,1\n");

        var row = (await ReadAsync(Source<Person>())).Single();

        row.Country.Should().Be("AU");
    }

    [Fact]
    public async Task A_missing_required_column_fails_once_naming_it()
    {
        Put("Name\nAda\n");

        var read = () => ReadAsync(Source<RequiredPerson>());

        (await read.Should().ThrowAsync<RecordBindingException>()).Which.MissingColumns.Should().Equal("Id");
    }

    [Fact]
    public async Task Missing_columns_can_be_made_an_error()
    {
        Put("Id,FirstName\n1,Ada\n");

        var read = () => ReadAsync(Source<Person>(o => o with { MissingColumns = MissingColumnBehavior.Throw }));

        (await read.Should().ThrowAsync<RecordBindingException>()).Which.MissingColumns.Should().BeEquivalentTo(["Amount", "Country"]);
    }

    [Fact]
    public async Task Builds_positional_records_through_their_constructor()
    {
        Put("firstname,id\nAda,1\n");

        var rows = await ReadAsync(Source<PositionalPerson>());

        rows.Should().Equal(new PositionalPerson(1, "Ada"));
    }

    [Fact]
    public async Task Honours_column_and_ignore_attributes()
    {
        Put("person_id,Name,Secret\n7,Ada,leaked\n");

        var row = (await ReadAsync(Source<AttributedPerson>())).Single();

        row.Id.Should().Be(7);
        row.Secret.Should().Be("unset");
    }

    [Fact]
    public async Task Applies_a_naming_policy()
    {
        Put("id,first_name,amount\n1,Ada,2\n");

        var row = (await ReadAsync(Source<Person>(o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower }))).Single();

        row.FirstName.Should().Be("Ada");
    }

    [Fact]
    public async Task A_value_that_does_not_convert_fails_with_its_position_field_and_raw_record()
    {
        Put("Id,FirstName,Amount\n1,Ada,1\n2,Grace,twelve\n");

        var read = () => ReadAsync(Source<Person>());

        var failure = (await read.Should().ThrowAsync<RecordMappingException>()).Which;
        failure.RecordNumber.Should().Be(2);
        failure.Field.Should().Be("Amount");
        failure.RawExcerpt.Should().Be("2,Grace,twelve\n");
        failure.RecordSource.Should().Be("mem://test/data.csv");
        failure.InnerException.Should().BeOfType<FieldMappingException>().Which.InnerException.Should().BeOfType<FieldConversionException>();
    }

    [Fact]
    public async Task A_row_error_handler_can_skip_bad_rows()
    {
        Put("Id,FirstName,Amount\n1,Ada,1\nx,Bad,1\n3,Grace,3\n");
        var errors = new List<RowError>();

        var rows = await ReadAsync(Source<Person>(o => o with { RowErrorHandler = e => { errors.Add(e); return RowErrorAction.Skip; } }));

        rows.Select(r => r.Id).Should().Equal(1, 3);
        errors.Should().ContainSingle().Which.Should().BeEquivalentTo(new { RecordNumber = 2, Field = "Id" });
    }

    [Fact]
    public async Task Malformed_quoting_is_a_row_error_not_a_failed_file()
    {
        Put("Id,FirstName,Amount\n1,Ada,1\n2,\"Gr\"ace,2\n3,Grace,3\n");

        var rows = await ReadAsync(Source<Person>(o => o with { RowErrorHandler = _ => RowErrorAction.Skip }));

        rows.Select(r => r.Id).Should().Equal(1, 3);
    }

    [Fact]
    public async Task Malformed_quoting_fails_without_a_handler()
    {
        Put("Id,FirstName,Amount\n2,\"Gr\"ace,2\n");

        var read = () => ReadAsync(Source<Person>());

        (await read.Should().ThrowAsync<RecordMappingException>()).Which.InnerException.Should().BeOfType<BadDataException>();
    }

    [Fact]
    public async Task Empty_fields_are_null_for_nullable_members_and_short_rows_read_as_empty()
    {
        Put("Id,Score,Seen\n1,,\n2\n3,9,2026-01-02T03:04:05Z\n");

        var rows = await ReadAsync(Source<NullableRow>());

        rows.Should().BeEquivalentTo([
            new NullableRow { Id = 1 },
            new NullableRow { Id = 2 },
            new NullableRow { Id = 3, Score = 9, Seen = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
        ]);
    }

    [Fact]
    public async Task An_empty_field_for_a_non_nullable_member_is_an_error()
    {
        Put("Id,FirstName,Amount\n1,Ada,\n");

        var read = () => ReadAsync(Source<Person>());

        (await read.Should().ThrowAsync<RecordMappingException>()).Which.Field.Should().Be("Amount");
    }

    [Fact]
    public async Task Without_a_header_columns_follow_member_declaration_order()
    {
        Put("1,Ada,2.5,NZ\n");

        var row = (await ReadAsync(Source<Person>(o => o with { HasHeader = false }))).Single();

        row.Should().BeEquivalentTo(new Person { Id = 1, FirstName = "Ada", Amount = 2.5m, Country = "NZ" });
    }

    [Fact]
    public async Task A_scalar_type_reads_one_value_per_line_without_a_header()
    {
        Put("3\n1\n2\n");

        var rows = await ReadAsync(Source<int>());

        rows.Should().Equal(3, 1, 2);
    }

    [Fact]
    public async Task Uses_the_configured_delimiter_and_culture()
    {
        Put("Id;FirstName;Amount\n1;Ada;1234,5\n");

        var row = (await ReadAsync(Source<Person>(o => o with { Delimiter = ";", Culture = CultureInfo.GetCultureInfo("de-DE") }))).Single();

        row.Amount.Should().Be(1234.5m);
    }

    [Fact]
    public async Task Does_not_detect_the_delimiter_unless_asked()
    {
        Put("Id;FirstName\n1;Ada\n");

        var explicitComma = () => ReadAsync(Source<Person>(o => o with { MissingColumns = MissingColumnBehavior.Throw }));
        var detected = await ReadAsync(Source<Person>(o => o with { DetectDelimiter = true }));

        await explicitComma.Should().ThrowAsync<RecordBindingException>();
        detected.Single().FirstName.Should().Be("Ada");
    }

    [Fact]
    public async Task Reads_dates_and_other_scalars_culture_invariantly()
    {
        Put("At,Day,Time,Span,Key,Weekday,Ratio,Flag\n" +
            "2026-01-02T03:04:05.123+10:00,2026-01-02,13:14:15,1.02:03:04,6f9619ff-8b86-d011-b42d-00cf4fc964ff,friday,0.1,1\n");

        var row = (await ReadAsync(Source<TypedRow>())).Single();

        row.Should().BeEquivalentTo(new TypedRow
        {
            At = new DateTimeOffset(2026, 1, 2, 3, 4, 5, 123, TimeSpan.FromHours(10)),
            Day = new DateOnly(2026, 1, 2),
            Time = new TimeOnly(13, 14, 15),
            Span = new TimeSpan(1, 2, 3, 4),
            Key = Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff"),
            Weekday = DayOfWeek.Friday,
            Ratio = 0.1,
            Flag = true,
        });
    }

    [Fact]
    public async Task Honours_a_byte_order_mark_and_the_configured_encoding()
    {
        Provider.Put(Uri(), [.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes("Id,FirstName,Amount\n1,Zoë,1\n")]);
        Provider.Put(Uri("latin1.csv"), Encoding.Latin1.GetBytes("Id,FirstName,Amount\n2,Zoë,1\n"));

        var utf8 = (await ReadAsync(Source<Person>())).Single();
        var latin1 = (await ReadAsync(Source<Person>(o => o with { Encoding = Encoding.Latin1 }, "latin1.csv"))).Single();

        utf8.Id.Should().Be(1);
        utf8.FirstName.Should().Be("Zoë");
        latin1.FirstName.Should().Be("Zoë");
    }

    [Fact]
    public async Task Reads_every_csv_file_of_a_directory_in_order_including_compressed_ones()
    {
        Put("Id,FirstName,Amount\n2,B,0\n", "in/b.csv");
        Put("Id,FirstName,Amount\n9,Ignored,0\n", "in/notes.txt");

        using (var gz = new MemoryStream())
        {
            await using (var zip = new GZipStream(gz, CompressionLevel.Fastest, true))
            {
                await zip.WriteAsync(Encoding.UTF8.GetBytes("Id,FirstName,Amount\n1,A,0\n"));
            }

            Provider.Put(Uri("in/a.csv.gz"), gz.ToArray());
        }

        var rows = await ReadAsync(Source<Person>(path: "in/"));

        rows.Select(r => r.Id).Should().Equal(1, 2);
    }

    [Fact]
    public void A_member_that_is_not_a_single_value_fails_when_the_node_is_created()
    {
        var create = () => Source<NestedRow>();

        create.Should().Throw<NotSupportedException>().WithMessage("*Tags (List`1)*");
    }

    [Fact]
    public void Validates_options()
    {
        var delimiter = () => Source<Person>(o => o with { Delimiter = "" });

        delimiter.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task A_manual_mapper_reads_by_name_or_index()
    {
        Put("Id,Name\n1,Ada\n2,Grace\n");

        var rows = await ReadAsync(CsvConnector.Source(
            Uri(),
            row => $"{row.RecordNumber}:{row.Get<int>("id")}:{row.Get<string>(1)}:{row.HasColumn("NAME")}:{row["missing"] ?? "-"}",
            o => o with { Provider = Provider }));

        rows.Should().Equal("1:1:Ada:True:-", "2:2:Grace:True:-");
    }

    [Fact]
    public async Task A_manual_mapper_failure_names_the_column()
    {
        Put("Id,Name\nx,Ada\n");

        var read = () => ReadAsync(CsvConnector.Source(Uri(), row => row.Get<int>("Id"), o => o with { Provider = Provider }));

        (await read.Should().ThrowAsync<RecordMappingException>()).Which.Field.Should().Be("Id");
    }

    [Fact]
    public async Task A_manual_mapper_can_read_leniently()
    {
        Put("Id,Name\nx,Ada\n4,Grace\n");

        var rows = await ReadAsync(CsvConnector.Source(
            Uri(),
            row => row.TryGet<int>("Id", out var id) ? id : -1,
            o => o with { Provider = Provider }));

        rows.Should().Equal(-1, 4);
    }
}
