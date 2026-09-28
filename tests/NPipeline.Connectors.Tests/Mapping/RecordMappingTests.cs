using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Mapping;
using Xunit;

namespace NPipeline.Connectors.Tests.Mapping;

/// <summary>A row of text fields, as a CSV reader would expose it.</summary>
public sealed class TextRowReader : IFieldReader
{
    public string?[] Fields { get; set; } = [];

    public TValue GetValue<TValue>(int ordinal) =>
        Fields[ordinal] is { } text
            ? ScalarParser.Parse<TValue>(text)
            : ScalarConverter.Convert<TValue>(null);
}

/// <summary>Records every write with the static type it was written as.</summary>
public sealed class RecordingWriter : IFieldWriter
{
    public List<(int Ordinal, Type Type, object? Value)> Writes { get; } = [];

    public void WriteValue<TValue>(int ordinal, TValue value) => Writes.Add((ordinal, typeof(TValue), value));
}

public sealed class RecordShapeTests
{
    [Fact]
    public void A_flat_format_rejects_members_that_are_not_single_values()
    {
        RecordShape.For<Customer>().ThrowIfNotFlat("CSV");

        FluentActions.Invoking(() => RecordShape.For<WithList>().ThrowIfNotFlat("CSV"))
            .Should().Throw<NotSupportedException>().WithMessage("CSV columns hold single values, but WithList maps Tags (List`1)*");
    }

    [Fact]
    public void Column_attribute_wins_over_the_naming_policy()
    {
        var shape = RecordShape.For<Customer>(new RecordShapeOptions { Naming = ColumnNamingPolicy.SnakeCaseLower });

        shape.Members.Select(m => m.ColumnName).Should().Equal("id", "full_name", "email_address", "http_server");
        shape.Members.Single(m => m.Name == nameof(Customer.Email)).HasExplicitColumn.Should().BeTrue();
    }

    [Fact]
    public void Ignored_members_are_left_out()
    {
        var shape = RecordShape.For<Customer>(new RecordShapeOptions { IsIgnored = m => m.Name == nameof(Customer.HttpServer) });

        shape.Members.Select(m => m.Name).Should().Equal(nameof(Customer.Id), nameof(Customer.FullName), nameof(Customer.Email));
    }

    [Fact]
    public void A_connector_can_supply_explicit_names()
    {
        var shape = RecordShape.For<Customer>(new RecordShapeOptions { ColumnName = m => m.Name == nameof(Customer.Id) ? "customer_id" : null });

        shape.Members[0].ColumnName.Should().Be("customer_id");
        shape.Members[0].HasExplicitColumn.Should().BeTrue();
    }

    [Fact]
    public void Base_class_members_come_first()
    {
        RecordShape.For<DerivedRecord>().Members.Select(m => m.Name).Should().Equal(nameof(BaseRecord.BaseId), nameof(DerivedRecord.Extra));
    }

    [Fact]
    public void Positional_records_are_constructed_through_their_constructor()
    {
        var shape = RecordShape.For<PositionalPerson>();

        shape.Constructor!.GetParameters().Select(p => p.Name).Should().Equal("Id", "Name", "Country");
        shape.Members.Single(m => m.Name == nameof(PositionalPerson.Id)).IsRequired.Should().BeTrue();
        shape.Members.Single(m => m.Name == nameof(PositionalPerson.Country)).IsRequired.Should().BeFalse("its parameter has a default");
    }

    [Fact]
    public void Required_members_are_detected()
    {
        RecordShape.For<RequiredRecord>().Members.Single(m => m.Name == nameof(RequiredRecord.Code)).IsRequired.Should().BeTrue();
    }

    [Fact]
    public void Types_without_a_usable_constructor_report_why()
    {
        RecordShape.For<Unconstructable>().ConstructionError.Should().Contain("no parameterless constructor");
    }

    [Fact]
    public void Scalar_types_have_no_members()
    {
        var shape = RecordShape.For<DateTime>();

        shape.IsScalar.Should().BeTrue();
        shape.Members.Should().BeEmpty();
    }

    [Theory]
    [InlineData(typeof(int?), true)]
    [InlineData(typeof(Status), true)]
    [InlineData(typeof(DateOnly), true)]
    [InlineData(typeof(byte[]), true)]
    [InlineData(typeof(Customer), false)]
    [InlineData(typeof(List<int>), false)]
    public void Classifies_scalars(Type type, bool expected) => TypeClassifier.IsScalar(type).Should().Be(expected);
}

public sealed class RecordBinderTests
{
    private readonly TextRowReader _reader = new();

    [Fact]
    public void Matches_headers_case_insensitively_and_ignores_extra_columns()
    {
        var map = RecordBinder.Bind<Order, TextRowReader>(["ID", "amount", "unused", "Status"]);
        _reader.Fields = ["7", "12.5", "x", "Active"];

        map(_reader).Should().BeEquivalentTo(new Order { Id = 7, Amount = 12.5m, Status = Status.Active });
    }

    [Fact]
    public void Uses_the_first_of_duplicate_columns()
    {
        var map = RecordBinder.Bind<Order, TextRowReader>(["Id", "id"], new RecordBindingOptions { MissingColumns = MissingColumnBehavior.Ignore });
        _reader.Fields = ["1", "2"];

        map(_reader).Id.Should().Be(1);
    }

    [Fact]
    public void Sets_init_only_properties()
    {
        var map = RecordBinder.Bind<InitOnly, TextRowReader>(["Id", "Name"]);
        _reader.Fields = ["3", "Ada"];

        map(_reader).Should().BeEquivalentTo(new InitOnly { Id = 3, Name = "Ada" });
    }

    [Fact]
    public void Constructs_positional_records_and_uses_parameter_defaults()
    {
        var map = RecordBinder.Bind<PositionalPerson, TextRowReader>(["name", "id"]);
        _reader.Fields = ["Grace", "2"];

        map(_reader).Should().Be(new PositionalPerson(2, "Grace", "AU"));
    }

    [Fact]
    public void Constructs_record_structs()
    {
        var map = RecordBinder.Bind<PointRecord, TextRowReader>(["X", "Y"]);
        _reader.Fields = ["1", "2"];

        map(_reader).Should().Be(new PointRecord { X = 1, Y = 2 });
    }

    [Fact]
    public void Missing_optional_columns_keep_initialisers()
    {
        var map = RecordBinder.Bind<Defaulted, TextRowReader>(["Id"]);
        _reader.Fields = ["1"];

        map(_reader).Country.Should().Be("AU");
    }

    [Fact]
    public void Missing_explicit_or_required_columns_fail_binding_once()
    {
        var explicitColumn = () => RecordBinder.Bind<Customer, TextRowReader>(["id"]);
        var required = () => RecordBinder.Bind<RequiredRecord, TextRowReader>(["other"]);
        var constructorParameter = () => RecordBinder.Bind<PositionalPerson, TextRowReader>(["name"]);

        explicitColumn.Should().Throw<RecordBindingException>().Which.MissingColumns.Should().Equal("email_address");
        required.Should().Throw<RecordBindingException>().Which.AvailableColumns.Should().Equal("other");
        constructorParameter.Should().Throw<RecordBindingException>().Which.MissingColumns.Should().Equal("Id");
    }

    [Fact]
    public void Throw_behaviour_requires_every_column()
    {
        var bind = () => RecordBinder.Bind<Defaulted, TextRowReader>(["Id"], new RecordBindingOptions { MissingColumns = MissingColumnBehavior.Throw });

        bind.Should().Throw<RecordBindingException>().Which.MissingColumns.Should().Equal("Country");
    }

    [Fact]
    public void Ignore_behaviour_never_fails()
    {
        var map = RecordBinder.Bind<RequiredRecord, TextRowReader>([], new RecordBindingOptions { MissingColumns = MissingColumnBehavior.Ignore });

        map(_reader).Code.Should().BeNull();
    }

    [Fact]
    public void A_bad_value_reports_the_member_and_column()
    {
        var map = RecordBinder.Bind<Order, TextRowReader>(["Id", "AMOUNT", "Status"]);
        _reader.Fields = ["1", "1,5", "Active"];

        var failure = FluentActions.Invoking(() => map(_reader)).Should().Throw<FieldMappingException>().Which;

        failure.Member.Should().Be(nameof(Order.Amount));
        failure.Column.Should().Be("AMOUNT", "the column is reported as the file spells it");
        failure.InnerException.Should().BeOfType<FieldConversionException>();
    }

    [Fact]
    public void A_missing_value_for_a_non_nullable_member_fails()
    {
        var map = RecordBinder.Bind<Order, TextRowReader>(["Id", "Amount", "Status"]);
        _reader.Fields = [null, "1", "Active"];

        FluentActions.Invoking(() => map(_reader)).Should().Throw<FieldMappingException>().Which.Member.Should().Be(nameof(Order.Id));
    }

    [Fact]
    public void Binds_scalar_types_to_the_first_column()
    {
        var map = RecordBinder.Bind<DateOnly, TextRowReader>(["day"]);
        _reader.Fields = ["2026-01-02"];

        map(_reader).Should().Be(new DateOnly(2026, 1, 2));
    }

    [Fact]
    public void Types_that_cannot_be_constructed_fail_binding()
    {
        var bind = () => RecordBinder.Bind<Unconstructable, TextRowReader>(["Id"]);

        bind.Should().Throw<RecordBindingException>().WithMessage("*no parameterless constructor*");
    }

    [Fact]
    public void Mappers_are_cached_by_column_layout()
    {
        var first = RecordBinder.Bind<Order, TextRowReader>(["Id", "Amount", "Status"]);
        var same = RecordBinder.Bind<Order, TextRowReader>(["Id", "Amount", "Status"]);
        var other = RecordBinder.Bind<Order, TextRowReader>(["Status", "Amount", "Id"]);

        same.Should().BeSameAs(first);
        other.Should().NotBeSameAs(first);
    }
}

public sealed class RecordWriterPlanTests
{
    [Fact]
    public void Writes_each_readable_member_with_its_static_type()
    {
        var plan = RecordWriterPlan.Create<Order, RecordingWriter>();
        var writer = new RecordingWriter();

        plan.Write(writer, new Order { Id = 7, Amount = 1.5m, Status = Status.Suspended, Note = null });

        plan.ColumnNames.Should().Equal("Id", "Amount", "Status", "Note");
        writer.Writes.Should().Equal(
            (0, typeof(int), 7),
            (1, typeof(decimal), 1.5m),
            (2, typeof(Status), Status.Suspended),
            (3, typeof(string), null));
    }

    [Fact]
    public void Applies_the_naming_policy()
    {
        var plan = RecordWriterPlan.Create<Customer, RecordingWriter>(new RecordShapeOptions { Naming = ColumnNamingPolicy.CamelCase });

        plan.ColumnNames.Should().Equal("id", "fullName", "email_address", "httpServer");
    }

    [Fact]
    public void Writes_scalars_as_one_column()
    {
        var plan = RecordWriterPlan.Create<DateTime, RecordingWriter>();
        var writer = new RecordingWriter();
        var value = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

        plan.Write(writer, value);

        plan.IsScalar.Should().BeTrue();
        plan.ColumnNames.Should().Equal("Value");
        writer.Writes.Should().Equal((0, typeof(DateTime), value));
    }

    [Fact]
    public void Writes_positional_records()
    {
        var plan = RecordWriterPlan.Create<PositionalPerson, RecordingWriter>();
        var writer = new RecordingWriter();

        plan.Write(writer, new PositionalPerson(1, "Ada", "NZ"));

        writer.Writes.Select(w => w.Value).Should().Equal(1, "Ada", "NZ");
    }
}

public sealed class Customer
{
    public int Id { get; set; }

    public string FullName { get; set; } = string.Empty;

    [Column("email_address")]
    public string Email { get; set; } = string.Empty;

    public string HttpServer { get; set; } = string.Empty;

    [IgnoreColumn]
    public string Secret { get; set; } = string.Empty;
}

public sealed class Order
{
    public int Id { get; set; }

    public decimal Amount { get; set; }

    public Status Status { get; set; }

    public string? Note { get; set; }
}

public sealed class InitOnly
{
    public int Id { get; init; }

    public string Name { get; init; } = string.Empty;
}

public sealed record PositionalPerson(int Id, string Name, string Country = "AU");

public record struct PointRecord(int X, int Y)
{
    public PointRecord()
        : this(0, 0)
    {
    }
}

public sealed class Defaulted
{
    public int Id { get; set; }

    public string Country { get; set; } = "AU";
}

public sealed class RequiredRecord
{
    public required string Code { get; set; }
}

/// <summary>Its constructor parameter matches no member, so the binder cannot supply it.</summary>
public sealed class Unconstructable(Guid secret)
{
    public int Id { get; set; }

    public bool HasSecret => secret != Guid.Empty;
}

public class BaseRecord
{
    public int BaseId { get; set; }
}

public sealed class DerivedRecord : BaseRecord
{
    public string Extra { get; set; } = string.Empty;
}

public sealed class WithList
{
    public int Id { get; set; }

    public List<string> Tags { get; set; } = [];
}
