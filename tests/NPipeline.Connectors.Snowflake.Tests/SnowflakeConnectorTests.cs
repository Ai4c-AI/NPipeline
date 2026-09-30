using AwesomeAssertions;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.Snowflake.Tests;

/// <summary>Snowflake's SQL and staged file format, tested without a Snowflake account.</summary>
public sealed class SnowflakeConnectorTests
{
    [Fact]
    public void Members_map_to_upper_snake_case_by_default()
    {
        var plan = SqlWritePlan<Order>.For(SnowflakeShape.Write(new SnowflakeWriteOptions { ConnectionString = "x", Table = "T" }.Naming));

        plan.ColumnNames.Should().Equal("ORDER_ID", "CUSTOMER_NAME", "TOTAL");
    }

    [Fact]
    public void Identity_members_are_not_written()
    {
        var plan = SqlWritePlan<WithIdentity>.For(SnowflakeShape.Write(new SnowflakeWriteOptions { ConnectionString = "x", Table = "T" }.Naming));

        plan.ColumnNames.Should().Equal("NAME");
    }

    [Fact]
    public void Parameters_are_positional_question_marks_named_by_position()
    {
        SnowflakeDialect.Instance.Insert("\"T\"", ["\"A\"", "\"B\""], 2).Should().Be("INSERT INTO \"T\" (\"A\", \"B\") VALUES (?, ?), (?, ?)");
        SnowflakeDialect.Instance.ParameterName(0).Should().Be("1");
        SnowflakeDialect.Instance.ParameterName(3).Should().Be("4");
    }

    [Fact]
    public void The_upsert_is_a_merge_from_values()
    {
        var sql = SnowflakeDialect.Instance.Upsert("\"T\"", ["\"ID\"", "\"NAME\""], ["\"ID\""], SqlUpsertAction.Update, 1);

        sql.Should().Be("MERGE INTO \"T\" AS target USING (SELECT COLUMN1 AS \"ID\", COLUMN2 AS \"NAME\" FROM VALUES (?, ?)) AS source"
                        + " ON target.\"ID\" = source.\"ID\" WHEN MATCHED THEN UPDATE SET target.\"NAME\" = source.\"NAME\""
                        + " WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\") VALUES (source.\"ID\", source.\"NAME\")");
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData("", "\"\"")]
    [InlineData("say \"hi\", then\nleave", "\"say \"\"hi\"\", then\nleave\"")]
    [InlineData(true, "TRUE")]
    [InlineData(1.5, "1.5")]
    public void Staged_fields_keep_null_and_empty_text_apart_and_quote_text(object? value, string expected) =>
        SnowflakeStagedCopyWriter<Order>.Field(value).Should().Be(expected);

    [Fact]
    public void Staged_fields_are_culture_invariant_and_full_precision()
    {
        var culture = System.Globalization.CultureInfo.CurrentCulture;

        try
        {
            System.Globalization.CultureInfo.CurrentCulture = System.Globalization.CultureInfo.GetCultureInfo("de-DE");

            SnowflakeStagedCopyWriter<Order>.Field(1234.5m).Should().Be("1234.5");
            SnowflakeStagedCopyWriter<Order>.Field(new DateTime(2026, 1, 2, 3, 4, 5).AddTicks(1234567)).Should().Be("2026-01-02 03:04:05.1234567");
            SnowflakeStagedCopyWriter<Order>.Field(new byte[] { 0xAB, 0x01 }).Should().Be("AB01");
            SnowflakeStagedCopyWriter<Order>.Field(new DateOnly(2026, 1, 2)).Should().Be("2026-01-02");
        }
        finally
        {
            System.Globalization.CultureInfo.CurrentCulture = culture;
        }
    }

    [Fact]
    public void Options_are_validated()
    {
        var stagedUpsert = () => SnowflakeConnector.Sink<Order>("x", "T", o => o with { WriteStrategy = SnowflakeWriteStrategy.StagedCopy, Upsert = SqlUpsert.On("ORDER_ID") });
        var noStage = () => SnowflakeConnector.Sink<Order>("x", "T", o => o with { Stage = " " });

        stagedUpsert.Should().Throw<NotSupportedException>();
        noStage.Should().Throw<ArgumentException>();
    }

    public sealed record Order(int OrderId, string CustomerName, decimal Total);

    public sealed class WithIdentity
    {
        [SnowflakeColumn("ID", Identity = true)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
