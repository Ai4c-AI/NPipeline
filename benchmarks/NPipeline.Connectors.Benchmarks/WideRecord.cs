using NPipeline.Connectors.Parquet.Attributes;

namespace NPipeline.Connectors.Benchmarks;

/// <summary>
///     A 20-column record with the column mix of a typical business export: integers, decimals, text, dates, a flag
///     and an identifier. Every connector benchmark reads and writes this shape so results are comparable.
/// </summary>
public sealed class WideRecord
{
    public int Id { get; set; }

    public int CustomerId { get; set; }

    public int Quantity { get; set; }

    public int Region { get; set; }

    public long OrderNumber { get; set; }

    public long WarehouseId { get; set; }

    public long Sequence { get; set; }

    public double Weight { get; set; }

    public double Discount { get; set; }

    public double Score { get; set; }

    [ParquetDecimal(18, 4)]
    public decimal UnitPrice { get; set; }

    [ParquetDecimal(18, 4)]
    public decimal Total { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Email { get; set; } = string.Empty;

    public string City { get; set; } = string.Empty;

    public string Notes { get; set; } = string.Empty;

    public DateTime CreatedUtc { get; set; }

    public DateTime ShippedUtc { get; set; }

    public bool Priority { get; set; }

    public Guid TrackingId { get; set; }

    public static WideRecord[] Generate(int count)
    {
        var random = new Random(42);
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        string[] cities = ["Brisbane", "Sydney", "Melbourne", "Perth", "Adelaide", "Hobart", "Darwin", "Canberra"];
        var records = new WideRecord[count];

        for (var i = 0; i < count; i++)
        {
            var quantity = random.Next(1, 50);
            var unitPrice = Math.Round((decimal)(random.NextDouble() * 500), 2);

            records[i] = new WideRecord
            {
                Id = i,
                CustomerId = random.Next(1, 100_000),
                Quantity = quantity,
                Region = random.Next(1, 9),
                OrderNumber = 1_000_000_000L + i,
                WarehouseId = random.Next(1, 40),
                Sequence = i * 7L,
                Weight = Math.Round(random.NextDouble() * 30, 3),
                Discount = Math.Round(random.NextDouble() * 0.3, 2),
                Score = random.NextDouble(),
                UnitPrice = unitPrice,
                Total = unitPrice * quantity,
                Name = $"Customer {i:D6}",
                Email = $"customer{i}@example.com",
                City = cities[i % cities.Length],
                Notes = i % 5 == 0 ? "Leave at the front door, beside the \"blue\" pot" : string.Empty,
                CreatedUtc = start.AddSeconds(i * 13),
                ShippedUtc = start.AddSeconds((i * 13) + 86_400),
                Priority = i % 3 == 0,
                TrackingId = new Guid(i, (short)(i >> 16), 0, [1, 2, 3, 4, 5, 6, 7, 8]),
            };
        }

        return records;
    }
}
