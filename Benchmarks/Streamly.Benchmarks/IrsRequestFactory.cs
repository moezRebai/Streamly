// Generates a deterministic set of IRS requests for load testing.
//
// Each request is unique (distinct Tenor + Notional + date combination) so
// the publisher computes a distinct SHA256 RequestId for each — no deduplication
// short-circuit masking the actual load on the request pipeline.
//
// Usage:
//   var requests = IrsRequestFactory.Generate(count: 1_000);

namespace Streamly.Benchmarks;

public static class IrsRequestFactory
{
    private static readonly string[] Tenors =
        ["1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "30Y"];

    private static readonly decimal[] Notionals =
        [1_000_000m, 5_000_000m, 10_000_000m, 25_000_000m, 50_000_000m, 100_000_000m];

    public static IReadOnlyList<IrsRequest> Generate(int count)
    {
        var requests = new List<IrsRequest>(count);
        // Fixed base date so request IDs are identical across different run days,
        // keeping SHA256 registry paths warm on repeated benchmark sessions.
        var today    = new DateOnly(2024, 1, 1);

        for (var i = 0; i < count; i++)
        {
            var tenor      = Tenors[i % Tenors.Length];
            var notional   = Notionals[i % Notionals.Length];
            var tenorYears = TenorToYears(tenor);

            // Stagger effective dates by index so requests with the same
            // tenor/notional combination still produce unique SHA256 hashes.
            var effectiveDate = today.AddDays(i / (Tenors.Length * Notionals.Length));
            var maturityDate  = effectiveDate.AddYears(tenorYears);

            requests.Add(new IrsRequest
            {
                Tenor         = tenor,
                Notional      = notional,
                EffectiveDate = effectiveDate,
                MaturityDate  = maturityDate,
            });
        }

        return requests;
    }

    private static int TenorToYears(string tenor) => tenor switch
    {
        "1Y"  => 1,
        "2Y"  => 2,
        "3Y"  => 3,
        "5Y"  => 5,
        "7Y"  => 7,
        "10Y" => 10,
        "15Y" => 15,
        "20Y" => 20,
        "30Y" => 30,
        _     => 5,
    };
}
