using Microsoft.Extensions.Options;
using Streamly.Core.Abstractions;
using Streamly.FakePublisher.Models;

namespace Streamly.FakePublisher;

public class SpotPricingFilterOptions
{
    public const string SectionName = "SpotPricingFilter";

    /// <summary>
    /// Currency pairs this instance will handle.
    /// Empty list = no filter, all pairs accepted (single-cluster default).
    /// </summary>
    public List<string> AllowedCurrencyPairs { get; set; } = [];
}

public class SpotPricingClusterFilter(IOptions<SpotPricingFilterOptions> options) : IClusterAffinityFilter<SpotRequest>
{
    private readonly HashSet<string> _allowed = new(
        options.Value.AllowedCurrencyPairs,
        StringComparer.OrdinalIgnoreCase);

    public bool Accepts(SpotRequest request)
        => _allowed.Count == 0 || _allowed.Contains(request.CurrencyPair);
}
