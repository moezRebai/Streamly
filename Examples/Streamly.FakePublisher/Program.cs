using Streamly.Core.Runtime;
using Streamly.Publisher;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddStreamly(context.Configuration, options =>
        {
            options.AddHandler<SpotRequest, SpotPrice, SpotPricingHandler>("SpotPricer");
        });
    })
    .Build();

await host.RunAsync();