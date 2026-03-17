using System.Diagnostics;
using Serilog;
using Streamly.Core.Runtime;
using Streamly.Publisher;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.File($"logs/{Process.GetCurrentProcess().ProcessName}-.log", rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    var host = Host.CreateDefaultBuilder(args)
        .UseSerilog()
        .ConfigureServices((context, services) =>
        {
            services.AddStreamly(context.Configuration, options =>
            {
                options.AddHandler<SpotRequest, SpotPrice, SpotPricingHandler>("SpotPricer");
            });
        })
        .Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application failed to start");
}
finally
{
    Log.CloseAndFlush();
}