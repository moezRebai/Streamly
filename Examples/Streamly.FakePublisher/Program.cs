using System.Diagnostics;
using Serilog;
using Streamly;
using Streamly.FakePublisher;
using Streamly.FakePublisher.IRSwap;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    //.WriteTo.Console()
    .WriteTo.File($"logs/{Process.GetCurrentProcess().ProcessName}-.log",
        rollingInterval: RollingInterval.Day)
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
                options.AddHandler<IrsRequest, IrsResponse, IrsPricingHandler>("IrsPricer");
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