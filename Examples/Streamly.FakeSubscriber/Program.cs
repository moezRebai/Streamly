using System.Diagnostics;
using Serilog;
using Streamly;
using Streamly.FakeSubscriber;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File($"logs/{Process.GetCurrentProcess().ProcessName}-.log",
        rollingInterval: RollingInterval.Day)
    .CreateLogger();

var host = Host.CreateDefaultBuilder(args)
    .UseSerilog()
    .ConfigureServices((context, services) =>
    {
        services.AddStreamly(context.Configuration, options =>
        {
            options.AddSubscriber<SpotRequest, SpotPrice>("SpotPricer");
            options.AddSubscriber<IrsRequest, IrsResponse>("IrsPricer");
        });
        
        //services.AddHostedService<SubscriberWorker>();
        services.AddHostedService<IrsSubscriberWorker>();
    })
    .Build();

await host.RunAsync();