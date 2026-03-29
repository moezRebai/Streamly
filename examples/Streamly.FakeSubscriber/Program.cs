using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Serilog;
using Streamly;
using Streamly.FakeSubscriber;
using Streamly.Monitoring;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft.AspNetCore.Hosting.Diagnostics", Serilog.Events.LogEventLevel.Warning)
    .MinimumLevel.Override("Microsoft.AspNetCore.Routing", Serilog.Events.LogEventLevel.Warning)
    .Enrich.FromLogContext()
    //.WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext} — {Message:lj}{NewLine}{Exception}")
    .WriteTo.File($"logs/{Process.GetCurrentProcess().ProcessName}-.log",
        rollingInterval: RollingInterval.Day,
        outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext} — {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var host = Host.CreateDefaultBuilder(args)
    .UseSerilog()
    .ConfigureWebHostDefaults(webBuilder =>
    {
        webBuilder.Configure(app =>
        {
            app.UseRouting();
            app.UseEndpoints(endpoints => endpoints.MapStreamlyEndpoints());
        });
    })
    .ConfigureServices((context, services) =>
    {
        services.AddRouting();

        services.AddStreamly(context.Configuration, options =>
        {
            options.AddSubscriber<SpotRequest, SpotPrice>("GetSpotPrice");
            options.AddSubscriber<IrsRequest, IrsResponse>("GetIrsPrice");
        });

        services.AddStreamlyMonitoring(options =>
        {
            options.InstanceId   = context.Configuration["Streamly:InstanceId"]
                                   ?? "fake-subscriber-1";
            options.InstanceRole = "Subscriber";
        });

        services.AddHostedService<SpotSubscriberWorker>();
        services.AddHostedService<SwapSubscriberWorker>();
    })
    .Build();

await host.RunAsync();