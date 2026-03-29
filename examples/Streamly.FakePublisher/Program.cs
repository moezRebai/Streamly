using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Serilog;
using Streamly;
using Streamly.Audit;
using Streamly.FakePublisher;
using Streamly.FakePublisher.Models;
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

try
{
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
                options.AddHandler<SpotRequest, SpotPrice, SpotPricingHandler>("GetSpotPrice");
                options.AddHandler<IrsRequest, IrsResponse, SwapPricingHandler>("GetIrsPrice");
            });

            services.AddStreamlyMonitoring(options =>
            {
                options.InstanceId             = context.Configuration["Streamly:InstanceId"]
                                                 ?? "fake-publisher-1";
                options.InstanceRole           = "Publisher";
                options.RetainClosedStreamsFor  = TimeSpan.Zero;
                options.PublishRateWindow       = TimeSpan.FromSeconds(10);
            });
            
            services.AddStreamlyAudit(options =>
            {
                options.RetentionDays  = 14;
                options.MaxStorageGb   = 10;
                options.StreamReplicas = 1;
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