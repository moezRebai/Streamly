using Streamly.Subscriber;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddStreamlySubscriber(context.Configuration, options =>
        {
            options.AddSubscriber<SpotRequest, SpotPrice>("SpotPricer");
        });

        // Add SUBSCRIBER components (reuses existing NATS infrastructure)
        //services.AddSubscriberComponents<SpotPriceRequest, SpotPrice>("SpotPricer");

        // Register the test worker that drives the two clients
        services.AddHostedService<SubscriberWorker>();
    })
    .Build();

await host.RunAsync();