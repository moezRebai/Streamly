1. I only need to PUBLISH
csharpservices.AddStreamly(configuration, options =>
{
    options.AddHandler<MyRequest, MyResponse, MyHandler>("MyStream");
});
✅ Includes: ILeaderElection, IStreamingTransport, handlers, runtime

2. I only need to SUBSCRIBE
csharpservices.AddStreamlySubscriber(configuration, options =>
{
    options.AddSubscriber<MyRequest, MyResponse>("MyStream");
});
✅ Includes: IStreamingTransport, subscriber components
❌ NO ILeaderElection (subscribers don't need it)
DON'T use AddStreamly() for subscribe-only - it includes unnecessary leader election!

3. I need BOTH (Subscribe + Publish)
csharp// Step 1: Register as Publisher (includes ILeaderElection)
services.AddStreamly(configuration, options =>
{
    // Streams I PUBLISH
    options.AddHandler<FxSwapRequest, FxSwapPrice, FxSwapHandler>("FxSwapPricer");
});

// Step 2: Add Subscriber components (reuses infrastructure)
services.AddSubscriberComponents<SpotPriceRequest, SpotPrice>("SpotPricer");
✅ Includes: Everything (leader election + publish + subscribe)