// // FILE: Streamly.LoadTests/Scenarios/FailoverScenario.cs
// //
// // Measures leader failover time: the gap between the last message received
// // from the old leader and the first message received from the new leader,
// // as observed by the subscriber (no cross-process coordination needed).
// //
// // NFR target: < 1000ms
// //
// // Methodology:
// //   1. Start Publisher1 (becomes leader) + Publisher2 (standby follower) + Subscriber
// //   2. Open a subscription and confirm prices are arriving from Publisher1
// //   3. Kill Publisher1 abruptly (StopAsync with no graceful shutdown)
// //   4. Measure the gap between the last Publisher1 message and the first
// //      Publisher2 message, using Stopwatch timestamps on the subscriber side
//
// using System.Diagnostics;
// using Microsoft.Extensions.Configuration;
// using Microsoft.Extensions.Hosting;
// using Microsoft.Extensions.Logging;
// using Streamly.Core.Models;
// using Streamly.Core.Runtime;
// using Streamly.LoadTests;
// using Streamly.Subscriber;
//
// namespace Streamly.LoadTests.Scenarios;
//
// public class FailoverScenario
// {
//     private const string NatsUrl    = "nats://localhost:4222";
//     private const string StreamName = "FailoverBench";
//
//     // How long to wait for prices to arrive before declaring the pre-kill
//     // phase stable. Must be long enough for leader election to complete.
//     private static readonly TimeSpan WarmupDelay = TimeSpan.FromSeconds(3);
//
//     // Timeout waiting for the new leader to publish after the kill.
//     private static readonly TimeSpan NewLeaderTimeout = TimeSpan.FromSeconds(5);
//
//     // Cool-down between iterations to let NATS and publishers fully settle.
//     private static readonly TimeSpan IterationCooldown = TimeSpan.FromSeconds(3);
//
//     public async Task RunAsync(int iterations = 5)
//     {
//         Console.WriteLine("\n=== Failover Scenario ===");
//         Console.WriteLine($"Iterations  : {iterations}");
//         Console.WriteLine($"NFR target  : < 1000ms");
//         Console.WriteLine($"New-leader timeout : {NewLeaderTimeout.TotalSeconds:F0}s\n");
//
//         using var reporter = new CsvReporter("Failover");
//         var failoverTimes = new List<double>();
//
//         for (var i = 0; i < iterations; i++)
//         {
//             Console.WriteLine($"[Iteration {i + 1}/{iterations}]");
//
//             var failoverMs = await RunSingleIterationAsync(reporter);
//
//             if (failoverMs >= 0)
//             {
//                 failoverTimes.Add(failoverMs);
//                 var status = failoverMs < 1000 ? "✓" : "✗";
//                 Console.WriteLine($"  Failover time : {failoverMs:F0}ms  {status}");
//             }
//
//             if (i < iterations - 1)
//                 await Task.Delay(IterationCooldown);
//         }
//
//         reporter.PrintSummary("Failover Time (ms)", failoverTimes);
//
//         var passed = failoverTimes.Count > 0 && failoverTimes.All(t => t < 1000);
//         Console.WriteLine($"\nNFR < 1000ms : {(passed ? "PASS ✓" : "FAIL ✗")}");
//     }
//
//     // ─── Single iteration ────────────────────────────────────────────────────
//
//     private async Task<double> RunSingleIterationAsync(CsvReporter reporter)
//     {
//         var publisher1 = BuildPublisherHost("FailoverPub1");
//         var publisher2 = BuildPublisherHost("FailoverPub2");
//         var subscriberHost = BuildSubscriberHost();
//
//         await publisher1.StartAsync();
//         await publisher2.StartAsync();
//         await subscriberHost.StartAsync();
//
//         try
//         {
//             // Allow time for leader election to complete and first prices to flow.
//             await Task.Delay(WarmupDelay);
//
//             var subscriber = subscriberHost.Services
//                 .GetRequiredService<IStreamingSubscriber<FailoverRequest, FailoverPrice>>();
//
//             // Track which publisher is sending and when.
//             string? activePublisherId = null;
//             long    lastMessageTick   = 0;
//             long    firstNewLeaderTick = 0;
//             string? newLeaderId       = null;
//
//             // Set to non-zero the instant we fire StopAsync on publisher1.
//             // Volatile ensures the onNext callback on any thread sees the update
//             // without stale cache reads.
//             volatile long leaderKilledTick = 0;
//
//             var tcsNewLeader = new TaskCompletionSource<long>(
//                 TaskCreationOptions.RunContinuationsAsynchronously);
//
//             // Fluent Subscribe API — matches the real IStreamingSubscriber contract.
//             var subscription = subscriber
//                 .Subscribe(
//                     new FailoverRequest("EUR/USD"),
//                     behavior: StreamBehavior.Live)
//                 .Subscribe(
//                     onNext: price =>
//                     {
//                         var now = Stopwatch.GetTimestamp();
//
//                         if (Interlocked.Read(ref leaderKilledTick) == 0)
//                         {
//                             // Pre-kill: track whoever is currently publishing.
//                             activePublisherId = price.PublisherId;
//                             Interlocked.Exchange(ref lastMessageTick, now);
//                         }
//                         else if (price.PublisherId != activePublisherId
//                                  && !tcsNewLeader.Task.IsCompleted)
//                         {
//                             // Post-kill: first message from a different publisher.
//                             firstNewLeaderTick = now;
//                             newLeaderId = price.PublisherId;
//                             tcsNewLeader.TrySetResult(now);
//                         }
//                     },
//                     onError: ex =>
//                     {
//                         // Subscription error during failover is expected and
//                         // handled by the watchdog/retry layer inside the subscriber.
//                         // We don't fail the TCS here — wait for the new leader message.
//                     });
//
//             // Confirm prices are arriving before we proceed with the kill.
//             await Task.Delay(2000);
//
//             if (activePublisherId is null)
//             {
//                 Console.WriteLine("  ⚠  No prices received before kill — skipping iteration");
//                 subscription.Dispose();
//                 return -1;
//             }
//
//             // ── Kill the leader ──────────────────────────────────────────────
//             Console.WriteLine($"  Killing leader ({activePublisherId})...");
//             var killedAt = Stopwatch.GetTimestamp();
//             Interlocked.Exchange(ref leaderKilledTick, killedAt);
//
//             reporter.RecordFailover("LeaderKilled", activePublisherId, 0);
//
//             // StopAsync without awaiting — we want an abrupt kill, not graceful.
//             // Publisher1 is assumed to be the leader because it started first and
//             // won the initial election. For a more robust test, inspect the
//             // publisher's IsLeader property before deciding which host to kill.
//             _ = publisher1.StopAsync(CancellationToken.None);
//
//             // ── Wait for new leader's first message ──────────────────────────
//             using var cts = new CancellationTokenSource(NewLeaderTimeout);
//             cts.Token.Register(() =>
//                 tcsNewLeader.TrySetException(
//                     new TimeoutException(
//                         $"New leader did not publish within {NewLeaderTimeout.TotalSeconds:F0}s")));
//
//             try
//             {
//                 await tcsNewLeader.Task;
//
//                 var failoverMs = (firstNewLeaderTick - killedAt)
//                     / (double)Stopwatch.Frequency * 1000.0;
//
//                 reporter.RecordFailover(
//                     "NewLeaderFirstMessage",
//                     newLeaderId ?? "unknown",
//                     (long)failoverMs);
//
//                 subscription.Dispose();
//                 return failoverMs;
//             }
//             catch (Exception ex)
//             {
//                 Console.WriteLine($"  ⚠  {ex.Message}");
//                 reporter.RecordFailover("Timeout", "none", (long)NewLeaderTimeout.TotalMilliseconds);
//                 subscription.Dispose();
//                 return NewLeaderTimeout.TotalMilliseconds; // counts as a failure in the summary
//             }
//         }
//         finally
//         {
//             await CleanupAsync(publisher1, publisher2, subscriberHost);
//         }
//     }
//
//     // ─── Host builders ───────────────────────────────────────────────────────
//
//     // NOTE: Replace AddStreamlyPublisher / AddStreamlyHandler below with the
//     // real DI registration shape once confirmed:
//     //   services.AddStreamly(config, opt => opt.AddHandler<FailoverRequest, FailoverPrice, FailoverPricingHandler>(StreamName));
//     private static IHost BuildPublisherHost(string instanceId)
//     {
//         var config = new ConfigurationBuilder()
//             .AddInMemoryCollection(new Dictionary<string, string?>
//             {
//                 ["Streamly:NatsUrl"]    = NatsUrl,
//                 ["Streamly:InstanceId"] = instanceId,
//             })
//             .Build();
//
//         return Host.CreateDefaultBuilder()
//             .ConfigureLogging(log => log
//                 .ClearProviders()
//                 .AddConsole()
//                 .AddFilter("Streamly", LogLevel.Information))
//             .ConfigureServices((_, services) =>
//             {
//                 services.AddStreamly(config, opt =>
//                     opt.AddHandler<FailoverRequest, FailoverPrice, FailoverPricingHandler>(StreamName));
//             })
//             .Build();
//     }
//
//     private static IHost BuildSubscriberHost()
//     {
//         var config = new ConfigurationBuilder()
//             .AddInMemoryCollection(new Dictionary<string, string?>
//             {
//                 ["Streamly:NatsUrl"]     = NatsUrl,
//                 ["Streamly:ServiceName"] = "FailoverSubscriber",
//             })
//             .Build();
//
//         return Host.CreateDefaultBuilder()
//             .ConfigureLogging(log => log.ClearProviders())
//             .ConfigureServices((_, services) =>
//             {
//                 services.AddStreamlySubscriber(config, opt =>
//                     opt.AddSubscriber<FailoverRequest, FailoverPrice>(StreamName));
//             })
//             .Build();
//     }
//
//     private static async Task CleanupAsync(params IHost[] hosts)
//     {
//         foreach (var host in hosts)
//         {
//             try   { await host.StopAsync(CancellationToken.None); }
//             catch { /* best effort — host may already be stopped */ }
//             finally { host.Dispose(); }
//         }
//     }
// }
//
// // ─── Models ──────────────────────────────────────────────────────────────────
// // PublisherId is embedded in the response so the subscriber can detect
// // which publisher instance sent each message without cross-process coordination.
//
// public record FailoverRequest(string CurrencyPair);
//
// public record FailoverPrice(
//     string   CurrencyPair,
//     decimal  Bid,
//     decimal  Ask,
//     string   PublisherId,
//     DateTime Timestamp);