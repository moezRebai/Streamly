// FILE: Streamly.LoadTests/Program.cs
//
// Launch order:
//   1. nats-server -js
//   2. Streamly.Test.Publisher  (dotnet run or from Rider)
//   3. dotnet run               (this project)

using Streamly.LoadTests;
using Streamly.LoadTests.Scenarios;

Console.WriteLine("Streamly Load Tests");
Console.WriteLine("===================");
Console.WriteLine("Requires: NATS server on nats://localhost:4222");
Console.WriteLine("Requires: Streamly.Test.Publisher running");
Console.WriteLine();
Console.WriteLine("  1 - Failover   (leader kill → new leader first price, NFR < 1000ms)");
Console.WriteLine("  2 - SmokeTest  (5k / 10k / 20k streams, 3 min sustained)");
Console.WriteLine();
Console.Write("Choice [1-2]: ");

var choice = Console.ReadLine()?.Trim();

switch (choice)
{
    case "1":
        //var failover = new FailoverScenario();
       // await failover.RunAsync(iterations: 5);
        break;

    case "2":
    default:
        var smoke = new SmokeTestScenario();
        await smoke.RunAsync(loadLevels: [5_000, 10_000, 20_000]);
        break;
}