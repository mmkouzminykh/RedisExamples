using StackExchange.Redis;
using System;
using System.Threading;

// Setup connection
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost:6379");
IDatabase db = redis.GetDatabase();
ISubscriber sub = redis.GetSubscriber();

const string queueKey = "task_queue";
const string lockKey = "resource_lock";
const string channelName = "notifications";

// We need a cancellation token source to stop listening loops gracefully
using var cts = new CancellationTokenSource();

// Background thread to handle 'Q' press for stopping listeners
_ = Task.Run(() =>
{
    while (true)
    {
        if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Q)
        {
            cts.Cancel();
            break;
        }
        Thread.Sleep(100);
    }
});

while (!cts.Token.IsCancellationRequested)
{
    Console.WriteLine("\n=== RECEIVER APP ===");
    Console.WriteLine("1. Read from Queue (Simple Queue)");
    Console.WriteLine("2. Wait for Lock (Mutex)");
    Console.WriteLine("3. Subscribe to Channel (Pub/Sub)");
    Console.WriteLine("4. Exit");
    Console.Write("Select option: ");

    var input = Console.ReadLine();

    switch (input)
    {
        case "1":
            // 1. Simple Queue: RPOP (Blocking Pop)
            Console.WriteLine("[Queue] Waiting for items... (Press 'Q' in menu to stop)");

            while (!cts.Token.IsCancellationRequested)
            {
                // We use ListRightPop. In StackExchange, standard pop is non-blocking.
                // For a blocking behavior (BRPOP), we check if null and loop/wait, 
                // or use the specific blocking methods if available in newer versions.
                // Simplest robust way: Polling with small delay.

                var item = db.ListRightPop(queueKey);

                if (item.HasValue)
                {
                    Console.WriteLine($"[Queue] Received: {item}");
                }
                else
                {
                    // Prevent CPU spin
                    Thread.Sleep(500);
                }
            }
            Console.WriteLine("[Queue] Stopped.");
            break;

        case "2":
            // 2. Mutex: Wait for Lock
            Console.WriteLine("[Lock] Waiting to acquire lock...");

            string lockToken = Guid.NewGuid().ToString();
            bool acquired = false;

            // Spin until lock is acquired
            while (!acquired && !cts.Token.IsCancellationRequested)
            {
                // Try to take lock with 5s expiry
                acquired = db.LockTake(lockKey, lockToken, TimeSpan.FromSeconds(5));

                if (!acquired)
                {
                    Console.Write("."); // Visual progress
                    Thread.Sleep(1000); // Wait 1 sec before retry
                }
            }

            if (acquired)
            {
                Console.WriteLine("\n[Lock] Lock ACQUIRED! Doing work for 3 seconds...");
                Thread.Sleep(3000);
                db.LockRelease(lockKey, lockToken);
                Console.WriteLine("[Lock] Lock Released.");
            }
            break;

        case "3":
            // 3. Pub/Sub: SUBSCRIBE
            Console.WriteLine("[Pub/Sub] Subscribed. Waiting for messages... (Press 'Q' to stop)");

            // Register handler
            await sub.SubscribeAsync(channelName, (channel, message) => {
                Console.WriteLine($"[Pub/Sub] Received: {message}");
            });

            // Wait for cancellation token (Q press)
            try
            {
                await Task.Delay(Timeout.Infinite, cts.Token);
            }
            catch (TaskCanceledException)
            {
                // Expected
            }

            await sub.UnsubscribeAsync(channelName);
            Console.WriteLine("[Pub/Sub] Unsubscribed.");
            break;

        case "4":
            cts.Cancel();
            redis.Dispose();
            return;

        default:
            Console.WriteLine("Invalid option.");
            break;
    }
}