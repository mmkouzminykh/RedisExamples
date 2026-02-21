using StackExchange.Redis;
using System;

// Setup connection
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost:6379");
IDatabase db = redis.GetDatabase();
ISubscriber sub = redis.GetSubscriber();

const string queueKey = "task_queue";
const string lockKey = "resource_lock";
const string channelName = "notifications";

while (true)
{
    Console.WriteLine("\n=== SENDER APP ===");
    Console.WriteLine("1. Push to Queue (Simple Queue)");
    Console.WriteLine("2. Set Lock (Mutex)");
    Console.WriteLine("3. Publish Message (Pub/Sub)");
    Console.WriteLine("4. Exit");
    Console.Write("Select option: ");

    var input = Console.ReadLine();

    switch (input)
    {
        case "1":
            // 1. Simple Queue: LPUSH
            Console.Write("Enter message to queue: ");
            var msg = Console.ReadLine();
            db.ListLeftPush(queueKey, msg);
            Console.WriteLine($"[Queue] Message '{msg}' pushed to queue.");
            break;

        case "2":
            // 2. Mutex: SET lock_value NX EX (Set if Not Exists with Expiry)
            Console.WriteLine("[Lock] Attempting to set lock...");

            // We set a random value to identify ownership, expiry of 10 seconds
            string lockToken = Guid.NewGuid().ToString();

            // LockTake returns true if successful (key did not exist)
            bool acquired = db.LockTake(lockKey, lockToken, TimeSpan.FromSeconds(10));

            if (acquired)
            {
                Console.WriteLine("[Lock] Lock ACQUIRED. Holding for 15 seconds (simulating work)...");
                Console.WriteLine("Press any key to release lock manually (or wait for auto-expiry).");

                // Simulate holding the lock
                await Task.Delay(15000);

                // Release manually if key hasn't changed (safe release)
                // Note: In this demo, we let it expire or release manually below
                bool released = db.LockRelease(lockKey, lockToken);
                Console.WriteLine(released ? "[Lock] Lock released manually." : "[Lock] Lock was already expired or token mismatch.");
            }
            else
            {
                Console.WriteLine("[Lock] Failed to acquire lock. It is already held by another process.");
            }
            break;

        case "3":
            // 3. Pub/Sub: PUBLISH
            Console.Write("Enter message to publish: ");
            var pubMsg = Console.ReadLine();
            sub.Publish(channelName, pubMsg);
            Console.WriteLine($"[Pub/Sub] Message '{pubMsg}' published.");
            break;

        case "4":
            redis.Dispose();
            return;

        default:
            Console.WriteLine("Invalid option.");
            break;
    }
}