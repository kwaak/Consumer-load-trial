using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using ConsumerLoadTrial;

// === Configuratie laden ===
var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json")
    .Build();

var redisConnectionString = config["Redis:ConnectionString"] ?? "localhost:6379";
var streamKey = config["Redis:StreamKey"] ?? "orders-stream";
var groupName = config["Redis:ConsumerGroup"] ?? "order-processors";
var messageCount = int.Parse(config["Redis:MessageCount"] ?? "50");
var consumerCount = int.Parse(config["Redis:ConsumerCount"] ?? "3");
var processingDelayMs = int.Parse(config["Redis:ProcessingDelayMs"] ?? "200");

Console.WriteLine("=== Consumer Load Trial - Redis Streams ===");
Console.WriteLine($"Redis:          {redisConnectionString}");
Console.WriteLine($"Stream:         {streamKey}");
Console.WriteLine($"Consumer Group: {groupName}");
Console.WriteLine($"Berichten:      {messageCount}");
Console.WriteLine($"Consumers:      {consumerCount}");
Console.WriteLine($"Vertraging:     {processingDelayMs}ms per bericht");
Console.WriteLine("============================================\n");

// === Verbinden met Redis ===
Console.WriteLine("Verbinden met Redis...");
var redis = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);
var db = redis.GetDatabase();
Console.WriteLine("Verbonden!\n");

// === Stream opschonen en consumer group aanmaken ===
// Verwijder oude stream als die bestaat (schone test)
await db.KeyDeleteAsync(streamKey);

// Maak de consumer group aan (MKSTREAM maakt de stream automatisch)
await db.StreamCreateConsumerGroupAsync(streamKey, groupName, "0-0", createStream: true);
Console.WriteLine($"Consumer group '{groupName}' aangemaakt op stream '{streamKey}'.\n");

// === Produceer berichten ===
var producer = new StreamProducer(db, streamKey);
await producer.ProduceMessagesAsync(messageCount);
Console.WriteLine();

// === Start consumers ===
using var cts = new CancellationTokenSource();
var consumers = new List<StreamConsumer>();
var consumerTasks = new List<Task>();

for (int i = 1; i <= consumerCount; i++)
{
    var consumer = new StreamConsumer(db, streamKey, groupName, $"consumer-{i}", processingDelayMs);
    consumers.Add(consumer);
    consumerTasks.Add(consumer.ConsumeAsync(cts.Token));
}

// === Wacht tot alle berichten verwerkt zijn ===
Console.WriteLine($"\nWachten tot alle {messageCount} berichten verwerkt zijn...\n");

while (consumers.Sum(c => c.ProcessedCount) < messageCount)
{
    await Task.Delay(500);
}

// Stop alle consumers
cts.Cancel();
await Task.WhenAll(consumerTasks);

// === Resultaten tonen ===
Console.WriteLine("\n=== RESULTATEN ===");
Console.WriteLine($"{"Consumer",-15} {"Verwerkt",10} {"Percentage",12}");
Console.WriteLine(new string('-', 37));

foreach (var consumer in consumers)
{
    var pct = (double)consumer.ProcessedCount / messageCount * 100;
    Console.WriteLine($"{consumer.Name,-15} {consumer.ProcessedCount,10} {pct,11:F1}%");
}

Console.WriteLine(new string('-', 37));
Console.WriteLine($"{"Totaal",-15} {consumers.Sum(c => c.ProcessedCount),10} {"100.0%",11}");
Console.WriteLine();

// === Gelijkmatigheid berekenen ===
var counts = consumers.Select(c => (double)c.ProcessedCount).ToList();
var avg = counts.Average();
var stddev = Math.Sqrt(counts.Sum(c => Math.Pow(c - avg, 2)) / counts.Count);
var cv = avg > 0 ? stddev / avg * 100 : 0;

Console.WriteLine($"Gemiddeld per consumer: {avg:F1}");
Console.WriteLine($"Standaardafwijking:     {stddev:F1}");
Console.WriteLine($"Variatiecoëfficiënt:    {cv:F1}% (lager = gelijkmatiger)");

if (cv < 10)
    Console.WriteLine("-> Uitstekende load verdeling!");
else if (cv < 25)
    Console.WriteLine("-> Redelijke load verdeling.");
else
    Console.WriteLine("-> Ongelijke load verdeling - overweeg tuning.");

// === Stream info tonen ===
var streamInfo = await db.StreamInfoAsync(streamKey);
var groupInfo = await db.StreamGroupInfoAsync(streamKey);

Console.WriteLine($"\n=== STREAM INFO ===");
Console.WriteLine($"Stream lengte:    {streamInfo.Length}");
Console.WriteLine($"Consumer groups:  {groupInfo.Length}");

foreach (var g in groupInfo)
{
    Console.WriteLine($"  Group '{g.Name}': {g.ConsumerCount} consumers, {g.PendingMessageCount} pending");
}

Console.WriteLine("\nKlaar!");
redis.Dispose();
