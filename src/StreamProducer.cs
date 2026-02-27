using StackExchange.Redis;

namespace ConsumerLoadTrial;

/// <summary>
/// Publiceert testberichten naar een Redis Stream.
/// </summary>
public class StreamProducer
{
    private readonly IDatabase _db;
    private readonly string _streamKey;

    public StreamProducer(IDatabase db, string streamKey)
    {
        _db = db;
        _streamKey = streamKey;
    }

    public async Task ProduceMessagesAsync(int count)
    {
        Console.WriteLine($"[Producer] Start met publiceren van {count} berichten naar '{_streamKey}'...");

        for (int i = 1; i <= count; i++)
        {
            var messageId = await _db.StreamAddAsync(_streamKey, new NameValueEntry[]
            {
                new("orderId", $"ORD-{i:D5}"),
                new("product", $"Product-{Random.Shared.Next(1, 20)}"),
                new("quantity", Random.Shared.Next(1, 100).ToString()),
                new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
            });

            Console.WriteLine($"[Producer] Bericht {i}/{count} gepubliceerd: {messageId}");
        }

        Console.WriteLine($"[Producer] Klaar - {count} berichten gepubliceerd.");
    }
}
