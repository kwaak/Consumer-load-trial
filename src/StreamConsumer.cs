using StackExchange.Redis;

namespace ConsumerLoadTrial;

/// <summary>
/// Een consumer die berichten leest uit een Redis Stream consumer group.
/// Meerdere instanties verdelen de load automatisch.
/// </summary>
public class StreamConsumer
{
    private readonly IDatabase _db;
    private readonly string _streamKey;
    private readonly string _groupName;
    private readonly string _consumerName;
    private readonly int _processingDelayMs;
    private int _processedCount;

    public int ProcessedCount => _processedCount;
    public string Name => _consumerName;

    public StreamConsumer(IDatabase db, string streamKey, string groupName, string consumerName, int processingDelayMs)
    {
        _db = db;
        _streamKey = streamKey;
        _groupName = groupName;
        _consumerName = consumerName;
        _processingDelayMs = processingDelayMs;
    }

    public async Task ConsumeAsync(CancellationToken ct)
    {
        Console.WriteLine($"[{_consumerName}] Gestart, wacht op berichten...");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var entries = await _db.StreamReadGroupAsync(
                    _streamKey,
                    _groupName,
                    _consumerName,
                    ">",       // Alleen nieuwe berichten
                    count: 1
                );

                if (entries.Length == 0)
                {
                    // Geen nieuwe berichten, kort wachten
                    await Task.Delay(100, ct);
                    continue;
                }

                foreach (var entry in entries)
                {
                    var orderId = entry.Values.FirstOrDefault(v => v.Name == "orderId").Value;
                    var product = entry.Values.FirstOrDefault(v => v.Name == "product").Value;
                    var quantity = entry.Values.FirstOrDefault(v => v.Name == "quantity").Value;

                    Console.WriteLine($"[{_consumerName}] Verwerkt: {orderId} | {product} x{quantity}");

                    // Simuleer verwerkingstijd
                    await Task.Delay(_processingDelayMs, ct);

                    // Bevestig dat het bericht verwerkt is
                    await _db.StreamAcknowledgeAsync(_streamKey, _groupName, entry.Id);
                    Interlocked.Increment(ref _processedCount);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_consumerName}] Fout: {ex.Message}");
                await Task.Delay(1000, ct);
            }
        }

        Console.WriteLine($"[{_consumerName}] Gestopt. Totaal verwerkt: {_processedCount}");
    }
}
