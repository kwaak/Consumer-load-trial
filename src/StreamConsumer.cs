using StackExchange.Redis;

namespace ConsumerLoadTrial;

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

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var entries = await _db.StreamReadGroupAsync(
                    _streamKey, _groupName, _consumerName,
                    ">", count: 1);

                if (entries.Length == 0)
                {
                    await Task.Delay(50, ct);
                    continue;
                }

                foreach (var entry in entries)
                {
                    var orderId = entry.Values.FirstOrDefault(v => v.Name == "orderId").Value;
                    Console.WriteLine($"  [{_consumerName}] Verwerkt: {orderId}");

                    // Simuleer verwerkingstijd
                    await Task.Delay(_processingDelayMs, ct);

                    await _db.StreamAcknowledgeAsync(_streamKey, _groupName, entry.Id);
                    Interlocked.Increment(ref _processedCount);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"  [{_consumerName}] Fout: {ex.Message}");
                await Task.Delay(1000, ct);
            }
        }
    }
}
