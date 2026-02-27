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
    private volatile bool _draining;

    public int ProcessedCount => _processedCount;
    public string Name => _consumerName;
    public bool IsDraining => _draining;
    public bool IsStopped { get; private set; }

    public StreamConsumer(IDatabase db, string streamKey, string groupName, string consumerName, int processingDelayMs)
    {
        _db = db;
        _streamKey = streamKey;
        _groupName = groupName;
        _consumerName = consumerName;
        _processingDelayMs = processingDelayMs;
    }

    /// <summary>
    /// Start drain: verwerk het huidige bericht af, maar pak geen nieuwe op.
    /// </summary>
    public void Drain()
    {
        _draining = true;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Als we aan het drainen zijn, stop met nieuwe berichten ophalen
            if (_draining)
            {
                IsStopped = true;
                Console.WriteLine($"  [{_consumerName}] Drain voltooid - gestopt.");
                return;
            }

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
                    var executionCode = entry.Values.FirstOrDefault(v => v.Name == "executionCode").Value;
                    var nodesRaw = entry.Values.FirstOrDefault(v => v.Name == "nodes").Value;
                    var nodes = nodesRaw.ToString().Split(',');

                    // Check of dit bericht een resumeFromNode markering heeft
                    var resumeFromNodeRaw = entry.Values.FirstOrDefault(v => v.Name == "resumeFromNode").Value;
                    int startNodeIndex = 0;
                    if (!resumeFromNodeRaw.IsNullOrEmpty)
                    {
                        startNodeIndex = int.Parse(resumeFromNodeRaw.ToString());
                        Console.WriteLine($"  [{_consumerName}] Hervat: {orderId} (exec: {executionCode}, vanaf node {startNodeIndex + 1}/{nodes.Length})");
                    }
                    else
                    {
                        Console.WriteLine($"  [{_consumerName}] Start: {orderId} (exec: {executionCode}, {nodes.Length} nodes)");
                    }

                    var delayPerNode = _processingDelayMs / nodes.Length;
                    bool drainedMidMessage = false;

                    // Verwerk elke node afzonderlijk, begin bij startNodeIndex
                    for (int nodeIdx = startNodeIndex; nodeIdx < nodes.Length; nodeIdx++)
                    {
                        if (ct.IsCancellationRequested) break;

                        Console.WriteLine($"    [{_consumerName}]   → Node {nodes[nodeIdx]} verwerken ({delayPerNode}ms)");
                        await Task.Delay(delayPerNode, ct);

                        // Check na elke node of we moeten drainen
                        if (_draining && nodeIdx < nodes.Length - 1)
                        {
                            // Nog niet alle nodes verwerkt - zet bericht terug op queue met markering
                            int resumeAt = nodeIdx + 1;
                            var remainingNodes = string.Join(",", nodes.Skip(resumeAt));
                            Console.WriteLine($"    [{_consumerName}]   ⚡ Drain ontvangen na node {nodes[nodeIdx]} - " +
                                              $"resterende nodes ({remainingNodes}) terug op queue (hervat bij {resumeAt + 1}/{nodes.Length})");

                            // Nieuw bericht op de queue met resumeFromNode markering
                            await _db.StreamAddAsync(_streamKey, new NameValueEntry[]
                            {
                                new("orderId", orderId.ToString()),
                                new("executionCode", executionCode.ToString()),
                                new("nodes", nodesRaw.ToString()),
                                new("resumeFromNode", resumeAt.ToString()),
                                new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
                            });

                            // Acknowledge het originele bericht (gedeeltelijk verwerkt)
                            await _db.StreamAcknowledgeAsync(_streamKey, _groupName, entry.Id);

                            drainedMidMessage = true;
                            IsStopped = true;
                            Console.WriteLine($"  [{_consumerName}] Drain voltooid - gestopt na gedeeltelijke verwerking van {orderId}.");
                            return;
                        }
                    }

                    if (!drainedMidMessage)
                    {
                        Console.WriteLine($"  [{_consumerName}] Klaar: {orderId}");
                        await _db.StreamAcknowledgeAsync(_streamKey, _groupName, entry.Id);
                        Interlocked.Increment(ref _processedCount);
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"  [{_consumerName}] Fout: {ex.Message}");
                await Task.Delay(1000, ct);
            }
        }

        IsStopped = true;
    }
}
