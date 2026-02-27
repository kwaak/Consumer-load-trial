using StackExchange.Redis;

namespace ConsumerLoadTrial;

/// <summary>
/// Beheert de consumers voor één tenant.
/// MaxConsumers bepaalt hoeveel berichten tegelijk verwerkt worden.
/// Opschalen = MaxConsumers verhogen.
/// </summary>
public class TenantHost
{
    private readonly IDatabase _db;
    private readonly Tenant _tenant;
    private readonly int _processingDelayMs;
    private readonly List<StreamConsumer> _consumers = new();
    private readonly List<Task> _tasks = new();

    public Tenant Tenant => _tenant;
    public int TotalProcessed => _consumers.Sum(c => c.ProcessedCount);

    public TenantHost(IDatabase db, Tenant tenant, int processingDelayMs)
    {
        _db = db;
        _tenant = tenant;
        _processingDelayMs = processingDelayMs;
    }

    public async Task SetupAsync()
    {
        // Schone stream
        await _db.KeyDeleteAsync(_tenant.StreamKey);
        await _db.StreamCreateConsumerGroupAsync(
            _tenant.StreamKey, _tenant.GroupName, "0-0", createStream: true);
    }

    public async Task ProduceAsync(int messageCount)
    {
        for (int i = 1; i <= messageCount; i++)
        {
            await _db.StreamAddAsync(_tenant.StreamKey, new NameValueEntry[]
            {
                new("orderId", $"{_tenant.Id}-ORD-{i:D4}"),
                new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
            });
        }
    }

    public void StartConsumers(CancellationToken ct)
    {
        for (int i = 1; i <= _tenant.MaxConsumers; i++)
        {
            var consumer = new StreamConsumer(
                _db, _tenant.StreamKey, _tenant.GroupName,
                $"{_tenant.Id}-worker-{i}", _processingDelayMs);
            _consumers.Add(consumer);
            _tasks.Add(consumer.RunAsync(ct));
        }
    }

    public async Task WaitForCompletionAsync(int expectedCount)
    {
        while (TotalProcessed < expectedCount)
            await Task.Delay(100);
    }

    public async Task StopAsync(CancellationTokenSource cts)
    {
        cts.Cancel();
        await Task.WhenAll(_tasks);
    }

    public void PrintStats(int expectedCount)
    {
        Console.WriteLine($"  Klant {_tenant.Id}: {TotalProcessed}/{expectedCount} verwerkt " +
                          $"({_tenant.MaxConsumers} consumers, max {_tenant.MaxConsumers} tegelijk)");
        foreach (var c in _consumers)
            Console.WriteLine($"    {c.Name}: {c.ProcessedCount} berichten");
    }
}
