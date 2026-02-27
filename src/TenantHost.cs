using StackExchange.Redis;

namespace ConsumerLoadTrial;

/// <summary>
/// Beheert de consumers voor één tenant.
/// Ondersteunt drain stop (graceful stop) en drain start (hervatten).
/// </summary>
public class TenantHost
{
    private readonly IDatabase _db;
    private readonly Tenant _tenant;
    private readonly int _processingDelayMs;
    private readonly int _nodesPerMessage;
    private readonly List<StreamConsumer> _consumers = new();
    private readonly List<Task> _tasks = new();
    private CancellationTokenSource? _cts;

    private System.Diagnostics.Stopwatch? _stopwatch;
    private int _nextWorkerId = 1;

    public Tenant Tenant => _tenant;
    public int TotalProcessed => _consumers.Sum(c => c.ProcessedCount);
    public TimeSpan Elapsed => _stopwatch?.Elapsed ?? TimeSpan.Zero;
    public int ActiveConsumerCount => _consumers.Count(c => !c.IsStopped);
    public IReadOnlyList<StreamConsumer> Consumers => _consumers;

    public TenantHost(IDatabase db, Tenant tenant, int processingDelayMs, int nodesPerMessage = 5)
    {
        _db = db;
        _tenant = tenant;
        _processingDelayMs = processingDelayMs;
        _nodesPerMessage = nodesPerMessage;
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
            var executionCode = $"EXEC-{Guid.NewGuid().ToString("N")[..8].ToUpper()}";
            var nodes = string.Join(",", Enumerable.Range(1, _nodesPerMessage).Select(n => $"N{n}"));

            await _db.StreamAddAsync(_tenant.StreamKey, new NameValueEntry[]
            {
                new("orderId", $"{_tenant.Id}-ORD-{i:D4}"),
                new("executionCode", executionCode),
                new("nodes", nodes),
                new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
            });
        }
    }

    public void StartConsumers(CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (_stopwatch == null)
            _stopwatch = System.Diagnostics.Stopwatch.StartNew();
        else if (!_stopwatch.IsRunning)
            _stopwatch.Start();

        for (int i = 0; i < _tenant.MaxConsumers; i++)
        {
            SpawnConsumer(_cts.Token);
        }
    }

    private StreamConsumer SpawnConsumer(CancellationToken ct)
    {
        var consumer = new StreamConsumer(
            _db, _tenant.StreamKey, _tenant.GroupName,
            $"{_tenant.Id}-worker-{_nextWorkerId++}", _processingDelayMs);
        _consumers.Add(consumer);
        _tasks.Add(consumer.RunAsync(ct));
        return consumer;
    }

    /// <summary>
    /// Drain stop: alle actieve consumers draaien hun huidig bericht af en stoppen.
    /// Nieuwe berichten worden niet meer opgepakt.
    /// </summary>
    public async Task DrainStopAsync()
    {
        Console.WriteLine($"  [{_tenant.Id}] DRAIN STOP gestart - consumers draaien huidige berichten af...");
        var activeConsumers = _consumers.Where(c => !c.IsStopped).ToList();

        foreach (var consumer in activeConsumers)
            consumer.Drain();

        // Wacht tot alle consumers klaar zijn met hun huidige bericht
        var timeout = TimeSpan.FromSeconds(30);
        var start = DateTime.UtcNow;
        while (activeConsumers.Any(c => !c.IsStopped) && DateTime.UtcNow - start < timeout)
            await Task.Delay(100);

        Console.WriteLine($"  [{_tenant.Id}] DRAIN STOP voltooid - {activeConsumers.Count} consumers gestopt.");
    }

    /// <summary>
    /// Drain start: start nieuwe consumers om het werk te hervatten.
    /// </summary>
    public void DrainStart(CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var activeCount = _consumers.Count(c => !c.IsStopped);
        var needed = _tenant.MaxConsumers - activeCount;

        Console.WriteLine($"  [{_tenant.Id}] DRAIN START - {needed} nieuwe consumers starten...");

        for (int i = 0; i < needed; i++)
        {
            SpawnConsumer(_cts.Token);
        }

        Console.WriteLine($"  [{_tenant.Id}] DRAIN START voltooid - {_tenant.MaxConsumers} consumers actief.");
    }

    public async Task WaitForCompletionAsync(int expectedCount)
    {
        while (TotalProcessed < expectedCount)
            await Task.Delay(100);
        _stopwatch?.Stop();
    }

    public async Task StopAsync()
    {
        _cts?.Cancel();
        await Task.WhenAll(_tasks);
    }
}
