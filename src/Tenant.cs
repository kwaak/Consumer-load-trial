namespace ConsumerLoadTrial;

public class Tenant
{
    public string Id { get; }
    public string StreamKey => $"tenant:{Id}:orders";
    public string GroupName => $"tenant:{Id}:processors";
    public int MaxConsumers { get; set; }

    public Tenant(string id, int maxConsumers)
    {
        Id = id;
        MaxConsumers = maxConsumers;
    }
}
