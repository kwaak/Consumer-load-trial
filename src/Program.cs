using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using ConsumerLoadTrial;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json")
    .Build();

var redisConn = config["Redis:ConnectionString"] ?? "localhost:6379";
var tenantCount = int.Parse(config["Tenants:Count"] ?? "10");
var consumersPerTenant = int.Parse(config["Tenants:DefaultConsumersPerTenant"] ?? "2");
var messagesPerTenant = int.Parse(config["Tenants:MessagesPerTenant"] ?? "10");
var delayMs = int.Parse(config["Tenants:ProcessingDelayMs"] ?? "1000");

Console.WriteLine("=== Consumer Load Trial - Multi-Tenant ===");
Console.WriteLine($"Klanten:                {tenantCount}");
Console.WriteLine($"Consumers per klant:    {consumersPerTenant} (standaard)");
Console.WriteLine($"Berichten per klant:    {messagesPerTenant}");
Console.WriteLine($"Verwerkingstijd:        {delayMs}ms per bericht");
Console.WriteLine($"Totaal berichten:       {tenantCount * messagesPerTenant}");
Console.WriteLine("==========================================\n");

// === Verbinden ===
var redis = await ConnectionMultiplexer.ConnectAsync(redisConn);
var db = redis.GetDatabase();

// === Tenants aanmaken ===
var tenants = Enumerable.Range(1, tenantCount)
    .Select(i => new Tenant($"klant-{i:D2}", consumersPerTenant))
    .ToList();

// klant-01 heeft betaald voor extra capaciteit: 3 consumers i.p.v. 2
tenants[0].MaxConsumers = 3;
Console.WriteLine($"[Upgrade] {tenants[0].Id} opgeschaald naar {tenants[0].MaxConsumers} consumers\n");

var hosts = tenants.Select(t => new TenantHost(db, t, delayMs)).ToList();

// === Setup streams en produceer berichten ===
Console.WriteLine("[Setup] Streams aanmaken en berichten publiceren...");
foreach (var host in hosts)
{
    await host.SetupAsync();
    await host.ProduceAsync(messagesPerTenant);
}
Console.WriteLine($"[Setup] {tenantCount * messagesPerTenant} berichten gepubliceerd over {tenantCount} klanten.\n");

// === Start alle consumers ===
Console.WriteLine("[Start] Consumers starten...");
var ctsList = new List<CancellationTokenSource>();
var totalConsumers = 0;
foreach (var host in hosts)
{
    var cts = new CancellationTokenSource();
    ctsList.Add(cts);
    host.StartConsumers(cts.Token);
    totalConsumers += host.Tenant.MaxConsumers;
}
Console.WriteLine($"[Start] {totalConsumers} consumers actief.\n");

// === Wacht tot alles verwerkt is ===
await Task.WhenAll(hosts.Select(h => h.WaitForCompletionAsync(messagesPerTenant)));

// === Stop consumers ===
for (int i = 0; i < hosts.Count; i++)
    await hosts[i].StopAsync(ctsList[i]);

// === Resultaten per klant, gesorteerd op tijd ===
Console.WriteLine("\n=== RESULTATEN PER KLANT ===\n");

var maxBarWidth = 40;
var maxTime = hosts.Max(h => h.Elapsed.TotalSeconds);

Console.WriteLine($"{"Klant",-12} {"Consumers",9} {"Berichten",9} {"Tijd",7}  Tijdlijn");
Console.WriteLine(new string('-', 80));

foreach (var host in hosts.OrderBy(h => h.Elapsed.TotalSeconds))
{
    var t = host.Tenant;
    var secs = host.Elapsed.TotalSeconds;
    var barLen = (int)(secs / maxTime * maxBarWidth);
    var bar = new string('█', barLen);
    var upgraded = t.MaxConsumers > consumersPerTenant ? " ★" : "";

    Console.WriteLine($"{t.Id,-12} {t.MaxConsumers,9} {host.TotalProcessed,9} {secs,6:F1}s  {bar}{upgraded}");
}

Console.WriteLine(new string('-', 80));

// === Vergelijking standaard vs opgeschaald ===
var standardHosts = hosts.Where(h => h.Tenant.MaxConsumers == consumersPerTenant).ToList();
var upgradedHosts = hosts.Where(h => h.Tenant.MaxConsumers > consumersPerTenant).ToList();

if (standardHosts.Any() && upgradedHosts.Any())
{
    var avgStandard = standardHosts.Average(h => h.Elapsed.TotalSeconds);
    var avgUpgraded = upgradedHosts.Average(h => h.Elapsed.TotalSeconds);

    Console.WriteLine($"\n=== VERGELIJKING ===");
    Console.WriteLine($"Standaard ({consumersPerTenant} consumers):  gemiddeld {avgStandard:F1}s per klant");
    Console.WriteLine($"Opgeschaald ({upgradedHosts.First().Tenant.MaxConsumers} consumers): gemiddeld {avgUpgraded:F1}s per klant");
    Console.WriteLine($"Verschil:                  {avgStandard - avgUpgraded:F1}s sneller ({(1 - avgUpgraded / avgStandard) * 100:F0}% winst)");
}

// === Theoretische analyse ===
Console.WriteLine($"\n=== THEORETISCH ===");
Console.WriteLine($"1 bericht = {delayMs}ms verwerkingstijd");
Console.WriteLine($"Standaard: {messagesPerTenant} berichten / {consumersPerTenant} consumers = " +
                  $"{Math.Ceiling((double)messagesPerTenant / consumersPerTenant)} rondes × {delayMs / 1000.0:F0}s = " +
                  $"{Math.Ceiling((double)messagesPerTenant / consumersPerTenant) * delayMs / 1000.0:F0}s");
foreach (var host in upgradedHosts)
{
    var mc = host.Tenant.MaxConsumers;
    Console.WriteLine($"Opgeschaald: {messagesPerTenant} berichten / {mc} consumers = " +
                      $"{Math.Ceiling((double)messagesPerTenant / mc)} rondes × {delayMs / 1000.0:F0}s = " +
                      $"{Math.Ceiling((double)messagesPerTenant / mc) * delayMs / 1000.0:F0}s");
}

Console.WriteLine("\nKlaar!");
redis.Dispose();
