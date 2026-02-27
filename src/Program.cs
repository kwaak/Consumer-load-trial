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
var delayMs = int.Parse(config["Tenants:ProcessingDelayMs"] ?? "500");

Console.WriteLine("=== Consumer Load Trial - Multi-Tenant ===");
Console.WriteLine($"Klanten:                {tenantCount}");
Console.WriteLine($"Consumers per klant:    {consumersPerTenant} (= max {consumersPerTenant} tegelijk)");
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

// Voorbeeld: klant-01 heeft betaald voor extra capaciteit (4 consumers)
// Uncomment de volgende regel om opschalen te testen:
// tenants[0].MaxConsumers = 4;

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
foreach (var host in hosts)
{
    var cts = new CancellationTokenSource();
    ctsList.Add(cts);
    host.StartConsumers(cts.Token);
}
Console.WriteLine($"[Start] {tenantCount * consumersPerTenant} consumers actief.\n");

// === Wacht tot alles verwerkt is ===
var sw = System.Diagnostics.Stopwatch.StartNew();
await Task.WhenAll(hosts.Select(h => h.WaitForCompletionAsync(messagesPerTenant)));
sw.Stop();

// === Stop consumers ===
for (int i = 0; i < hosts.Count; i++)
    await hosts[i].StopAsync(ctsList[i]);

// === Resultaten ===
Console.WriteLine("\n=== RESULTATEN ===");
Console.WriteLine($"Totale verwerkingstijd: {sw.Elapsed.TotalSeconds:F1}s\n");

foreach (var host in hosts)
    host.PrintStats(messagesPerTenant);

// === Theoretische tijden ===
Console.WriteLine("\n=== ANALYSE ===");
var theoreticalPerTenant = Math.Ceiling((double)messagesPerTenant / consumersPerTenant) * delayMs / 1000.0;
Console.WriteLine($"Theoretisch per klant ({messagesPerTenant} berichten / {consumersPerTenant} consumers): {theoreticalPerTenant:F1}s");
Console.WriteLine($"Werkelijke tijd: {sw.Elapsed.TotalSeconds:F1}s");
Console.WriteLine($"Alle klanten draaien parallel, dus totale tijd ≈ tijd per klant.");

Console.WriteLine("\n=== OPSCHALEN ===");
Console.WriteLine($"Huidige capaciteit per klant: {consumersPerTenant} berichten tegelijk");
Console.WriteLine("Wil een klant sneller? Verhoog MaxConsumers in de config of per tenant.");
Console.WriteLine("Voorbeeld: klant met 4 consumers verwerkt 2x zo snel als met 2.");

Console.WriteLine("\nKlaar!");
redis.Dispose();
