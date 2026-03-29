using Microsoft.AspNetCore.ResponseCompression;
using MudBlazor.Services;
using Polly;
using Polly.Extensions.Http;
using Streamly.Audit;
using Streamly.Dashboard.Aggregation;
using Streamly.Dashboard.Components;
using Streamly.Dashboard.Hubs;

var builder = WebApplication.CreateBuilder(args);

// ── MudBlazor ─────────────────────────────────────────────────────────────
builder.Services.AddMudServices();

// ── Blazor (interactive server) ────────────────────────────────────────────
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// ── SignalR ────────────────────────────────────────────────────────────────
builder.Services.AddSignalR();

builder.Services.AddResponseCompression(opts =>
{
    opts.MimeTypes = ResponseCompressionDefaults.MimeTypes.Concat(
        ["application/octet-stream"]);
});

// ── Dashboard configuration ────────────────────────────────────────────────
builder.Services.Configure<DashboardOptions>(
    builder.Configuration.GetSection(DashboardOptions.SectionKey));

// ── Aggregation singletons ─────────────────────────────────────────────────
builder.Services.AddSingleton<InstanceRegistry>();
builder.Services.AddHostedService<MetricsAggregator>();

// ── Audit — direct JetStream access (bypasses publisher HTTP endpoint) ─────
builder.Services.AddSingleton<IJetStreamConnectionAccessor, DashboardNatsConnection>();
builder.Services.AddStreamlyAuditReader(options =>
    builder.Configuration.GetSection(AuditOptions.SectionName).Bind(options));

// ── Named HttpClients — one per configured instance ────────────────────────
var dashboardOptions =
    builder.Configuration
           .GetSection(DashboardOptions.SectionKey)
           .Get<DashboardOptions>() ?? new DashboardOptions();

var retryPolicy = HttpPolicyExtensions
    .HandleTransientHttpError()
    .WaitAndRetryAsync(
        retryCount: 2,
        sleepDurationProvider: _ => TimeSpan.FromMilliseconds(500));

foreach (var instance in dashboardOptions.Instances)
{
    builder.Services
        .AddHttpClient(MetricsAggregator.HttpClientName(instance.Id), client =>
        {
            client.BaseAddress = new Uri(instance.BaseUrl.TrimEnd('/') + "/");
            client.Timeout     = TimeSpan.FromSeconds(3);
            client.DefaultRequestHeaders.Add("Accept", "application/json");
        })
        .AddPolicyHandler(retryPolicy);
}

// ── Build ──────────────────────────────────────────────────────────────────
var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseResponseCompression();
app.UseAntiforgery();

app.MapHub<MetricsHub>("/hubs/metrics");

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();