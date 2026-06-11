using TeslamateStreamingBridge.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<MessageTransformer>();
builder.Services.AddSingleton<WebSocketBroadcaster>();
builder.Services.AddSingleton<KafkaHealthState>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30),
});

app.MapGet("/", () => Results.Json(new { status = "ok" }));

// Readiness: the HTTP server (TeslaMate's WebSocket endpoint) is up. Intentionally
// independent of Kafka so a broker outage never deregisters us from the Service
// and drops TeslaMate's streaming socket.
app.MapGet("/health/ready", () => Results.Json(new { status = "ready" }));

// Liveness: fails when the Kafka consumer has had no live broker connection for
// MaxKafkaDowntime, so Kubernetes restarts the pod and librdkafka reconnects
// cleanly instead of retrying a dead connection forever.
var maxKafkaDowntime = TimeSpan.FromSeconds(90);
app.MapGet("/health/live", (KafkaHealthState health) =>
    health.IsHealthy(maxKafkaDowntime)
        ? Results.Json(new { status = "healthy", lastBrokerUpUtc = health.LastBrokerUpUtc })
        : Results.Json(
            new { status = "unhealthy", lastBrokerUpUtc = health.LastBrokerUpUtc },
            statusCode: StatusCodes.Status503ServiceUnavailable));

app.Map("/streaming/", async (HttpContext ctx, WebSocketBroadcaster broadcaster) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    await broadcaster.HandleConnectionAsync(ws, ctx.RequestAborted);
});

app.Run();
