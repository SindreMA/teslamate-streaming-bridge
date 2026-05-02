using TeslamateStreamingBridge.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<MessageTransformer>();
builder.Services.AddSingleton<WebSocketBroadcaster>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30),
});

app.MapGet("/", () => Results.Json(new { status = "ok" }));

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
