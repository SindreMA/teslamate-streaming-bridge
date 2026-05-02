using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace TeslamateStreamingBridge.Services;

/// Owns the per-VIN WebSocket map. Teslamate connects to /streaming/, sends a
/// `data:subscribe_oauth` (or `data:subscribe_all`) frame with `tag` = VIN, and
/// from then on we forward any matching telemetry update to that socket.
public sealed class WebSocketBroadcaster(ILogger<WebSocketBroadcaster> logger)
{
    private readonly ConcurrentDictionary<string, WebSocket> _tags = new();

    public async Task HandleConnectionAsync(WebSocket ws, CancellationToken ct)
    {
        var helloLoop = Task.Run(() => SendHelloLoopAsync(ws, ct), ct);
        var ownedTags = new HashSet<string>();
        var buffer = new byte[8 * 1024];

        try
        {
            while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var result = await ws.ReceiveAsync(buffer, ct);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                if (result.MessageType != WebSocketMessageType.Text || result.Count == 0)
                {
                    continue;
                }

                var text = Encoding.UTF8.GetString(buffer, 0, result.Count);
                await HandleClientFrameAsync(ws, text, ownedTags, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            logger.LogDebug(ex, "WebSocket closed unexpectedly");
        }
        finally
        {
            foreach (var tag in ownedTags)
            {
                _tags.TryRemove(new KeyValuePair<string, WebSocket>(tag, ws));
                logger.LogInformation("Close: {Tag}", tag);
            }
        }
    }

    public async Task BroadcastAsync(string vin, string payload, CancellationToken ct)
    {
        if (!_tags.TryGetValue(vin, out var ws) || ws.State != WebSocketState.Open)
        {
            return;
        }

        try
        {
            await ws.SendAsync(Encoding.UTF8.GetBytes(payload), WebSocketMessageType.Text, true, ct);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Broadcast failed for {Vin}", vin);
        }
    }

    private async Task HandleClientFrameAsync(WebSocket ws, string text, HashSet<string> ownedTags, CancellationToken ct)
    {
        ClientFrame? frame;
        try
        {
            frame = JsonSerializer.Deserialize<ClientFrame>(text);
        }
        catch (JsonException)
        {
            return;
        }

        if (frame is null || string.IsNullOrEmpty(frame.Tag))
        {
            return;
        }

        if (frame.MsgType is "data:subscribe_oauth" or "data:subscribe_all")
        {
            logger.LogInformation("Subscribe {MsgType} {Tag}", frame.MsgType, frame.Tag);
            _tags[frame.Tag] = ws;
            ownedTags.Add(frame.Tag);

            var helloMsg = frame.MsgType == "data:subscribe_all"
                ? $"control:hello:{frame.Tag}"
                : "control:hello";

            await SendJsonAsync(ws, new { msg_type = helloMsg, connection_timeout = 30000 }, ct);
        }
    }

    private static async Task SendHelloLoopAsync(WebSocket ws, CancellationToken ct)
    {
        var hello = JsonSerializer.SerializeToUtf8Bytes(new { msg_type = "control:hello", connection_timeout = 30000 });
        try
        {
            while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), ct);
                if (ws.State == WebSocketState.Open)
                {
                    await ws.SendAsync(hello, WebSocketMessageType.Text, true, ct);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    private static async Task SendJsonAsync(WebSocket ws, object payload, CancellationToken ct)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(payload);
        await ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
    }

    private sealed class ClientFrame
    {
        [System.Text.Json.Serialization.JsonPropertyName("msg_type")]
        public string? MsgType { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("tag")]
        public string? Tag { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("token")]
        public string? Token { get; set; }
    }
}
