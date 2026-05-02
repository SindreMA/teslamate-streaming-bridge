namespace TeslamateStreamingBridge.Services;

public sealed class KafkaOptions
{
    public string Brokers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "tesla_telemetry_V";
    public string GroupId { get; set; } = "teslamate-bridge";
}
