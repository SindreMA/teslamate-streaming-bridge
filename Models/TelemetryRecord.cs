using System.Text.Json.Serialization;

namespace TeslamateStreamingBridge.Models;

public sealed class TelemetryRecord
{
    [JsonPropertyName("vin")]
    public string Vin { get; set; } = "";

    [JsonPropertyName("createdAt")]
    public string CreatedAt { get; set; } = "";

    [JsonPropertyName("data")]
    public List<TelemetryDatum> Data { get; set; } = new();
}

public sealed class TelemetryDatum
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    [JsonPropertyName("value")]
    public TelemetryValue Value { get; set; } = new();
}

public sealed class TelemetryValue
{
    [JsonPropertyName("doubleValue")]
    public double? DoubleValue { get; set; }

    [JsonPropertyName("intValue")]
    public long? IntValue { get; set; }

    [JsonPropertyName("stringValue")]
    public string? StringValue { get; set; }

    [JsonPropertyName("shiftStateValue")]
    public string? ShiftStateValue { get; set; }

    [JsonPropertyName("locationValue")]
    public LocationValue? LocationValue { get; set; }
}

public sealed class LocationValue
{
    [JsonPropertyName("latitude")]
    public double Latitude { get; set; }

    [JsonPropertyName("longitude")]
    public double Longitude { get; set; }
}
