using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using TeslamateStreamingBridge.Models;

namespace TeslamateStreamingBridge.Services;

/// Ports the upstream MyTeslaMate transformMessage(): merges incoming
/// fleet-telemetry decoded records (only changed fields are sent) into a
/// per-VIN running snapshot, then formats it as the comma-separated value
/// string Teslamate's legacy Owner streaming protocol expects.
public sealed class MessageTransformer(ILogger<MessageTransformer> logger)
{
    private readonly ConcurrentDictionary<string, Dictionary<string, object?>> _lastValues = new();

    public TransformResult? Transform(string json)
    {
        TelemetryRecord? record;
        try
        {
            record = JsonSerializer.Deserialize<TelemetryRecord>(json);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Failed to parse telemetry record");
            return null;
        }

        if (record is null || string.IsNullOrEmpty(record.Vin))
        {
            return null;
        }

        var snapshot = _lastValues.GetOrAdd(record.Vin, _ => new Dictionary<string, object?>());
        lock (snapshot)
        {
            foreach (var item in record.Data)
            {
                if (item.Value.LocationValue is { } loc)
                {
                    snapshot["Latitude"] = loc.Latitude;
                    snapshot["Longitude"] = loc.Longitude;
                }
                else if (item.Value.ShiftStateValue is { } shift)
                {
                    snapshot[item.Key] = shift.Replace("ShiftState", string.Empty, StringComparison.Ordinal);
                }
                else if (item.Value.DoubleValue is { } d)
                {
                    snapshot[item.Key] = d;
                }
                else if (item.Value.IntValue is { } i)
                {
                    snapshot[item.Key] = i;
                }
                else if (item.Value.StringValue is { } s)
                {
                    snapshot[item.Key] = s;
                }
            }

            // Don't emit until we have at least a position and a gear (matches upstream gating).
            if (!snapshot.TryGetValue("Latitude", out var lat) || lat is null
                || !snapshot.TryGetValue("Longitude", out var lon) || lon is null
                || !snapshot.TryGetValue("Gear", out var gear) || gear is null
                || (gear is string gearStr && gearStr.Length == 0))
            {
                return null;
            }

            long power = 0;
            if (snapshot.TryGetValue("Power", out var p) && TryToInt(p, out var pv))
            {
                power = pv;
            }
            if (snapshot.TryGetValue("DCChargingPower", out var dc) && TryToInt(dc, out var dcv) && dcv > 0)
            {
                power = dcv;
            }
            if (snapshot.TryGetValue("ACChargingPower", out var ac) && TryToInt(ac, out var acv) && acv > 0)
            {
                power = acv;
            }

            string speed = TryToInt(snapshot.GetValueOrDefault("VehicleSpeed"), out var spd)
                ? spd.ToString(CultureInfo.InvariantCulture)
                : string.Empty;

            string soc = TryToInt(snapshot.GetValueOrDefault("Soc"), out var s2)
                ? s2.ToString(CultureInfo.InvariantCulture)
                : string.Empty;

            string heading = snapshot.GetValueOrDefault("GpsHeading")?.ToString() ?? string.Empty;
            string odometer = snapshot.GetValueOrDefault("Odometer")?.ToString() ?? string.Empty;
            string ratedRange = snapshot.GetValueOrDefault("RatedRange")?.ToString() ?? string.Empty;
            string estRange = snapshot.GetValueOrDefault("EstBatteryRange")?.ToString() ?? string.Empty;

            long timestamp = TryParseTimestamp(record.CreatedAt);

            string value = string.Join(',',
                timestamp.ToString(CultureInfo.InvariantCulture),
                speed,
                odometer,
                soc,
                "", // elevation
                heading,
                ((double)lat).ToString(CultureInfo.InvariantCulture),
                ((double)lon).ToString(CultureInfo.InvariantCulture),
                power.ToString(CultureInfo.InvariantCulture),
                gear?.ToString() ?? "",
                ratedRange,
                estRange,
                heading);

            return new TransformResult(record.Vin, value);
        }
    }

    private static bool TryToInt(object? value, out long result)
    {
        switch (value)
        {
            case long l:
                result = l;
                return true;
            case int i:
                result = i;
                return true;
            case double d:
                result = (long)d;
                return true;
            case string s when long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lv):
                result = lv;
                return true;
            case string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv):
                result = (long)dv;
                return true;
            default:
                result = 0;
                return false;
        }
    }

    private static long TryParseTimestamp(string createdAt)
    {
        if (DateTimeOffset.TryParse(createdAt, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto))
        {
            return dto.ToUnixTimeMilliseconds();
        }
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}

public sealed record TransformResult(string Vin, string Value);
