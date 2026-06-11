using System.Threading;

namespace TeslamateStreamingBridge.Services;

/// Tracks whether the Kafka consumer currently has a live connection to a real
/// broker, so an HTTP liveness probe can restart the pod if connectivity is
/// lost for too long. librdkafka silently retries forever on a dead broker
/// (e.g. after a broker restart combined with a DNS blip) without throwing, so
/// the process can sit "Running" yet permanently disconnected. The statistics
/// callback feeds MarkBrokerUp() roughly every StatisticsIntervalMs; if that
/// stops happening, the consumer is no longer talking to Kafka.
public sealed class KafkaHealthState
{
    private long _lastBrokerUpTicks = DateTime.UtcNow.Ticks;

    /// Called whenever a real (nodeid >= 0) broker is observed in the UP state.
    public void MarkBrokerUp() => Interlocked.Exchange(ref _lastBrokerUpTicks, DateTime.UtcNow.Ticks);

    public DateTime LastBrokerUpUtc => new(Interlocked.Read(ref _lastBrokerUpTicks), DateTimeKind.Utc);

    /// Healthy if a broker has been seen UP within maxDowntime. Initialised to
    /// construction time so the probe tolerates the initial connect window.
    public bool IsHealthy(TimeSpan maxDowntime) => DateTime.UtcNow - LastBrokerUpUtc <= maxDowntime;
}
