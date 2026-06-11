using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace TeslamateStreamingBridge.Services;

public sealed class KafkaConsumerService(
    IOptions<KafkaOptions> options,
    MessageTransformer transformer,
    WebSocketBroadcaster broadcaster,
    KafkaHealthState health,
    ILogger<KafkaConsumerService> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.Brokers,
            GroupId = _options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            SessionTimeoutMs = 30000,
            // Emit broker statistics so the liveness probe can tell whether we
            // are actually connected (librdkafka never throws on a dead broker).
            StatisticsIntervalMs = 5000,
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler((_, e) => logger.LogError("Kafka error: {Reason}", e.Reason))
            .SetStatisticsHandler((_, json) => UpdateHealthFromStatistics(json))
            .Build();

        try
        {
            consumer.Subscribe(_options.Topic);
            logger.LogInformation("Kafka consumer subscribed: brokers={Brokers} topic={Topic} group={Group}",
                _options.Brokers, _options.Topic, _options.GroupId);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string>? cr;
                try
                {
                    cr = await Task.Run(() => consumer.Consume(stoppingToken), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    logger.LogWarning(ex, "Consume error");
                    continue;
                }

                if (cr?.Message?.Value is not { Length: > 0 } payload)
                {
                    continue;
                }

                var transformed = transformer.Transform(payload);
                if (transformed is null)
                {
                    continue;
                }

                var frame = System.Text.Json.JsonSerializer.Serialize(new
                {
                    msg_type = "data:update",
                    tag = transformed.Vin,
                    value = transformed.Value,
                });
                await broadcaster.BroadcastAsync(transformed.Vin, frame, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Kafka consumer crashed");
            lifetime.StopApplication();
        }
        finally
        {
            try { consumer.Close(); } catch { }
        }
    }

    private bool _brokerWasUp;

    /// Parses the librdkafka statistics JSON and refreshes health state when a
    /// real broker (nodeid >= 0, i.e. not a bootstrap entry) is in the UP state.
    private void UpdateHealthFromStatistics(string json)
    {
        var anyBrokerUp = false;
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("brokers", out var brokers)
                && brokers.ValueKind == JsonValueKind.Object)
            {
                foreach (var broker in brokers.EnumerateObject())
                {
                    if (broker.Value.TryGetProperty("nodeid", out var nodeId)
                        && nodeId.GetInt32() >= 0
                        && broker.Value.TryGetProperty("state", out var state)
                        && state.GetString() == "UP")
                    {
                        anyBrokerUp = true;
                        break;
                    }
                }
            }
        }
        catch (JsonException ex)
        {
            logger.LogDebug(ex, "Failed to parse Kafka statistics");
            return;
        }

        if (anyBrokerUp)
        {
            health.MarkBrokerUp();
        }

        if (anyBrokerUp != _brokerWasUp)
        {
            _brokerWasUp = anyBrokerUp;
            if (anyBrokerUp)
            {
                logger.LogInformation("Kafka broker connection is UP");
            }
            else
            {
                logger.LogWarning("Kafka broker connection is DOWN (no broker in UP state)");
            }
        }
    }
}
