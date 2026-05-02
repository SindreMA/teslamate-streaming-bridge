using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace TeslamateStreamingBridge.Services;

public sealed class KafkaConsumerService(
    IOptions<KafkaOptions> options,
    MessageTransformer transformer,
    WebSocketBroadcaster broadcaster,
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
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler((_, e) => logger.LogError("Kafka error: {Reason}", e.Reason))
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
}
