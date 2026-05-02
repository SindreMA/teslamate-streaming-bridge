# teslamate-streaming-bridge

Kafka consumer that translates Tesla Fleet Telemetry decoded records into the
legacy Owner streaming WSS protocol Teslamate already speaks. Replaces the
polling-based `TESLA_API_HOST` integration so the cluster stops hitting Tesla
Fleet API rate limits.

ASP.NET Core 10 minimal API + Confluent.Kafka + WebSockets.

## Pipeline

```
Tesla Model Y → fleet-telemetry → Kafka topic tesla_telemetry_V
                                              │
                                              ▼
                              this bridge (Kafka consumer)
                                              │
                                              ▼
                              Teslamate (TESLA_WSS_HOST=…:8081)
```

## Configuration

`Kafka__*` env vars (ASP.NET Core configuration convention):

| Env var | Default | Notes |
|---|---|---|
| `Kafka__Brokers` | `localhost:9092` | Comma-separated bootstrap servers. |
| `Kafka__Topic` | `tesla_telemetry_V` | Must match `<fleet-telemetry namespace>_V`. |
| `Kafka__GroupId` | `teslamate-bridge` | |
| `ASPNETCORE_URLS` | `http://+:8081` | Where Teslamate connects (`/streaming/`). |

`fleet-telemetry` must run with `transmit_decoded_records: true` so the Kafka
payloads are JSON (the format `MessageTransformer` expects), not protobuf.

## Endpoints

- `GET /` — health check
- `WS /streaming/` — what Teslamate connects to. Subscribe with
  `data:subscribe_oauth` or `data:subscribe_all`, `tag` = VIN.

## Build locally

```
dotnet run
docker build -t teslamate-streaming-bridge .
```

## CI

`.github/workflows/ci.yml` builds the image and pushes to Harbor at
`registry.k8s.sindrema.com/images/teslamate-streaming-bridge:<timestamp>`,
then commits an updated image tag in `SindreMA/sindre-k8s` so Flux rolls the
deployment. Same shape as `Matcros_API`.

Required GitHub secrets: `HARBOR_USERNAME`, `HARBOR_PASSWORD`, `GH_PAT`.

## Lineage

Logic ported from
[MyTeslaMate/websocket](https://github.com/MyTeslaMate/websocket). The Pub/Sub
HTTP receiver was dropped (Kafka consumer instead) and the
`api.myteslamate.com` validation in the `data:subscribe_all` path was removed.
