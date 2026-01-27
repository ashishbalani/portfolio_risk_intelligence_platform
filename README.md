# Portfolio Risk Optimization Platform

## processing-service-streams

### Local run with Docker Compose (Kafka)

1) Start Kafka (and Postgres/Prometheus/Grafana if you want):

```bash
# from repo root

docker compose -f infra/docker-compose.yml up -d
```

2) Run the Streams app:

```bash
./gradlew :processing-service-streams:bootRun
```

3) Optional environment overrides:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_STREAMS_APP_ID=processing-service-streams
export STREAMS_STATE_DIR=./build/kstreams
```

The service exposes health and metrics at:
- `GET /actuator/health`
- `GET /actuator/prometheus`

### Streams configuration

Key settings (env overrides shown in `application.yml`):
- `streams.bootstrap-servers`
- `streams.application-id`
- `streams.base-currency`
- `streams.exactly-once` (uses `exactly_once_v2`)
- `streams.num-stream-threads`
- `streams.state-dir`
- `streams.cache-max-bytes-buffering`
- `streams.commit-interval-ms`
- `streams.replace-thread-on-exception`
- `streams.suppress-outputs`

Repartition topics are explicitly created by `through(...)` in the topology:
- `trades-by-instrument`
- `trades-by-fx`
- `trades-by-position`
- `exposures-by-portfolio`
