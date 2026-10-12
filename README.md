# Real-Time Crypto Pipeline: Kafka + Airflow + Postgres

A small end-to-end data engineering project that streams crypto prices through Kafka, stores them in Postgres, and aggregates them into hourly OHLC candles with an Airflow DAG. Everything runs locally with Docker Compose.

## Architecture

```
Binance API -> producer -> [Kafka topic: crypto.prices] -> consumer -> Postgres: raw_prices
                                                                              |
                                              Airflow DAG (@hourly) <---------+--> hourly_ohlc
```

| Component | Role |
|---|---|
| `producer` | Polls the public Binance API every 5 seconds (BTC, ETH, SOL) and publishes events to Kafka. The symbol is used as the message key, so each symbol stays ordered inside one partition. |
| `kafka` | Single-broker KRaft cluster. The topic `crypto.prices` has 3 partitions. |
| `consumer` | Reads from Kafka and writes to `raw_prices`. Offsets are committed manually, only after the database write succeeds (at-least-once delivery). |
| `postgres` | Stores raw events and the aggregated hourly candles. |
| `airflow` | Runs the hourly DAG `crypto_hourly_ohlc`. |

## Design decisions

- **Idempotent writes.** `raw_prices` has `UNIQUE (kafka_partition, kafka_offset)` and the consumer uses `ON CONFLICT DO NOTHING`, so reprocessing a batch never creates duplicates.
- **Manual offset commits.** A crash between the write and the commit means a batch is read again, never lost.
- **Idempotent aggregation.** The DAG upserts candles (`ON CONFLICT ... DO UPDATE`), so re-running a task for the same interval gives the same result.
- **Data quality checks.** The DAG fails loudly if no data arrived in the interval, and validates each candle (`low <= open, close <= high`).

## Airflow DAG

```
check_raw_data  >>  aggregate_hourly  >>  quality_check
```

1. `check_raw_data` fails if no events landed in the interval.
2. `aggregate_hourly` builds one open/high/low/close candle per symbol per hour.
3. `quality_check` verifies that the candles are consistent.

The DAG uses `retries=2` with a 1-minute delay and `catchup=False`.

## Requirements

- Docker Desktop (with Docker Compose v2)
- About 4 GB of free RAM and 6 GB of free disk space

## Run it

```bash
docker compose up -d --build
```

- Airflow UI: http://localhost:8080 (user `admin`, password `admin`). Enable the `crypto_hourly_ohlc` DAG.
- Watch the flow: `docker compose logs -f producer consumer`
- Read the topic:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto.prices --from-beginning --max-messages 5
```

- Check consumer lag:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group crypto-loader
```

- Query the results:

```bash
docker compose exec postgres psql -U postgres -d crypto -c "select * from hourly_ohlc"
```

Stop everything with `docker compose stop` (data is kept) or `docker compose down` (containers are removed, volumes are kept).

> The credentials in this repo (`postgres/postgres`, `admin/admin`) are for local development only.

## Project structure

```
.
├── app/                  # producer and consumer (Python, confluent-kafka)
├── dags/                 # Airflow DAG
├── sql/init.sql          # database and table creation
├── docs/notes.md         # study notes: Kafka and Airflow concepts
└── docker-compose.yml
```

## Roadmap

- [ ] Scale the consumer group and observe partition rebalancing
- [ ] Failure experiments: stop the consumer, Kafka or Postgres and document the recovery
- [ ] Second consumer group that alerts on price drops greater than 2%
- [ ] Airflow backfill with `catchup=True`
- [ ] Grafana dashboard on `hourly_ohlc`
- [ ] Tests with pytest and CI with GitHub Actions
- [ ] Transformations with dbt
- [ ] Schema Registry with Avro

## License

MIT. See [LICENSE](LICENSE).
