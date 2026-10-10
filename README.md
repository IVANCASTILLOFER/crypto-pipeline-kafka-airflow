# Pipeline de precios cripto: Kafka + Airflow

```
Binance API -> producer -> [Kafka: crypto.prices] -> consumer -> Postgres.raw_prices
                                                                      |
                                       Airflow DAG (@hourly) <--------+--> hourly_ohlc
```

## Levantar

```
docker compose up -d --build
```

- Airflow: http://localhost:8080 (admin / admin) -> activa el DAG `crypto_hourly_ohlc`
- Ver flujo: `docker compose logs -f producer consumer`
- Leer el topic: `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto.prices --from-beginning --max-messages 5`
- Datos: `docker compose exec postgres psql -U postgres -d crypto -c "select * from hourly_ohlc"`

## Ejercicios

**Kafka**
1. Para el consumer 2 min, mira el lag (`kafka-consumer-groups.sh --describe`) y reinicialo.
2. Escala consumers del mismo group y observa el reparto de particiones.
3. Agrega un 2do consumer group que alerte si el precio cae >2%.
4. Prueba Schema Registry + Avro.

**Airflow**
1. Dispara el DAG manualmente y revisa Graph y Logs.
2. Para el producer y observa como falla `check_raw_data` y sus retries.
3. Agrega un sensor, un BranchOperator y un TaskGroup.
4. Pon `catchup=True` y haz un backfill.
5. Alerta en `on_failure_callback`.

**Siguiente nivel:** dbt sobre `hourly_ohlc`, dashboard en Metabase/Grafana, tests con pytest, CI.
