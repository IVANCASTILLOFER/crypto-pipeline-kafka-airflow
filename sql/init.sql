CREATE DATABASE airflow;
CREATE DATABASE crypto;
\c crypto

CREATE TABLE raw_prices (
    id               BIGSERIAL PRIMARY KEY,
    symbol           TEXT NOT NULL,
    price            NUMERIC(20,8) NOT NULL,
    event_ts         TIMESTAMPTZ NOT NULL,
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    UNIQUE (kafka_partition, kafka_offset)   -- idempotencia del consumer
);
CREATE INDEX ON raw_prices (event_ts);

CREATE TABLE hourly_ohlc (
    symbol   TEXT NOT NULL,
    hour_ts  TIMESTAMPTZ NOT NULL,
    open     NUMERIC(20,8),
    high     NUMERIC(20,8),
    low      NUMERIC(20,8),
    close    NUMERIC(20,8),
    n_ticks  INT,
    PRIMARY KEY (symbol, hour_ts)
);
