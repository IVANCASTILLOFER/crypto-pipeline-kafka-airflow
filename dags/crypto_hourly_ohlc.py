"""Batch por hora: valida datos crudos, agrega a velas OHLC y verifica calidad."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN = "crypto_db"


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["crypto", "kafka"],
)
def crypto_hourly_ohlc():

    @task
    def check_raw_data(data_interval_start=None, data_interval_end=None):
        hook = PostgresHook(CONN)
        n = hook.get_first(
            "SELECT count(*) FROM raw_prices WHERE event_ts >= %s AND event_ts < %s",
            parameters=(data_interval_start, data_interval_end),
        )[0]
        if n == 0:
            raise ValueError("No llegaron datos desde Kafka en esta ventana")
        return n

    aggregate = SQLExecuteQueryOperator(
        task_id="aggregate_hourly",
        conn_id=CONN,
        sql="""
        INSERT INTO hourly_ohlc (symbol, hour_ts, open, high, low, close, n_ticks)
        SELECT symbol, date_trunc('hour', event_ts),
               (array_agg(price ORDER BY event_ts ASC))[1],
               max(price), min(price),
               (array_agg(price ORDER BY event_ts DESC))[1],
               count(*)
        FROM raw_prices
        WHERE event_ts >= '{{ data_interval_start }}' AND event_ts < '{{ data_interval_end }}'
        GROUP BY symbol, date_trunc('hour', event_ts)
        ON CONFLICT (symbol, hour_ts) DO UPDATE
          SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
              close = EXCLUDED.close, n_ticks = EXCLUDED.n_ticks;
        """,
    )

    @task
    def quality_check(data_interval_start=None):
        hook = PostgresHook(CONN)
        bad = hook.get_first(
            """SELECT count(*) FROM hourly_ohlc
               WHERE hour_ts = date_trunc('hour', %s::timestamptz)
                 AND (low > high OR open NOT BETWEEN low AND high
                      OR close NOT BETWEEN low AND high)""",
            parameters=(data_interval_start,),
        )[0]
        if bad:
            raise ValueError(f"{bad} velas inconsistentes")

    check_raw_data() >> aggregate >> quality_check()


crypto_hourly_ohlc()
