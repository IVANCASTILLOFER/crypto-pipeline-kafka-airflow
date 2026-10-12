# Study Notes: Kafka, Airflow, Docker and Git

Concepts learned while building this project, written as short answers to common interview questions.

## Kafka

**What is a topic?**
A named channel where related events are published. Kafka keeps the messages (retention) instead of deleting them once they are read.

**What is a partition?**
A topic is split into partitions so that work can be spread across consumers and read in parallel. Ordering is guaranteed only inside a single partition. This project's topic has 3.

**What is an offset?**
The position of a message inside its partition. A consumer group stores the last offset it processed, which is how it resumes after a restart.

**Why use the symbol as the message key?**
Kafka hashes the key to choose the partition. All events with the same key land in the same partition, so they stay in order. Two keys can end up in the same partition, and some partitions can stay empty. That is normal.

**What happens if the consumer goes down?**
Nothing is lost. Messages stay in Kafka. When the consumer returns, it continues from its last committed offset and catches up.

**What is consumer lag?**
The number of messages the consumer still has to read: `LOG-END-OFFSET - CURRENT-OFFSET`. A small lag is normal. A lag that keeps growing means the consumer cannot keep up.

**Why commit the offset after writing to the database?**
If the commit happened first and the consumer crashed, Kafka would consider those messages processed while they were never saved, so data would be lost. Committing after the write means a crash causes a re-read (a duplicate), never a loss. This is at-least-once delivery.

**What is idempotency, and where is it used here?**
An operation is idempotent when running it many times has the same effect as running it once. Here, the unique constraint on `(kafka_partition, kafka_offset)` plus `ON CONFLICT DO NOTHING` makes re-processing safe.

**Delivery guarantees**
- At-most-once: may lose messages, never duplicates.
- At-least-once: never loses messages, may duplicate (used here).
- Exactly-once: no loss and no duplicates, needs transactions.

**What is a consumer group and a rebalance?**
A consumer group shares the partitions of a topic among its members, and each partition is read by one member at a time. When a member joins or leaves, Kafka rebalances the partitions.

## Airflow

**What is a DAG?**
A Directed Acyclic Graph: tasks with dependencies and no loops. Tasks without dependencies between them can run in parallel.

**What does `>>` mean?**
"Then". `a >> b` makes `b` run only after `a` succeeds.

**`failed` vs `upstream_failed`**
- `failed`: the task ran and failed. This is where the problem is.
- `upstream_failed`: the task never ran because an upstream task failed.

Debugging rule: find the red task and read its **Logs** (from the bottom up). The **Event Log** only records actions taken in Airflow, not code errors.

**What does `retries` do?**
It sets how many extra attempts a failed task gets. `retries=2` means one original attempt plus up to two retries.

**What does `catchup=False` do?**
Airflow runs only the latest interval instead of every interval since `start_date`. With `catchup=True` it would schedule all the missed intervals (a backfill).

**Logical dates**
Each run processes its own data interval (`data_interval_start` to `data_interval_end`), not "now". A run at 14:00 processes 13:00 to 14:00. This is why the first run failed: no events existed for the previous hour.

## Docker

**Container vs virtual machine**
A container shares the host operating system kernel, so it is lighter and starts in seconds. A virtual machine includes a full guest OS.

**What does Docker Compose do?**
It reads `docker-compose.yml` and starts every service of the project together, with its networks and volumes.

**Key commands**

| Command | Purpose |
|---|---|
| `docker compose up -d --build` | Start all services (and rebuild images) |
| `docker compose ps` | Show service status |
| `docker compose logs -f <service>` | Follow logs |
| `docker compose stop` / `start` | Pause / resume, keeping everything |
| `docker compose down` | Remove containers, keep volumes |
| `docker compose down -v` | Remove containers **and volumes** (data loss) |
| `docker stats --no-stream` | CPU and memory per container |
| `docker system df` | Disk space used by Docker |

## Git

```
edit files -> git add -> git commit -m "message" -> git push
```

- `git add` stages changes.
- `git commit` saves a snapshot in the local history.
- `git push` sends the commits to the remote (GitHub).
- A pull request is a separate step: a request to merge one branch into another.
- Every repository has its own branches. Two repositories can both have a `main` without any relation.
