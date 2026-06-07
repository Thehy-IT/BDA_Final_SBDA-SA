# Banking Transaction Anomaly Pipeline

Real-time fraud detection on synthetic banking transactions using Kafka and a micro-batch stream processor, with a live monitoring dashboard.

```
Producer → Kafka (transactions) → Anomaly Detector → Kafka (fraud-alerts) → Dashboard
                                        ↕
                                      Redis
                                  (velocity / geo state)
```

## Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka 3.6 (KRaft, no ZooKeeper) |
| Stream processor | Python micro-batch (drop-in PySpark version included) |
| Fraud state store | Redis 7 |
| Dashboard backend | FastAPI + Server-Sent Events |
| Dashboard frontend | Vanilla JS + Chart.js |
| Orchestration | Docker Compose |

## Hướng dẫn chạy dự án bằng Docker (Quick start)

Dự án được triển khai dễ dàng thông qua Docker Compose. Hãy đảm bảo bạn đã cài đặt Docker và Docker Compose trên máy của mình.

### 1. Khởi chạy toàn bộ hệ thống
Sử dụng lệnh sau để build các images và chạy các containers ở chế độ ngầm (detached mode):

```bash
docker compose up --build -d
```
*(Hoặc dùng Makefile: `make up`)*

Sau khi hệ thống khởi động hoàn tất, bạn có thể truy cập **Dashboard giám sát** tại địa chỉ:
👉 **[http://localhost:8080](http://localhost:8080)**

### 2. Xem logs của hệ thống
Để theo dõi quá trình sinh dữ liệu hoặc phát hiện gian lận theo thời gian thực:

```bash
# Xem toàn bộ logs:
docker compose logs -f

# Hoặc chỉ xem logs của một số dịch vụ cụ thể:
docker compose logs -f producer processor dashboard
```
*(Hoặc dùng Makefile: `make logs`)*

### 3. Tạm dừng hệ thống (Giữ lại dữ liệu)
Để tắt các containers nhưng vẫn giữ lại cấu hình volumes (dữ liệu của Kafka, Redis):

```bash
docker compose down
```
*(Hoặc dùng Makefile: `make down`)*

### 4. Tắt và xoá sạch dữ liệu (Clean)
Nếu bạn muốn reset hoàn toàn trạng thái (xoá cả containers, networks, và volumes dữ liệu):

```bash
docker compose down -v --remove-orphans
```
*(Hoặc dùng Makefile: `make clean`)*

## Fraud detection rules

| Rule | Trigger | Severity |
|---|---|---|
| `HIGH_AMOUNT` | Transaction ≥ $5,000 | HIGH / CRITICAL |
| `CARD_NOT_PRESENT_HIGH` | Card absent + amount ≥ $2,000 | HIGH |
| `ODD_HOURS` | Transaction at 02:00–05:00 UTC | MEDIUM |
| `ROUND_NUMBER` | Exact round amount ($500, $1 000 …) | LOW |
| `HIGH_RISK_MERCHANT` | Category in `wire_transfer`, `crypto`, `gambling`, `unknown` | MEDIUM |
| `VELOCITY` | > 6 transactions from same account in 10 min | HIGH |
| `GEO_VELOCITY` | Same account in locations ≥ 400 km apart within 30 min | CRITICAL |

Risk scores from triggered rules are summed; the highest bucket determines overall severity.

## PySpark deployment

A production-ready PySpark Structured Streaming job is included at `processor/spark_detector.py`. Run it on any Spark cluster:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  processor/spark_detector.py
```

Stateful rules (velocity, geo-velocity) are handled via Redis `ForeachBatch` in `anomaly_detector.py` and can be ported to Spark's `mapGroupsWithState` for fully stateful cluster processing.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TPS` | `5` | Transactions per second from the producer |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap address |
| `REDIS_HOST` | `redis` | Redis hostname |

Copy `.env.example` → `.env` to override.

## Project layout

```
banking-anomaly-pipeline/
├── docker-compose.yml
├── Makefile
├── producer/
│   ├── transaction_producer.py   # synthetic data generator
│   └── Dockerfile
├── processor/
│   ├── anomaly_detector.py       # micro-batch processor (runs in Docker)
│   ├── spark_detector.py         # PySpark Structured Streaming version
│   ├── fraud_rules.py            # stateless + stateful rule engine
│   └── Dockerfile
└── dashboard/
    ├── app.py                    # FastAPI + SSE backend
    ├── static/index.html         # real-time monitoring UI
    └── Dockerfile
```
