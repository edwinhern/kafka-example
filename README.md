# kafka-example

Goal:

1. Learn how to create a producer
2. Learn to create a Kafka Topic
3. Learn how to create a consumer
4. Learn how to define schema using proto
5. See workflow work

## Usage

### 1. Start Kafka & Kafbat UI

```bash
docker compose up -d
```

- Kafka: `localhost:9092`
- Kafbat UI: [http://localhost:8080](http://localhost:8080)

### 2. Produce Messages

```bash
pnpm producer
```

### 3. Consume Messages

```bash
pnpm consumer
```
