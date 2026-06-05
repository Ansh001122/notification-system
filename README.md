# Notification System — Kafka + Spring Boot + PostgreSQL

A simple, interview-ready project demonstrating event-driven architecture.

## How it works (the one-liner for interviews)

> "A REST API receives a notification request, publishes it as a JSON event to a Kafka topic, and a consumer picks it up asynchronously and saves it to PostgreSQL."

## Flow

```
POST /api/notifications
        |
        v
 NotificationController
        |
        v
 NotificationProducer  ──────────►  Kafka Topic: "notifications"
                                            |
                                            v
                                   NotificationConsumer
                                            |
                                            v
                                       PostgreSQL
                                   (notifications table)
```

## Tech Stack

- Java 17
- Spring Boot 3.2
- Apache Kafka (Spring Kafka)
- PostgreSQL 15
- Docker + Docker Compose
- Lombok

## Run it locally

### Step 1 — Start Kafka and PostgreSQL

```bash
docker-compose up -d
```

### Step 2 — Run the app

```bash
mvn spring-boot:run
```

### Step 3 — Send a notification

```bash
curl -X POST http://localhost:8080/api/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "userId": 1,
    "email": "ansh@example.com",
    "subject": "Your order is confirmed",
    "body": "Order #101 has been placed successfully.",
    "type": "EMAIL"
  }'
```

### Step 4 — Check it was saved

```bash
curl http://localhost:8080/api/notifications
```

### Step 5 — See it in Kafka UI

Open **http://localhost:9090** → Topics → notifications → Messages

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/notifications` | Send a notification (triggers full flow) |
| GET | `/api/notifications` | Get all saved notifications |
| GET | `/api/notifications/user/{userId}` | Get notifications for a user |
| GET | `/api/notifications/failed` | Get failed notifications |

---

## Project Structure

```
src/main/java/com/example/notification/
├── NotificationApplication.java      ← entry point
├── config/
│   └── KafkaConfig.java              ← creates the Kafka topic
├── controller/
│   └── NotificationController.java   ← REST API
├── producer/
│   └── NotificationProducer.java     ← sends to Kafka
├── consumer/
│   └── NotificationConsumer.java     ← reads from Kafka, saves to DB
├── service/
│   └── NotificationService.java      ← glues everything
├── model/
│   └── Notification.java             ← JPA entity (DB table)
├── repository/
│   └── NotificationRepository.java   ← Spring Data JPA
└── dto/
    └── NotificationMessage.java      ← Kafka message shape
```

---

## Interview Q&A (practice these)

**Q: What is Kafka and why did you use it here?**
> Kafka is a distributed message broker. I used it so the sender and receiver are decoupled — the REST API just publishes an event and returns immediately, without waiting for the notification to actually be sent.

**Q: What is a Producer?**
> The Producer is `NotificationProducer`. It uses `KafkaTemplate.send()` to publish a message to the `notifications` topic. The message is serialized to JSON before sending.

**Q: What is a Consumer?**
> The Consumer is `NotificationConsumer`. The `@KafkaListener` annotation tells Spring to poll the topic continuously. When a message arrives, the method runs automatically and saves the result to PostgreSQL.

**Q: What is a Consumer Group?**
> A consumer group is a set of consumers that share the work. Each message goes to only one consumer in the group. If I had two instances of this service running, they'd share the load — one handles odd messages, the other handles even ones.

**Q: What happens if the service crashes mid-processing?**
> Kafka tracks offsets — where each consumer group last read. On restart, the consumer picks up from where it stopped. That's why I use `auto-offset-reset: earliest` in config.

**Q: What is a Topic? What is a Partition?**
> A topic is like a queue for a specific type of event — here it's `notifications`. A partition is a subdivision of a topic for parallelism. More partitions = more consumers can read simultaneously. I used 1 partition here for simplicity.

**Q: Why save to PostgreSQL after consuming?**
> It's an audit log. Every notification processed — whether it succeeded or failed — is recorded. You can query `/api/notifications/failed` to find anything that needs a retry.

**Q: What if Kafka is down when I POST a notification?**
> The `KafkaTemplate.send()` will fail and the callback logs the error. In production, you'd add a retry mechanism or a fallback queue. For this demo, the error is logged clearly.
