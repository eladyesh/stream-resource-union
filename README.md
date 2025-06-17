# Kafka Commands

### Produce message:
Use this command to send a JSON-formatted message to the Kafka topic `otel-traces-a`.

```bash
echo '{"traceId":"test123","span":"test-span","service":"test-service1234323423"}' | docker exec -i a7c54c0a3431 kafka-console-producer --broker-list localhost:9092 --topic otel-traces-a
```

---

### Create topic:

Create a Kafka topic named `otel-traces-a` with 1 partition and your specified replication factor.
Replace `<kafka-container-id>` with your actual Kafka container ID.

```bash
docker exec -it <kafka-container-id> kafka-topics --create --topic otel-traces-a --bootstrap-server localhost:9092 --partitions 1 --replication-factor 
```

---

### Delete topic:

Deletes the Kafka topic `otel-traces-a`, waits for 5 seconds, then recreates the topic with 1 partition and replication factor 1.

```bash
docker exec -it a7c54c0a3431 kafka-topics --bootstrap-server localhost:9092 --delete --topic otel-traces-a && timeout /t 5 && docker exec -it a7c54c0a3431 kafka-topics --bootstrap-server localhost:9092 --create --topic otel-traces-a --partitions 1 --replication-factor 1
```
