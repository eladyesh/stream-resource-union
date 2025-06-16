produce message: 
echo '{"traceId":"test123","span":"test-span","service":"test-service1234323423"}' | docker exec -i a7c54c0a3431 kafka-console-producer --broker-list localhost:9092 --topic otel-traces-a


create topic:
docker exec -it <kafka-container-id> kafka-topics --create --topic otel-traces-a --bootstrap-server localhost:9092 --partitions 1 --replication-factor 



delete topic:
docker exec -it a7c54c0a3431 kafka-topics --bootstrap-server localhost:9092 --delete --topic otel-traces-a && timeout /t 5 && docker exec -it a7c54c0a3431 kafka-topics --bootstrap-server localhost:9092 --create --topic otel-traces-a --partitions 1 --replication-factor 1
