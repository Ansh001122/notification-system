package com.example.notification.producer;

import com.example.notification.dto.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationProducer {

    private final KafkaTemplate<String, NotificationMessage> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    /**
     * Sends a message to Kafka.
     *
     * Interview explanation:
     * - KafkaTemplate is Spring's wrapper around the Kafka producer API.
     * - send(topic, key, message): topic = where, key = which partition, message = the data.
     * - The message is serialized to JSON bytes before sending.
     * - This is ASYNC — it returns immediately. Kafka handles delivery in the background.
     */
    public void send(NotificationMessage message) {
        log.info("Sending notification to Kafka | userId={} | type={}", message.getUserId(), message.getType());

        kafkaTemplate.send(topic, String.valueOf(message.getUserId()), message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Delivered | topic={} | partition={} | offset={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to deliver message | userId={} | error={}", message.getUserId(), ex.getMessage());
                    }
                });
    }
}
