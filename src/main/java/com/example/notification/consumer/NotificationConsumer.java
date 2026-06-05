package com.example.notification.consumer;

import com.example.notification.dto.NotificationMessage;
import com.example.notification.model.Notification;
import com.example.notification.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationRepository repository;

    /**
     * @KafkaListener tells Spring: "keep polling this topic in the background."
     * Every time a message arrives, this method is called automatically.
     *
     * Interview explanation:
     * - groupId: consumers with the same group share the work (each message goes to one consumer).
     * - If this service crashes and restarts, Kafka resumes from where it left off (offsets).
     * - @Payload = the actual message. @Header = Kafka metadata like partition and offset.
     *
     * The try/catch is important — if processing fails, we save it as FAILED
     * instead of crashing the consumer. This is basic production awareness.
     */
    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(
            @Payload NotificationMessage message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received message | userId={} | partition={} | offset={}", message.getUserId(), partition, offset);

        try {
            // Simulate sending the notification (email/SMS/push)
            processNotification(message);

            // Save successful record to PostgreSQL
            Notification notification = Notification.builder()
                    .userId(message.getUserId())
                    .email(message.getEmail())
                    .subject(message.getSubject())
                    .body(message.getBody())
                    .type(message.getType())
                    .status(Notification.Status.SENT)
                    .processedAt(LocalDateTime.now())
                    .build();

            repository.save(notification);
            log.info("Notification saved to DB | userId={} | status=SENT", message.getUserId());

        } catch (Exception ex) {
            log.error("Failed to process notification | userId={} | error={}", message.getUserId(), ex.getMessage());

            // Save failed record so nothing is silently lost
            Notification failed = Notification.builder()
                    .userId(message.getUserId())
                    .email(message.getEmail())
                    .subject(message.getSubject())
                    .body(message.getBody())
                    .type(message.getType())
                    .status(Notification.Status.FAILED)
                    .processedAt(LocalDateTime.now())
                    .build();

            repository.save(failed);
        }
    }

    /**
     * In a real system this would call an email provider (SendGrid, SES),
     * an SMS gateway (Twilio), or a push service (FCM).
     * Kept as a log statement here so the project runs without external dependencies.
     */
    private void processNotification(NotificationMessage message) {
        log.info("Sending {} to {} | subject: {}", message.getType(), message.getEmail(), message.getSubject());
        // e.g. emailService.send(message.getEmail(), message.getSubject(), message.getBody());
    }
}
