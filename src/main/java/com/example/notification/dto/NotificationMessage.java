package com.example.notification.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This is the object that travels through Kafka.
 * Producer serializes it to JSON → Kafka stores it → Consumer deserializes it back.
 *
 * Interview tip: Kafka doesn't care about your object type.
 * It just sees bytes. JsonSerializer/Deserializer handle the conversion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationMessage {
    private Long userId;
    private String email;
    private String subject;
    private String body;
    private String type;   // EMAIL, SMS, PUSH
}
