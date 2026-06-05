package com.example.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic}")
    private String topic;

    /**
     * Spring auto-creates this topic if it doesn't exist yet.
     * 1 partition for simplicity. In production you'd use 3+ for parallelism.
     *
     * Interview tip: partitions = parallelism. More partitions = more consumers
     * can read at the same time.
     */
    @Bean
    public NewTopic notificationTopic() {
        return TopicBuilder.name(topic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
