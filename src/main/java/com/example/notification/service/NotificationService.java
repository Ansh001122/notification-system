package com.example.notification.service;

import com.example.notification.dto.NotificationMessage;
import com.example.notification.model.Notification;
import com.example.notification.producer.NotificationProducer;
import com.example.notification.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationProducer producer;
    private final NotificationRepository repository;

    public void sendNotification(NotificationMessage message) {
        producer.send(message);
    }

    public List<Notification> getAllNotifications() {
        return repository.findAll();
    }

    public List<Notification> getByUser(Long userId) {
        return repository.findByUserId(userId);
    }

    public List<Notification> getFailedNotifications() {
        return repository.findByStatus(Notification.Status.FAILED);
    }
}
