package com.example.notification.controller;

import com.example.notification.dto.NotificationMessage;
import com.example.notification.model.Notification;
import com.example.notification.service.NotificationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService service;

    /**
     * POST /api/notifications
     * Triggers the full flow:
     *   HTTP request → Producer → Kafka → Consumer → PostgreSQL
     */
    @PostMapping
    public ResponseEntity<String> send(@Valid @RequestBody NotificationMessage message) {
        service.sendNotification(message);
        return ResponseEntity.ok("Notification queued successfully");
    }

    @GetMapping
    public ResponseEntity<List<Notification>> getAll() {
        return ResponseEntity.ok(service.getAllNotifications());
    }

    @GetMapping("/user/{userId}")
    public ResponseEntity<List<Notification>> getByUser(@PathVariable Long userId) {
        return ResponseEntity.ok(service.getByUser(userId));
    }

    @GetMapping("/failed")
    public ResponseEntity<List<Notification>> getFailed() {
        return ResponseEntity.ok(service.getFailedNotifications());
    }
}
