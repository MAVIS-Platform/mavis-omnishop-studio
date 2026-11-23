package com.omnishop.notifications;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class NotificationConsumerController {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumerController.class);

    private final NotificationRepository repository;
    private final String version;
    private final int artificialLatencyMs;
    private final int consumerLagMs;

    private final AtomicLong rabbitConsumed = new AtomicLong();
    private final AtomicLong kafkaConsumed = new AtomicLong();

    public NotificationConsumerController(
            NotificationRepository repository,
            @Value("${omnishop.version:v1}") String version,
            @Value("${omnishop.artificial-latency-ms:0}") int artificialLatencyMs,
            @Value("${omnishop.consumer-lag-ms:0}") int consumerLagMs) {
        this.repository = repository;
        this.version = version;
        this.artificialLatencyMs = Math.max(0, Math.min(3000, artificialLatencyMs));
        this.consumerLagMs = Math.max(0, Math.min(3000, consumerLagMs));
    }

    @RabbitListener(queues = NotificationWorkerApplication.ORDERS_QUEUE)
    public void onOrderEvent(String payload) {
        simulateConsumerLag();
        long count = rabbitConsumed.incrementAndGet();
        persistSafe("rabbitmq", "order.created", payload);
        if (count <= 5 || count % 25 == 0) {
            log.info("[ASYNC:RabbitMQ] consumed order event count={}", count);
        }
    }

    @RabbitListener(queues = NotificationWorkerApplication.PAYMENTS_QUEUE)
    public void onPaymentEvent(String payload) {
        simulateConsumerLag();
        long count = rabbitConsumed.incrementAndGet();
        persistSafe("rabbitmq", "payment.settled", payload);
        if (count <= 5 || count % 25 == 0) {
            log.info("[ASYNC:RabbitMQ] consumed payment event count={}", count);
        }
    }

    @KafkaListener(topics = "order-events", groupId = "notification-analytics-group")
    public void onKafkaOrderEvent(String payload) {
        simulateConsumerLag();
        long count = kafkaConsumed.incrementAndGet();
        persistSafe("kafka", "order-events", payload);
        if (count <= 5 || count % 25 == 0) {
            log.info("[ASYNC:Kafka] consumed telemetry event count={}", count);
        }
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "UP",
                "service", "notification-worker",
                "version", version,
                "rabbitConsumed", rabbitConsumed.get(),
                "kafkaConsumed", kafkaConsumed.get(),
                "timestamp", Instant.now().toString()
        );
    }

    @GetMapping("/api/notifications")
    public Map<String, Object> recentNotifications() {
        applyLatency();
        try {
            List<NotificationDocument> recent = repository.findTop20ByOrderByReceivedAtDesc();
            return Map.of("items", recent, "total", repository.count());
        } catch (Exception ex) {
            log.warn("MongoDB query fallback: {}", ex.getMessage());
            return Map.of("items", List.of(), "total", rabbitConsumed.get() + kafkaConsumed.get(), "mongoFallback", true);
        }
    }

    @GetMapping("/api/analytics/summary")
    public Map<String, Object> analyticsSummary() {
        applyLatency();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("service", "notification-worker");
        out.put("version", version);
        out.put("rabbitConsumedInMemory", rabbitConsumed.get());
        out.put("kafkaConsumedInMemory", kafkaConsumed.get());
        try {
            long totalDocs = repository.count();
            long rabbitDocs = repository.countBySourceBroker("rabbitmq");
            long kafkaDocs = repository.countBySourceBroker("kafka");
            out.put("totalNotifications", totalDocs);
            out.put("mongoRabbitDocs", rabbitDocs);
            out.put("mongoKafkaDocs", kafkaDocs);
            out.put("mongoStatus", "CONNECTED");
        } catch (Exception ex) {
            out.put("totalNotifications", rabbitConsumed.get() + kafkaConsumed.get());
            out.put("mongoStatus", "DEGRADED:" + ex.getClass().getSimpleName());
        }
        return out;
    }

    private void persistSafe(String broker, String key, String payload) {
        try {
            repository.save(new NotificationDocument(broker, key, payload));
        } catch (Exception ex) {
            log.warn("MongoDB persist skipped ({}:{}): {}", broker, key, ex.getMessage());
        }
    }

    private void simulateConsumerLag() {
        if (consumerLagMs > 0) {
            try { Thread.sleep(consumerLagMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private void applyLatency() {
        if (artificialLatencyMs > 0) {
            try { Thread.sleep(artificialLatencyMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
