package com.omnishop.notifications;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.Instant;

@Document(collection = "notifications")
public class NotificationDocument {
    @Id
    private String id;
    private String sourceBroker;
    private String routingKey;
    private String payload;
    private Instant receivedAt;

    public NotificationDocument() {}

    public NotificationDocument(String sourceBroker, String routingKey, String payload) {
        this.sourceBroker = sourceBroker;
        this.routingKey = routingKey;
        this.payload = payload;
        this.receivedAt = Instant.now();
    }

    public String getId() { return id; }
    public String getSourceBroker() { return sourceBroker; }
    public String getRoutingKey() { return routingKey; }
    public String getPayload() { return payload; }
    public Instant getReceivedAt() { return receivedAt; }
}
