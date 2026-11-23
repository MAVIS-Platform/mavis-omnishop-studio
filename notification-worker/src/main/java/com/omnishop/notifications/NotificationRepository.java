package com.omnishop.notifications;

import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface NotificationRepository extends MongoRepository<NotificationDocument, String> {
    List<NotificationDocument> findTop20ByOrderByReceivedAtDesc();
    long countBySourceBroker(String sourceBroker);
}
