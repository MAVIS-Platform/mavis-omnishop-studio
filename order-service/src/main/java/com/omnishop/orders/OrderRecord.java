package com.omnishop.orders;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "orders_records")
public class OrderRecord {
    @Id
    @Column(length = 48)
    private String id;

    @Column(nullable = false, length = 64)
    private String customerId;

    @Column(nullable = false)
    private Long productId;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(length = 64)
    private String paymentId;

    @Column(length = 16)
    private String paymentProtocol;

    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    public OrderRecord() {}

    public OrderRecord(String id, String customerId, Long productId, int quantity, BigDecimal totalAmount,
                       String status, String paymentId, String paymentProtocol) {
        this.id = id;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.status = status;
        this.paymentId = paymentId;
        this.paymentProtocol = paymentProtocol;
        this.createdAt = Instant.now();
    }

    public String getId() { return id; }
    public String getCustomerId() { return customerId; }
    public Long getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getPaymentId() { return paymentId; }
    public String getPaymentProtocol() { return paymentProtocol; }
    public Instant getCreatedAt() { return createdAt; }
}
