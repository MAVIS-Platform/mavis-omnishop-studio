package com.omnishop.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omnishop.payment.grpc.AuthorizePaymentRequest;
import com.omnishop.payment.grpc.AuthorizePaymentResponse;
import com.omnishop.payment.grpc.PaymentGatewayGrpc;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@RestController
public class OrderController implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderRepository repository;
    private final RestClient restClient;
    private final ManagedChannel paymentGrpcChannel;
    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String version;
    private final String catalogUrl;
    private final String paymentUrl;
    private final String paymentProtocol;
    private final boolean publishAsyncEvents;
    private final int artificialLatencyMs;
    private final int faultErrorRatePct;

    public OrderController(
            OrderRepository repository,
            RestClient restClient,
            ManagedChannel paymentGrpcChannel,
            RabbitTemplate rabbitTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${omnishop.version:v1}") String version,
            @Value("${omnishop.catalog-url}") String catalogUrl,
            @Value("${omnishop.payment-url}") String paymentUrl,
            @Value("${omnishop.payment-protocol:grpc}") String paymentProtocol,
            @Value("${omnishop.publish-async-events:true}") boolean publishAsyncEvents,
            @Value("${omnishop.artificial-latency-ms:0}") int artificialLatencyMs,
            @Value("${omnishop.fault-error-rate-pct:0}") int faultErrorRatePct) {
        this.repository = repository;
        this.restClient = restClient;
        this.paymentGrpcChannel = paymentGrpcChannel;
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.version = version;
        this.catalogUrl = catalogUrl;
        this.paymentUrl = paymentUrl;
        this.paymentProtocol = paymentProtocol != null ? paymentProtocol.trim().toLowerCase(Locale.ROOT) : "grpc";
        this.publishAsyncEvents = publishAsyncEvents;
        this.artificialLatencyMs = Math.max(0, Math.min(3000, artificialLatencyMs));
        this.faultErrorRatePct = Math.max(0, Math.min(100, faultErrorRatePct));
    }

    @Override
    public void run(String... args) {
        seedInternal();
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "UP",
                "service", "order-service",
                "version", version,
                "paymentProtocol", paymentProtocol,
                "publishAsyncEvents", publishAsyncEvents,
                "timestamp", Instant.now().toString()
        );
    }

    @PostMapping("/api/seed")
    public Map<String, Object> seed() {
        int count = seedInternal();
        return Map.of("seeded", true, "ordersCount", count);
    }

    @GetMapping("/api/orders")
    public Map<String, Object> list() {
        applyRuntimeFaults();
        List<OrderRecord> items = repository.findAll();
        return Map.of("items", items.stream().map(this::toDto).toList(), "total", items.size());
    }

    @GetMapping("/api/orders/{id}")
    public ResponseEntity<Map<String, Object>> getById(@PathVariable String id) {
        applyRuntimeFaults();
        return repository.findById(id)
                .map(o -> ResponseEntity.ok(toDto(o)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "ORDER_NOT_FOUND", "id", id)));
    }

    /**
     * Read-only GET orchestration path driven by MAVIS LiveProbe:
     * Queries Postgres -> Validates product via REST (catalog-service) ->
     * Authorizes probe via gRPC or REST (payment-service .NET 8) ->
     * Emits real async telemetry events to RabbitMQ + Kafka (consumed by notification-worker -> MongoDB).
     */
    @GetMapping("/api/orders/live-flow")
    public Map<String, Object> liveFlow() {
        applyRuntimeFaults();
        OrderRecord sample = repository.findById("ORD-1001").orElseGet(() -> {
            seedInternal();
            return repository.findById("ORD-1001").orElse(
                    new OrderRecord("ORD-1001", "cust-demo", 1L, 1, new BigDecimal("2499.00"), "PAID", "PAY-SEED-1", paymentProtocol));
        });

        Map<String, Object> product = restClient.get()
                .uri(catalogUrl + "/api/products/" + sample.getProductId())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() {});

        Map<String, Object> payment = authorizePayment(
                sample.getId(),
                sample.getCustomerId(),
                sample.getTotalAmount().doubleValue(),
                "probe-" + sample.getId(),
                true
        );

        Map<String, Object> asyncStatus = publishAsync(sample, payment, true);

        Map<String, Object> res = new LinkedHashMap<>();
        res.put("order", toDto(sample));
        res.put("catalogCheck", product);
        res.put("paymentAuth", payment);
        res.put("asyncMessaging", asyncStatus);
        res.put("protocol", paymentProtocol);
        return res;
    }

    @PostMapping("/api/orders")
    public ResponseEntity<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        applyRuntimeFaults();
        long productId = body.get("productId") instanceof Number n ? n.longValue() : 1L;
        int quantity = body.get("quantity") instanceof Number n ? Math.max(1, n.intValue()) : 1;
        String customerId = String.valueOf(body.getOrDefault("customerId", "cust-" + ThreadLocalRandom.current().nextInt(100, 999)));

        Map<String, Object> product = restClient.get()
                .uri(catalogUrl + "/api/products/" + productId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() {});
        double unitPrice = product != null && product.get("price") instanceof Number n ? n.doubleValue() : 199.0;
        double total = unitPrice * quantity;
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Map<String, Object> payment = authorizePayment(orderId, customerId, total, "idem-" + orderId, false);
        String payStatus = String.valueOf(payment.getOrDefault("status", "AUTHORIZED"));
        String payId = String.valueOf(payment.getOrDefault("paymentId", "PAY-" + orderId));

        OrderRecord saved = repository.save(new OrderRecord(
                orderId, customerId, productId, quantity, BigDecimal.valueOf(total),
                "AUTHORIZED".equalsIgnoreCase(payStatus) ? "PAID" : "PAYMENT_FAILED",
                payId, paymentProtocol
        ));
        Map<String, Object> asyncStatus = publishAsync(saved, payment, false);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "order", toDto(saved),
                "payment", payment,
                "asyncMessaging", asyncStatus
        ));
    }

    @PutMapping("/api/orders/{id}/status")
    public ResponseEntity<Map<String, Object>> updateStatus(@PathVariable String id, @RequestBody Map<String, Object> body) {
        applyRuntimeFaults();
        return repository.findById(id).map(o -> {
            o.setStatus(String.valueOf(body.getOrDefault("status", "SHIPPED")));
            return ResponseEntity.ok(toDto(repository.save(o)));
        }).orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "ORDER_NOT_FOUND")));
    }

    @DeleteMapping("/api/orders/{id}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable String id) {
        applyRuntimeFaults();
        if (!repository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "ORDER_NOT_FOUND"));
        }
        repository.deleteById(id);
        return ResponseEntity.ok(Map.of("deleted", true, "id", id));
    }

    private Map<String, Object> authorizePayment(String orderId, String customerId, double amount, String idempotencyKey, boolean dryRun) {
        if ("rest".equals(paymentProtocol)) {
            return restClient.post()
                    .uri(paymentUrl + "/api/payments/authorize")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "orderId", orderId,
                            "customerId", customerId,
                            "amount", amount,
                            "currency", "USD",
                            "idempotencyKey", idempotencyKey,
                            "dryRun", dryRun
                    ))
                    .retrieve()
                    .body(new ParameterizedTypeReference<Map<String, Object>>() {});
        }
        // Default: real HTTP/2 Protobuf gRPC call to .NET 8 payment-service:9090
        PaymentGatewayGrpc.PaymentGatewayBlockingStub stub = PaymentGatewayGrpc
                .newBlockingStub(paymentGrpcChannel)
                .withDeadlineAfter(3, TimeUnit.SECONDS);
        AuthorizePaymentResponse resp = stub.authorizePayment(AuthorizePaymentRequest.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setAmount(amount)
                .setCurrency("USD")
                .setIdempotencyKey(idempotencyKey)
                .setDryRun(dryRun)
                .build());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("paymentId", resp.getPaymentId());
        out.put("orderId", resp.getOrderId());
        out.put("status", resp.getStatus());
        out.put("fraudScore", resp.getFraudScore());
        out.put("serviceVersion", resp.getServiceVersion());
        out.put("cachedIdempotency", resp.getCachedIdempotency());
        out.put("transport", "gRPC");
        return out;
    }

    private Map<String, Object> publishAsync(OrderRecord order, Map<String, Object> payment, boolean probe) {
        if (!publishAsyncEvents) {
            return Map.of("published", false, "reason", "PUBLISH_ASYNC_EVENTS=false");
        }
        String rabbitState = "OK";
        String kafkaState = "OK";
        try {
            String payload = objectMapper.writeValueAsString(Map.of(
                    "eventType", probe ? "order.probe.verified" : "order.created",
                    "orderId", order.getId(),
                    "customerId", order.getCustomerId(),
                    "amount", order.getTotalAmount().doubleValue(),
                    "paymentVersion", payment.getOrDefault("serviceVersion", "v1"),
                    "protocol", paymentProtocol,
                    "occurredAt", Instant.now().toString()
            ));
            try {
                rabbitTemplate.convertAndSend(OrderServiceApplication.EXCHANGE_NAME, "order.created", payload);
            } catch (Exception ex) {
                rabbitState = "DEGRADED:" + ex.getClass().getSimpleName();
                log.warn("RabbitMQ publish degraded for order={}: {}", order.getId(), ex.getMessage());
            }
            try {
                kafkaTemplate.send(OrderServiceApplication.KAFKA_TOPIC, order.getId(), payload);
            } catch (Exception ex) {
                kafkaState = "DEGRADED:" + ex.getClass().getSimpleName();
                log.warn("Kafka publish degraded for order={}: {}", order.getId(), ex.getMessage());
            }
        } catch (Exception ex) {
            return Map.of("published", false, "error", ex.getMessage());
        }
        return Map.of("rabbitmq", rabbitState, "kafka", kafkaState);
    }

    private int seedInternal() {
        try {
            if (repository.count() == 0) {
                repository.saveAll(List.of(
                        new OrderRecord("ORD-1001", "cust-alice", 1L, 1, new BigDecimal("2499.00"), "PAID", "PAY-1001", "grpc"),
                        new OrderRecord("ORD-1002", "cust-bob", 2L, 2, new BigDecimal("2398.00"), "PAID", "PAY-1002", "grpc"),
                        new OrderRecord("ORD-1003", "cust-carol", 3L, 1, new BigDecimal("399.00"), "SHIPPED", "PAY-1003", "rest")
                ));
                log.info("Seeded 3 default orders into PostgreSQL");
            }
            return (int) repository.count();
        } catch (Exception ex) {
            log.warn("Deferred order seed (DB warming up): {}", ex.getMessage());
            return 0;
        }
    }

    private void applyRuntimeFaults() {
        if (artificialLatencyMs > 0) {
            try { Thread.sleep(artificialLatencyMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (faultErrorRatePct > 0 && ThreadLocalRandom.current().nextInt(100) < faultErrorRatePct) {
            throw new IllegalStateException("Injected order-service fault (" + faultErrorRatePct + "%)");
        }
    }

    private Map<String, Object> toDto(OrderRecord o) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", o.getId());
        m.put("customerId", o.getCustomerId());
        m.put("productId", o.getProductId());
        m.put("quantity", o.getQuantity());
        m.put("totalAmount", o.getTotalAmount().doubleValue());
        m.put("status", o.getStatus());
        m.put("paymentId", o.getPaymentId());
        m.put("paymentProtocol", o.getPaymentProtocol());
        m.put("createdAt", o.getCreatedAt().toString());
        return m;
    }
}
