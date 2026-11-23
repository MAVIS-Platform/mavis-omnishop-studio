package com.omnishop.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

@RestController
public class GatewayController {

    private final DownstreamClient downstream;
    private final String version;

    public GatewayController(DownstreamClient downstream, @Value("${omnishop.version:v1}") String version) {
        this.downstream = downstream;
        this.version = version;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "UP",
                "service", "api-gateway",
                "version", version,
                "protocols", List.of("REST", "GraphQL", "SSE"),
                "policies", downstream.activePolicySnapshot(),
                "timestamp", Instant.now().toString()
        );
    }

    @GetMapping("/version")
    public Map<String, Object> version() {
        return Map.of("service", "api-gateway", "version", version);
    }

    /**
     * Read-only GET endpoint tailored for MAVIS LiveProbe: exercises the full architecture
     * (Gateway -> Catalog [Redis+Postgres] -> Order [Postgres + gRPC Payment + RabbitMQ + Kafka]
     * -> Payment [.NET 8 gRPC/REST + Postgres + Redis + RabbitMQ] -> Notification Worker [MongoDB]).
     */
    @GetMapping("/api/v1/storefront/live-flow")
    public Map<String, Object> storefrontLiveFlow() {
        downstream.enforceGatewayPolicies();
        Map<String, Object> catalog = downstream.getCatalogProduct(1L);
        Map<String, Object> orderFlow = downstream.triggerOrderLiveFlow();
        Map<String, Object> analytics = downstream.getAnalyticsSummary();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", "OK");
        out.put("gatewayVersion", version);
        out.put("catalog", catalog);
        out.put("orderFlow", orderFlow);
        out.put("analytics", analytics);
        out.put("timestamp", Instant.now().toString());
        return out;
    }

    @GetMapping("/api/v1/storefront/overview")
    public Map<String, Object> overview() {
        downstream.enforceGatewayPolicies();
        Map<String, Object> products = downstream.getCatalogProducts(null);
        Map<String, Object> orders = downstream.getOrders();
        Map<String, Object> analytics = downstream.getAnalyticsSummary();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("gatewayVersion", version);
        out.put("products", products);
        out.put("orders", orders);
        out.put("analytics", analytics);
        return out;
    }

    @GetMapping("/api/v1/products")
    public Map<String, Object> listProducts(@RequestParam(required = false) String category,
                                            @RequestParam(required = false) String q) {
        downstream.enforceGatewayPolicies();
        StringBuilder qs = new StringBuilder();
        if (category != null && !category.isBlank()) qs.append("category=").append(category);
        if (q != null && !q.isBlank()) {
            if (!qs.isEmpty()) qs.append("&");
            qs.append("q=").append(q);
        }
        return downstream.getCatalogProducts(qs.toString());
    }

    @GetMapping("/api/v1/products/{id}")
    public Map<String, Object> getProduct(@PathVariable long id) {
        downstream.enforceGatewayPolicies();
        return downstream.getCatalogProduct(id);
    }

    @GetMapping("/api/v1/orders")
    public Map<String, Object> listOrders() {
        downstream.enforceGatewayPolicies();
        return downstream.getOrders();
    }

    @GetMapping("/api/v1/orders/{id}")
    public Map<String, Object> getOrderDetail(@PathVariable String id) {
        downstream.enforceGatewayPolicies();
        Map<String, Object> order = downstream.getOrder(id);
        Map<String, Object> payment = downstream.getPaymentByOrder(id);
        return Map.of("order", order, "payment", payment);
    }

    @PostMapping("/api/v1/checkout")
    public Map<String, Object> checkout(@RequestBody Map<String, Object> request) {
        downstream.enforceGatewayPolicies();
        long productId = request.get("productId") instanceof Number n ? n.longValue() : 1L;
        Map<String, Object> product = downstream.getCatalogProduct(productId);
        Map<String, Object> order = downstream.createOrder(request);
        return Map.of("checkout", "CONFIRMED", "product", product, "order", order);
    }

    @GetMapping("/api/v1/notifications")
    public Map<String, Object> listNotifications() {
        downstream.enforceGatewayPolicies();
        return downstream.getNotifications();
    }

    @GetMapping("/api/v1/graphql/storefront")
    public Map<String, Object> graphqlStorefrontProbe() {
        downstream.enforceGatewayPolicies();
        return Map.of("data", Map.of("storefrontOverview", storefrontOverview()));
    }

    @GetMapping(value = "/api/v1/stream/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents() {
        SseEmitter emitter = new SseEmitter(5_000L);
        try {
            Map<String, Object> summary = downstream.getAnalyticsSummary();
            emitter.send(SseEmitter.event()
                    .name("storefront-pulse")
                    .data(Map.of("gatewayVersion", version, "analytics", summary, "at", Instant.now().toString())));
            emitter.complete();
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
        return emitter;
    }

    // --- GraphQL Query Resolvers (POST /graphql) ---
    @QueryMapping
    public Map<String, Object> storefrontOverview() {
        Map<String, Object> products = downstream.getCatalogProducts(null);
        Map<String, Object> orders = downstream.getOrders();
        Map<String, Object> analytics = downstream.getAnalyticsSummary();
        boolean degraded = Boolean.TRUE.equals(products.get("fallback"))
                || Boolean.TRUE.equals(orders.get("fallback"))
                || Boolean.TRUE.equals(analytics.get("fallback"));
        return Map.of(
                "gatewayVersion", version,
                "productCount", asInt(products.get("total")),
                "orderCount", asInt(orders.get("total")),
                "notificationCount", asInt(analytics.get("totalNotifications")),
                "circuitBreakerEnabled", downstream.isCircuitBreakerEnabled(),
                "degraded", degraded
        );
    }

    @QueryMapping
    public Map<String, Object> productById(@Argument String id) {
        return downstream.getCatalogProduct(Long.parseLong(id));
    }

    @SuppressWarnings("unchecked")
    @QueryMapping
    public List<Map<String, Object>> recentOrders() {
        Object items = downstream.getOrders().get("items");
        return items instanceof List<?> list ? (List<Map<String, Object>>) list : List.of();
    }

    @ExceptionHandler(DownstreamClient.RateLimitedException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimit(DownstreamClient.RateLimitedException ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "RATE_LIMITED", "message", ex.getMessage()));
    }

    @ExceptionHandler(DownstreamClient.DownstreamUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleUnavailable(DownstreamClient.DownstreamUnavailableException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "DOWNSTREAM_UNAVAILABLE", "target", ex.getTarget(), "message", ex.getMessage()));
    }

    private static int asInt(Object val) {
        return val instanceof Number n ? n.intValue() : 0;
    }
}
