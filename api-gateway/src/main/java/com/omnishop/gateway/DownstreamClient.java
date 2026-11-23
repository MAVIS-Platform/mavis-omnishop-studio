package com.omnishop.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Service
public class DownstreamClient {
    private static final Logger log = LoggerFactory.getLogger(DownstreamClient.class);

    private final RestClient restClient;
    private final String catalogUrl;
    private final String orderUrl;
    private final String paymentUrl;
    private final String notificationUrl;
    private final boolean circuitBreakerEnabled;
    private final int retryMaxAttempts;
    private final int artificialLatencyMs;
    private final int rateLimitRps;

    private final ConcurrentHashMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> circuitOpenUntilEpochMs = new ConcurrentHashMap<>();
    private final AtomicLong rateWindowEpochSec = new AtomicLong(0);
    private final AtomicInteger rateWindowCount = new AtomicInteger(0);

    public DownstreamClient(
            RestClient restClient,
            @Value("${omnishop.catalog-url}") String catalogUrl,
            @Value("${omnishop.order-url}") String orderUrl,
            @Value("${omnishop.payment-url}") String paymentUrl,
            @Value("${omnishop.notification-url}") String notificationUrl,
            @Value("${omnishop.resilience.circuit-breaker-enabled:false}") boolean circuitBreakerEnabled,
            @Value("${omnishop.resilience.retry-max-attempts:1}") int retryMaxAttempts,
            @Value("${omnishop.resilience.artificial-latency-ms:0}") int artificialLatencyMs,
            @Value("${omnishop.resilience.rate-limit-rps:100}") int rateLimitRps) {
        this.restClient = restClient;
        this.catalogUrl = catalogUrl;
        this.orderUrl = orderUrl;
        this.paymentUrl = paymentUrl;
        this.notificationUrl = notificationUrl;
        this.circuitBreakerEnabled = circuitBreakerEnabled;
        this.retryMaxAttempts = Math.max(0, Math.min(3, retryMaxAttempts));
        this.artificialLatencyMs = Math.max(0, Math.min(3000, artificialLatencyMs));
        this.rateLimitRps = Math.max(1, rateLimitRps);
    }

    public boolean isCircuitBreakerEnabled() { return circuitBreakerEnabled; }
    public int getRetryMaxAttempts() { return retryMaxAttempts; }
    public int getArtificialLatencyMs() { return artificialLatencyMs; }
    public int getRateLimitRps() { return rateLimitRps; }

    public void enforceGatewayPolicies() {
        long sec = Instant.now().getEpochSecond();
        if (rateWindowEpochSec.get() != sec) {
            rateWindowEpochSec.set(sec);
            rateWindowCount.set(0);
        }
        if (rateWindowCount.incrementAndGet() > rateLimitRps) {
            log.warn("[RESILIENCE] rate-limit exceeded limit={}rps", rateLimitRps);
            throw new RateLimitedException("Gateway rate limit exceeded: " + rateLimitRps + " rps");
        }
        if (artificialLatencyMs > 0) {
            try {
                Thread.sleep(artificialLatencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Map<String, Object> getCatalogProducts(String query) {
        String path = (query != null && !query.isBlank()) ? "/api/products?" + query : "/api/products";
        return callWithResilience("catalog-service",
                () -> getMap(catalogUrl + path),
                () -> Map.of("items", List.of(), "total", 0, "fallback", true, "source", "circuit-breaker"));
    }

    public Map<String, Object> getCatalogProduct(long id) {
        return callWithResilience("catalog-service",
                () -> getMap(catalogUrl + "/api/products/" + id),
                () -> Map.of("id", id, "name", "Fallback Product", "price", 0.0, "stock", 0, "cacheHit", false, "fallback", true));
    }

    public Map<String, Object> getOrders() {
        return callWithResilience("order-service",
                () -> getMap(orderUrl + "/api/orders"),
                () -> Map.of("items", List.of(), "total", 0, "fallback", true, "source", "circuit-breaker"));
    }

    public Map<String, Object> getOrder(String id) {
        return callWithResilience("order-service",
                () -> getMap(orderUrl + "/api/orders/" + id),
                () -> Map.of("id", id, "status", "FALLBACK_UNAVAILABLE", "fallback", true));
    }

    public Map<String, Object> triggerOrderLiveFlow() {
        return callWithResilience("order-service",
                () -> getMap(orderUrl + "/api/orders/live-flow"),
                () -> Map.of("status", "DEGRADED_FALLBACK", "fallback", true, "source", "circuit-breaker"));
    }

    public Map<String, Object> createOrder(Map<String, Object> body) {
        return callWithResilience("order-service",
                () -> postMap(orderUrl + "/api/orders", body),
                () -> Map.of("status", "QUEUED_FALLBACK", "fallback", true, "source", "circuit-breaker"));
    }

    public Map<String, Object> getPaymentByOrder(String orderId) {
        return callWithResilience("payment-service",
                () -> getMap(paymentUrl + "/api/payments/order/" + orderId),
                () -> Map.of("orderId", orderId, "status", "PENDING_RETRY", "fallback", true, "source", "circuit-breaker"));
    }

    public Map<String, Object> getNotifications() {
        return callWithResilience("notification-worker",
                () -> getMap(notificationUrl + "/api/notifications"),
                () -> Map.of("items", List.of(), "total", 0, "fallback", true));
    }

    public Map<String, Object> getAnalyticsSummary() {
        return callWithResilience("notification-worker",
                () -> getMap(notificationUrl + "/api/analytics/summary"),
                () -> Map.of("totalNotifications", 0, "totalKafkaEvents", 0, "fallback", true));
    }

    private Map<String, Object> callWithResilience(String target, Supplier<Map<String, Object>> call, Supplier<Map<String, Object>> fallback) {
        long now = System.currentTimeMillis();
        if (circuitBreakerEnabled && circuitOpenUntilEpochMs.getOrDefault(target, 0L) > now) {
            log.warn("[RESILIENCE] circuit-breaker=OPEN target={} returning fallback", target);
            return fallback.get();
        }
        RuntimeException lastError = null;
        int totalAttempts = 1 + retryMaxAttempts;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                Map<String, Object> res = call.get();
                consecutiveFailures.computeIfAbsent(target, k -> new AtomicInteger()).set(0);
                return res != null ? res : Map.of();
            } catch (RuntimeException ex) {
                lastError = ex;
                int fails = consecutiveFailures.computeIfAbsent(target, k -> new AtomicInteger()).incrementAndGet();
                if (attempt < totalAttempts) {
                    log.warn("[RESILIENCE] retry attempt={}/{} target={} err={}", attempt, retryMaxAttempts, target, ex.getClass().getSimpleName());
                } else if (circuitBreakerEnabled && fails >= 2) {
                    circuitOpenUntilEpochMs.put(target, System.currentTimeMillis() + 10_000L);
                    log.warn("[RESILIENCE] circuit-breaker=TRIPPED target={} failures={} returning fallback", target, fails);
                    return fallback.get();
                } else if (circuitBreakerEnabled) {
                    log.warn("[RESILIENCE] fallback activated target={} err={}", target, ex.getClass().getSimpleName());
                    return fallback.get();
                }
            }
        }
        throw new DownstreamUnavailableException(target, lastError);
    }

    private Map<String, Object> getMap(String url) {
        return restClient.get()
                .uri(url)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() {});
    }

    private Map<String, Object> postMap(String url, Map<String, Object> body) {
        return restClient.post()
                .uri(url)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() {});
    }

    public static class RateLimitedException extends RuntimeException {
        public RateLimitedException(String msg) { super(msg); }
    }

    public static class DownstreamUnavailableException extends RuntimeException {
        private final String target;
        public DownstreamUnavailableException(String target, Throwable cause) {
            super("Downstream service unavailable: " + target, cause);
            this.target = target;
        }
        public String getTarget() { return target; }
    }

    public Map<String, Object> activePolicySnapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("circuitBreakerEnabled", circuitBreakerEnabled);
        snap.put("retryMaxAttempts", retryMaxAttempts);
        snap.put("artificialLatencyMs", artificialLatencyMs);
        snap.put("rateLimitRps", rateLimitRps);
        return snap;
    }
}
