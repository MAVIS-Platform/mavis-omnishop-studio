package com.omnishop.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class CatalogController implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(CatalogController.class);

    private final ProductRepository repository;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final String version;
    private final boolean cacheEnabled;
    private final long cacheTtlSeconds;
    private final int artificialLatencyMs;
    private final int faultErrorRatePct;

    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();

    public CatalogController(
            ProductRepository repository,
            StringRedisTemplate redis,
            ObjectMapper objectMapper,
            @Value("${omnishop.version:v1}") String version,
            @Value("${omnishop.cache-enabled:true}") boolean cacheEnabled,
            @Value("${omnishop.cache-ttl-seconds:30}") long cacheTtlSeconds,
            @Value("${omnishop.artificial-latency-ms:0}") int artificialLatencyMs,
            @Value("${omnishop.fault-error-rate-pct:0}") int faultErrorRatePct) {
        this.repository = repository;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.version = version;
        this.cacheEnabled = cacheEnabled;
        this.cacheTtlSeconds = Math.max(5, cacheTtlSeconds);
        this.artificialLatencyMs = Math.max(0, Math.min(3000, artificialLatencyMs));
        this.faultErrorRatePct = Math.max(0, Math.min(100, faultErrorRatePct));
    }

    @Override
    public void run(String... args) {
        try {
            if (repository.count() == 0) {
                repository.saveAll(List.of(
                        new Product("SKU-MBP16", "MacBook Pro 16 M4", "laptops", new BigDecimal("2499.00"), 42),
                        new Product("SKU-IP16P", "iPhone 16 Pro 256GB", "phones", new BigDecimal("1199.00"), 120),
                        new Product("SKU-WH1000", "Sony WH-1000XM6 ANC", "audio", new BigDecimal("399.00"), 85),
                        new Product("SKU-KEYCHR", "Keychron Q1 Pro Mechanical", "accessories", new BigDecimal("199.00"), 64),
                        new Product("SKU-DELL4K", "Dell UltraSharp 27 4K", "monitors", new BigDecimal("649.00"), 31)
                ));
                log.info("Seeded 5 default catalog products into PostgreSQL");
            }
        } catch (Exception ex) {
            log.warn("Deferred catalog seed (DB warming up): {}", ex.getMessage());
        }
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "UP",
                "service", "catalog-service",
                "version", version,
                "cacheEnabled", cacheEnabled,
                "cacheHits", cacheHits.get(),
                "cacheMisses", cacheMisses.get(),
                "timestamp", Instant.now().toString()
        );
    }

    @GetMapping("/api/products")
    public Map<String, Object> list(@RequestParam(required = false) String category,
                                    @RequestParam(required = false) String q) {
        applyRuntimeFaults();
        List<Product> items;
        if (category != null && !category.isBlank()) {
            items = repository.findByCategoryIgnoreCase(category);
        } else if (q != null && !q.isBlank()) {
            items = repository.findByNameContainingIgnoreCase(q);
        } else {
            items = repository.findAll();
        }
        return Map.of(
                "items", items.stream().map(p -> toDto(p, false)).toList(),
                "total", items.size(),
                "cacheEnabled", cacheEnabled,
                "cacheHits", cacheHits.get(),
                "cacheMisses", cacheMisses.get()
        );
    }

    @GetMapping("/api/products/{id}")
    public ResponseEntity<Map<String, Object>> getById(@PathVariable Long id) {
        applyRuntimeFaults();
        String cacheKey = "omnishop:product:" + id;
        if (cacheEnabled) {
            try {
                String cached = redis.opsForValue().get(cacheKey);
                if (cached != null) {
                    cacheHits.incrementAndGet();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> dto = objectMapper.readValue(cached, Map.class);
                    dto.put("cacheHit", true);
                    return ResponseEntity.ok(dto);
                }
            } catch (Exception ex) {
                log.warn("Redis read fallback to PostgreSQL for key={}: {}", cacheKey, ex.getMessage());
            }
        }
        cacheMisses.incrementAndGet();
        Optional<Product> found = repository.findById(id);
        if (found.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "PRODUCT_NOT_FOUND", "id", id));
        }
        Map<String, Object> dto = toDto(found.get(), false);
        if (cacheEnabled) {
            try {
                redis.opsForValue().set(cacheKey, objectMapper.writeValueAsString(dto), Duration.ofSeconds(cacheTtlSeconds));
            } catch (Exception ex) {
                log.warn("Redis write skipped for key={}: {}", cacheKey, ex.getMessage());
            }
        }
        return ResponseEntity.ok(dto);
    }

    @PostMapping("/api/products")
    public ResponseEntity<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        applyRuntimeFaults();
        String sku = String.valueOf(body.getOrDefault("sku", "SKU-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase()));
        String name = String.valueOf(body.getOrDefault("name", "Unnamed Product"));
        String category = String.valueOf(body.getOrDefault("category", "general"));
        BigDecimal price = new BigDecimal(String.valueOf(body.getOrDefault("price", "99.00")));
        int stock = body.get("stock") instanceof Number n ? n.intValue() : 10;
        Product saved = repository.save(new Product(sku, name, category, price, stock));
        return ResponseEntity.status(HttpStatus.CREATED).body(toDto(saved, false));
    }

    @PutMapping("/api/products/{id}")
    public ResponseEntity<Map<String, Object>> update(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        applyRuntimeFaults();
        return repository.findById(id).map(p -> {
            if (body.containsKey("name")) p.setName(String.valueOf(body.get("name")));
            if (body.containsKey("category")) p.setCategory(String.valueOf(body.get("category")));
            if (body.containsKey("price")) p.setPrice(new BigDecimal(String.valueOf(body.get("price"))));
            if (body.get("stock") instanceof Number n) p.setStock(n.intValue());
            Product saved = repository.save(p);
            evictCache(id);
            return ResponseEntity.ok(toDto(saved, false));
        }).orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "PRODUCT_NOT_FOUND")));
    }

    @PatchMapping("/api/products/{id}/stock")
    public ResponseEntity<Map<String, Object>> adjustStock(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        applyRuntimeFaults();
        int delta = body.get("delta") instanceof Number n ? n.intValue() : -1;
        return repository.findById(id).map(p -> {
            p.setStock(Math.max(0, p.getStock() + delta));
            Product saved = repository.save(p);
            evictCache(id);
            return ResponseEntity.ok(toDto(saved, false));
        }).orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "PRODUCT_NOT_FOUND")));
    }

    @DeleteMapping("/api/products/{id}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable Long id) {
        applyRuntimeFaults();
        if (!repository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "PRODUCT_NOT_FOUND"));
        }
        repository.deleteById(id);
        evictCache(id);
        return ResponseEntity.ok(Map.of("deleted", true, "id", id));
    }

    private void evictCache(Long id) {
        try {
            redis.delete("omnishop:product:" + id);
        } catch (Exception ignored) {}
    }

    private void applyRuntimeFaults() {
        if (artificialLatencyMs > 0) {
            try { Thread.sleep(artificialLatencyMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (faultErrorRatePct > 0 && ThreadLocalRandom.current().nextInt(100) < faultErrorRatePct) {
            throw new IllegalStateException("Injected catalog fault (" + faultErrorRatePct + "%)");
        }
    }

    private static Map<String, Object> toDto(Product p, boolean cacheHit) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", p.getId());
        m.put("sku", p.getSku());
        m.put("name", p.getName());
        m.put("category", p.getCategory());
        m.put("price", p.getPrice().doubleValue());
        m.put("stock", p.getStock());
        m.put("cacheHit", cacheHit);
        return m;
    }
}
