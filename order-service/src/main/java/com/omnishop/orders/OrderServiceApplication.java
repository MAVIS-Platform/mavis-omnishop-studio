package com.omnishop.orders;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@SpringBootApplication
public class OrderServiceApplication {
    public static final String EXCHANGE_NAME = "omnishop.events";
    public static final String KAFKA_TOPIC = "order-events";

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    @Bean
    RestClient restClient() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(2));
        factory.setReadTimeout(Duration.ofSeconds(3));
        return RestClient.builder().requestFactory(factory).build();
    }

    @Bean
    TopicExchange omnishopExchange() {
        return new TopicExchange(EXCHANGE_NAME, true, false);
    }

    @Bean(destroyMethod = "shutdownNow")
    ManagedChannel paymentGrpcChannel(
            @Value("${omnishop.payment-grpc-host:payment-service}") String host,
            @Value("${omnishop.payment-grpc-port:9090}") int port) {
        return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
}
