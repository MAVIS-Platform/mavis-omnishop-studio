package com.omnishop.notifications;

import org.springframework.amqp.core.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class NotificationWorkerApplication {
    public static final String EXCHANGE_NAME = "omnishop.events";
    public static final String ORDERS_QUEUE = "omnishop.notifications.orders";
    public static final String PAYMENTS_QUEUE = "omnishop.notifications.payments";

    public static void main(String[] args) {
        SpringApplication.run(NotificationWorkerApplication.class, args);
    }

    @Bean
    TopicExchange omnishopExchange() {
        return new TopicExchange(EXCHANGE_NAME, true, false);
    }

    @Bean
    Queue ordersQueue() {
        return new Queue(ORDERS_QUEUE, true);
    }

    @Bean
    Queue paymentsQueue() {
        return new Queue(PAYMENTS_QUEUE, true);
    }

    @Bean
    Binding ordersBinding(Queue ordersQueue, TopicExchange omnishopExchange) {
        return BindingBuilder.bind(ordersQueue).to(omnishopExchange).with("order.#");
    }

    @Bean
    Binding paymentsBinding(Queue paymentsQueue, TopicExchange omnishopExchange) {
        return BindingBuilder.bind(paymentsQueue).to(omnishopExchange).with("payment.#");
    }
}
