package com.example.order_service.service;

import com.example.bookingservice.event.BookingEvent;
import com.example.order_service.client.InventoryServiceClient;
import com.example.order_service.model.Order;
import com.example.order_service.repository.OrderRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderService {

    private OrderRepository orderRepository;
    private InventoryServiceClient inventoryServiceClient;

    @Autowired
    public OrderService(OrderRepository orderRepository, InventoryServiceClient inventoryServiceClient) {
        this.orderRepository = orderRepository;
        this.inventoryServiceClient = inventoryServiceClient;
    }

    @KafkaListener(topics = "booking", groupId = "order-service")
    public void orderEvent(BookingEvent bookingEvent) {
        log.info("Received order event: {}",bookingEvent);

        Order order = createOrder(bookingEvent);
        orderRepository.saveAndFlush(order);

        inventoryServiceClient.updateInventory(order.getEventId(), order.getTicketCount());
        log.info("Inventory updated for event: {}, remaining tickets: {}", order.getEventId(), order.getTicketCount());
    }


    private Order createOrder(BookingEvent event) {
        return Order.builder()
                .customerId(event.getUserId())
                .eventId(event.getEventId())
                .ticketCount(event.getTicketCount())
                .totalPrice(event.getTotalPrice())
                .build();
    }
}
