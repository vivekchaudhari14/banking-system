package com.banking.paymentservice.service;

import com.banking.paymentservice.entity.EventStatus;
import com.banking.paymentservice.entity.PaymentOutboxEvent;
import com.banking.paymentservice.repository.PaymentOutboxEventRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentOutboxEventPublisher {

    private final PaymentOutboxEventRepository outboxEventRepository;

    private final KafkaTemplate<String, String> kafkaTemplate;


    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<PaymentOutboxEvent> events =
                outboxEventRepository
                        .findTop100ByStatusOrderByCreatedAtAsc(
                                EventStatus.PENDING
                        );

        for (PaymentOutboxEvent event : events) {

            try {

                kafkaTemplate
                        .send(
                                event.getEventType(),
                                event.getAggregateId(),
                                event.getPayload()
                        )
                        .get();

                event.setStatus(
                        EventStatus.PUBLISHED
                );

                outboxEventRepository.save(event);

                log.info(
                        "Payment outbox event published successfully. " +
                                "eventId={}, eventType={}, aggregateId={}",
                        event.getId(),
                        event.getEventType(),
                        event.getAggregateId()
                );

            } catch (Exception e) {

                log.error(
                        "Failed to publish payment outbox event. " +
                                "eventId={}, eventType={}, aggregateId={}",
                        event.getId(),
                        event.getEventType(),
                        event.getAggregateId(),
                        e
                );

                // Keep PENDING.
                // Next scheduler execution will retry.
            }
        }
    }
}