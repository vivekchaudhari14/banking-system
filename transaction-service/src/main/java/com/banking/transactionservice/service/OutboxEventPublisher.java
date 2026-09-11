package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.OutboxEvent;
import com.banking.transactionservice.entity.OutboxEventStatus;
import com.banking.transactionservice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxEventPublisher {

    private final OutboxEventRepository outboxEventRepository;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository.findTop100ByStatusOrderByCreatedAtAsc(
                        OutboxEventStatus.PENDING);

        for (OutboxEvent event : events) {

            try {

                // 1. Publish event to Kafka
                kafkaTemplate
                        .send(
                                "transaction.initiated",
                                event.getAggregateId(),
                                event.getPayload()
                        )
                        .get();

                // 2. Kafka publish successful
                event.setStatus(OutboxEventStatus.PUBLISHED);

                // 3. Set published timestamp
                event.setPublishedAt(Instant.now());

                // 4. Mark event as published in DB
                outboxEventRepository.save(event);

                log.info(
                        "Outbox event published successfully. eventId={}, eventType={}, aggregateId={}",
                        event.getId(),
                        event.getEventType(),
                        event.getAggregateId()
                );

            } catch (Exception e) {

                // Keep status as PENDING.
                // Scheduler will retry this event in the next execution.
                log.error(
                        "Failed to publish outbox event. eventId={}, eventType={}, aggregateId={}",
                        event.getId(),
                        event.getEventType(),
                        event.getAggregateId(),
                        e
                );
            }
        }
    }
}