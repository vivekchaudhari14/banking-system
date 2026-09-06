package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.OutboxEvent;
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
                outboxEventRepository
                        .findTop100ByStatusOrderByCreatedAtAsc(
                                "PENDING"
                        );

        for (OutboxEvent event : events) {

            try {

                kafkaTemplate
                        .send(
                                event.getEventType(),
                                event.getAggregateId(),
                                event.getPayload()
                        )
                        .get();

                event.setStatus("PUBLISHED");
                event.setPublishedAt(Instant.now());

                outboxEventRepository.save(event);

                log.info(
                        "Outbox event published successfully. eventId={}, type={}",
                        event.getId(),
                        event.getEventType()
                );

            } catch (Exception e) {

                log.error(
                        "Failed to publish outbox event. eventId={}",
                        event.getId(),
                        e
                );
            }
        }
    }
}