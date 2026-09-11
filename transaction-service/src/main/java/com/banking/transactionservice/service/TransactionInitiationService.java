package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.OutboxEvent;
import com.banking.transactionservice.entity.OutboxEventStatus;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.OutboxEventRepository;
import com.banking.transactionservice.repository.TransactionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionInitiationService {

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Transaction markProcessingAndCreateOutbox(
            Transaction transaction) {

        // 1. Transaction status -> PROCESSING
        transaction.setStatus(TransactionStatus.PROCESSING);

        Transaction savedTransaction =
                transactionRepository.save(transaction);

        // 2. Create transaction.initiated event
        Map<String, Object> event = new HashMap<>();

        event.put(
                "transactionId",
                savedTransaction.getId()
        );

        event.put(
                "senderAccountNumber",
                savedTransaction.getSenderAccountNumber()
        );

        event.put(
                "receiverAccountNumber",
                savedTransaction.getReceiverAccountNumber()
        );

        event.put(
                "amount",
                savedTransaction.getAmount()
        );

        event.put(
                "type",
                savedTransaction.getType()
        );

        event.put(
                "description",
                savedTransaction.getDescription()
        );

        // 3. Convert event to JSON
        String payload;

        try {

            payload = objectMapper.writeValueAsString(event);

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to serialize transaction initiated event",
                    e
            );
        }

        // 4. Create Outbox Event
        OutboxEvent outboxEvent =
                OutboxEvent.builder()
                        .aggregateId(
                                savedTransaction.getId()
                        )
                        .eventType(
                                "TRANSACTION_INITIATED"
                        )
                        .payload(payload)
                        .status(
                                OutboxEventStatus.PENDING
                        )
                        .build();

        // 5. Save Outbox Event
        outboxEventRepository.save(outboxEvent);

        log.info(
                "Transaction marked PROCESSING and outbox event created: transactionId={}",
                savedTransaction.getId()
        );

        return savedTransaction;
    }
}