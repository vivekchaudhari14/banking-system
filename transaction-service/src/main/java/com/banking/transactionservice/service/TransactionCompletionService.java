package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.OutboxEvent;
import com.banking.transactionservice.entity.OutboxEventStatus;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.event.TransactionCompletedEvent;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionCompletionService {

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Transaction completeTransaction(Transaction transaction) {

        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());

        Transaction savedTransaction =
                transactionRepository.save(transaction);

        try {

            TransactionCompletedEvent event =
                    new TransactionCompletedEvent(
                            savedTransaction.getId(),
                            savedTransaction.getSenderAccountNumber(),
                            savedTransaction.getReceiverAccountNumber(),
                            savedTransaction.getAmount(),
                            savedTransaction.getDescription()
                    );

            String payload =
                    objectMapper.writeValueAsString(event);

            OutboxEvent outboxEvent =
                    OutboxEvent.builder()
                            .aggregateId(savedTransaction.getId())
                            .eventType("transaction.completed")
                            .payload(payload)
                            .status(OutboxEventStatus.PENDING)
                            .createdAt(Instant.now())
                            .build();

            outboxEventRepository.save(outboxEvent);

            log.info(
                    "Transaction completed and outbox event created. transactionId={}",
                    savedTransaction.getId()
            );

            return savedTransaction;

        } catch (Exception e) {

            log.error(
                    "Failed to create outbox event. transactionId={}",
                    savedTransaction.getId(),
                    e
            );

            throw new RuntimeException(
                    "Failed to create transaction completed event",
                    e
            );
        }
    }
}