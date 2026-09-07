package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionCompensationService {

    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public void compensateTransaction(
            Transaction transaction,
            String reason) {

        log.warn(
                "SAGA COMPENSATION START - transaction: {} sender: {} amount: {}",
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getAmount()
        );

        try {

            // Step 1: Refund sender
            accountServiceClient.refundBalance(
                    transaction.getSenderAccountNumber(),
                    transaction.getAmount(),
                    transaction.getId() + ":REFUND"
            );

            log.info(
                    "SAGA COMPENSATION - {} refunded to {}",
                    transaction.getAmount(),
                    transaction.getSenderAccountNumber()
            );

            // Step 2: Mark transaction failed
            transaction.setStatus(TransactionStatus.FAILED);
            transaction.setFailureReason(
                    reason + " - Amount refunded"
            );

            transactionRepository.save(transaction);

            // Step 3: Publish refund event
            Map<String, Object> refundEvent = new HashMap<>();

            refundEvent.put("transactionId", transaction.getId());
            refundEvent.put(
                    "senderAccountNumber",
                    transaction.getSenderAccountNumber()
            );
            refundEvent.put("amount", transaction.getAmount());
            refundEvent.put("reason", reason);

            kafkaTemplate.send(
                    TRANSACTION_REFUNDED_TOPIC,
                    transaction.getId(),
                    refundEvent
            ).get();

        } catch (Exception e) {

            log.error(
                    "SAGA COMPENSATION FAILED - transaction: {}",
                    transaction.getId(),
                    e
            );

            transaction.setStatus(TransactionStatus.FLAGGED);

            transaction.setFailureReason(
                    reason +
                            " - COMPENSATION FAILED. Manual reconciliation required."
            );

            transactionRepository.save(transaction);

            throw new RuntimeException(
                    "Transaction compensation failed",
                    e
            );
        }
    }
}