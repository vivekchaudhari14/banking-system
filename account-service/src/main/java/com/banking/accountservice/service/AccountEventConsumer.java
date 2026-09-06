package com.banking.accountservice.service;

import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor

public class AccountEventConsumer {

    private final AccountService accountService;

    /*
        consume transaction completed event from kafka
        credits receiver account
        @param payload

     */

    @KafkaListener(
            topics = "transaction.completed",
            groupId = "account-service"
    )
    public void handleTransactionCompleted(
            TransactionCompletedEvent event) {

        try {

            accountService.creditBalance(
                    event.getReceiverAccountNumber(),
                    event.getAmount(),
                    event.getTransactionId()
            );

            log.info(
                    "Transaction {} completed. Receiver {} credited.",
                    event.getTransactionId(),
                    event.getReceiverAccountNumber()
            );

        } catch (Exception e) {

            log.error(
                    "Receiver credit failed for transaction {}",
                    event.getTransactionId(),
                    e
            );

            throw e;
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload
    ) {

        String accountNumber =
                (String) payload.get("accountNumber");

        try {

            log.info(
                    "Fraud detected - blocking account: {}",
                    accountNumber
            );

            accountService.blockAccount(accountNumber);

            log.info(
                    "Account blocked successfully: {}",
                    accountNumber
            );

        } catch (Exception e) {

            log.error(
                    "Error blocking account. accountNumber={}",
                    accountNumber,
                    e
            );

            // Kafka Retry / DLT साठी
            throw e;
        }
    }

}
