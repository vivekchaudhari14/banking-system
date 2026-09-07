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
            groupId = "account-service-group"
    )
    public void handleTransactionCompleted(
            Map<String, Object> event) {

        String transactionId =
                String.valueOf(event.get("transactionId"));

        String receiverAccountNumber =
                String.valueOf(event.get("receiverAccountNumber"));

        BigDecimal amount =
                new BigDecimal(event.get("amount").toString());

        accountService.creditBalance(
                receiverAccountNumber,
                amount,
                transactionId
        );
    }

    @KafkaListener(
            topics = "fraud.detected",
            groupId = "account-service-group"
    )
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
