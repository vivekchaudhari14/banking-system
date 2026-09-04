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
            Map<String, Object> event
    ) {

        String transactionId =
                (String) event.get("transactionId");

        String receiverAccountNumber =
                (String) event.get("receiverAccountNumber");

        BigDecimal amount =
                new BigDecimal(event.get("amount").toString());

        log.info(
                "Received transaction.completed: transactionId={}, receiver={}, amount={}",
                transactionId,
                receiverAccountNumber,
                amount
        );

        try {

            accountService.creditBalance(
                    receiverAccountNumber,
                    amount,
                    transactionId
            );

            log.info(
                    "Receiver credited successfully. transactionId={}",
                    transactionId
            );

        } catch (Exception e) {

            log.error(
                    "Receiver credit failed. transactionId={}",
                    transactionId,
                    e
            );

            // IMPORTANT:
            // Exception rethrow करायचा
            throw e;
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - blocking  account :{}", accountNumber);
            accountService.blockAccount(accountNumber);
        }
        catch (Exception e){
            log.error("error blocking account : {} ", e.getMessage());
        }
    }

}
