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

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String,Object> payload ){
        try{
            String accountNumber = (String) payload.get("receiveraccountNumber");
            BigDecimal amount = new BigDecimal((String) payload.get("amount"));

            log.info("Crediting account :{} amount {}", receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);


        } catch (Exception e) {
            log.error("error crediting account :", e.getMessage());
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
