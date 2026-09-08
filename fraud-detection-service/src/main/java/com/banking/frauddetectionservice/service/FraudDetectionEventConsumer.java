package com.banking.frauddetectionservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor

public class FraudDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;
    private final ObjectMapper objectMapper;

    /*

       Listen to transaction Initiated topic
       every transaction goes through fraud check before completing


     */

    @KafkaListener(
            topics = "transaction.initiated",
            groupId = "fraud-detection-group"
    )
    public void consumeTransactionInitiated(@Payload String payload) {

        log.info("🔥 TRANSACTION.INITIATED RECEIVED: {}", payload);

        log.info("Received transaction.initiated event: {}", payload);

        try {
            Map<String, Object> transaction =
                    objectMapper.readValue(
                            payload,
                            new TypeReference<Map<String, Object>>() {}
                    );

            log.info(
                    "Received transaction for fraud check: {}",
                    transaction.get("transactionId")
            );

            fraudDetectionService.checkTransaction(transaction);

        } catch (Exception e) {
            log.error("Error processing transaction for fraud check", e);
            throw new RuntimeException(e);
        }
    }

}
