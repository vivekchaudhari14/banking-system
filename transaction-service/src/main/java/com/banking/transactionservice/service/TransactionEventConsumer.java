package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import com.banking.transactionservice.service.TransactionService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionService transactionService;
    private final ObjectMapper objectMapper;

    private final SecureRandom secureRandom = new SecureRandom();

    // =========================================================
    // 1. FRAUD / VERIFICATION REQUIRED
    // =========================================================

    @KafkaListener(
            topics = "verification.required",
            groupId = "transaction-service"
    )
    public void handleVerificationRequired(Map<String, Object> event) {

        try {

            // =====================================================
            // 1. Extract event data
            // =====================================================

            String transactionId =
                    (String) event.get("transactionId");

            String accountNumber =
                    (String) event.get("accountNumber");

            String reason =
                    (String) event.get("reason");

            log.info(
                    "Verification required for transactionId={}, accountNumber={}, reason={}",
                    transactionId,
                    accountNumber,
                    reason
            );

            // =====================================================
            // 2. Find transaction
            // =====================================================

            Transaction transaction =
                    transactionRepository.findById(transactionId)
                            .orElseThrow(() ->
                                    new RuntimeException(
                                            "Transaction not found: " + transactionId
                                    )
                            );

            // =====================================================
            // 3. Prevent duplicate OTP generation
            // =====================================================

            if (transaction.getStatus() != TransactionStatus.PROCESSING) {

                log.info(
                        "Transaction {} is already in status {}. Skipping verification event.",
                        transactionId,
                        transaction.getStatus()
                );

                return;
            }

            // =====================================================
            // 4. Generate secure 6 digit OTP
            // =====================================================

            String otp =
                    String.valueOf(
                            100000 + secureRandom.nextInt(900000)
                    );

            log.info(
                    "OTP generated for transactionId={}",
                    transactionId
            );

            // =====================================================
            // 5. Redis keys
            // =====================================================

            String otpKey =
                    "transaction:otp:" + transactionId;

            String attemptKey =
                    "transaction:otp:attempts:" + transactionId;

            // =====================================================
            // 6. Store OTP in Redis for 5 minutes
            // =====================================================

            redisTemplate.opsForValue()
                    .set(
                            otpKey,
                            otp,
                            Duration.ofMinutes(5)
                    );

            // =====================================================
            // 7. Store OTP attempts
            // =====================================================

            redisTemplate.opsForValue()
                    .set(
                            attemptKey,
                            "0",
                            Duration.ofMinutes(5)
                    );

            // =====================================================
            // 8. Update transaction status
            // =====================================================

            transaction.setStatus(
                    TransactionStatus.PENDING_VERIFICATION
            );

            transaction.setFailureReason(reason);

            transactionRepository.save(transaction);

            // =====================================================
            // 9. Create OTP generated event
            // =====================================================

            Map<String, Object> otpEvent =
                    new HashMap<>();

            otpEvent.put(
                    "transactionId",
                    transactionId
            );

            otpEvent.put(
                    "accountNumber",
                    accountNumber
            );

            otpEvent.put(
                    "otp",
                    otp
            );

            otpEvent.put(
                    "amount",
                    transaction.getAmount()
            );

            otpEvent.put(
                    "reason",
                    reason
            );

            // =====================================================
            // 10. Convert HashMap → JSON String
            // =====================================================

            String otpEventJson;

            try {

                otpEventJson =
                        objectMapper.writeValueAsString(otpEvent);

            } catch (JsonProcessingException e) {

                log.error(
                        "Failed to serialize OTP event for transactionId={}",
                        transactionId,
                        e
                );

                throw new RuntimeException(
                        "Failed to serialize OTP event",
                        e
                );
            }

            // =====================================================
            // 11. Publish OTP event to Kafka
            // =====================================================

            kafkaTemplate.send(
                    "transaction.otp.generated",
                    transactionId,
                    otpEventJson
            );

            log.info(
                    "OTP generated event published successfully for transactionId={}",
                    transactionId
            );

        } catch (Exception e) {

            log.error(
                    "Error processing verification.required event",
                    e
            );

            /*
             * Don't swallow exception.
             * Kafka error handler can handle the exception.
             */
            throw e;
        }
    }


    // =========================================================
    // 2. FRAUD CLEAN RESULT
    // =========================================================

    @KafkaListener(
            topics = "fraud.check.clean",
            groupId = "transaction-service"
    )
    public void handleFraudCleanResult(Map<String, Object> event) {

        try {

            String transactionId =
                    (String) event.get("transactionId");

            log.info(
                    "Fraud clean result received for transactionId={}",
                    transactionId
            );

            transactionService.processCleanResult(transactionId);

            log.info(
                    "Fraud clean result processed successfully for transactionId={}",
                    transactionId
            );

        } catch (Exception e) {

            log.error(
                    "Error processing fraud.check.clean event",
                    e
            );

            /*
             * Don't swallow the exception.
             * This allows Kafka retry/error handling.
             */
            throw e;
        }
    }
}