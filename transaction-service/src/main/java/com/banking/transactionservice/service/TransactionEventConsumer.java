package com.banking.transactionservice.kafka;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import com.banking.transactionservice.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionService transactionService;

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

            Transaction transaction =
                    transactionRepository.findById(transactionId)
                            .orElseThrow(() ->
                                    new RuntimeException(
                                            "Transaction not found: " + transactionId
                                    )
                            );

            /*
             * Important:
             *
             * If message is retried after OTP was already generated,
             * don't generate another OTP.
             */
            if (transaction.getStatus() != TransactionStatus.PROCESSING) {

                log.info(
                        "Transaction {} is already in status {}. Skipping verification event.",
                        transactionId,
                        transaction.getStatus()
                );

                return;
            }

            // Generate secure 6 digit OTP
            String otp =
                    String.valueOf(
                            100000 + secureRandom.nextInt(900000)
                    );

            // Redis keys
            String otpKey =
                    "transaction:otp:" + transactionId;

            String attemptKey =
                    "transaction:otp:attempts:" + transactionId;

            // Store OTP for 5 minutes
            redisTemplate.opsForValue()
                    .set(
                            otpKey,
                            otp,
                            Duration.ofMinutes(5)
                    );

            // Store OTP attempts
            redisTemplate.opsForValue()
                    .set(
                            attemptKey,
                            "0",
                            Duration.ofMinutes(5)
                    );

            // Update transaction status
            transaction.setStatus(
                    TransactionStatus.PENDING_VERIFICATION
            );

            transaction.setFailureReason(reason);

            transactionRepository.save(transaction);

            // Publish OTP generated event
            Map<String, Object> otpEvent = Map.of(
                    "transactionId", transactionId,
                    "accountNumber", accountNumber,
                    "otp", otp
            );

            kafkaTemplate.send(
                    "transaction.otp.generated",
                    otpEvent
            );

            log.info(
                    "OTP generated successfully for transactionId={}",
                    transactionId
            );

        } catch (Exception e) {

            log.error(
                    "Error processing verification.required event",
                    e
            );

            /*
             * Very important.
             *
             * Don't swallow exception.
             * Kafka retry/error-handler can handle it.
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