package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private static final String VERIFIACTION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    public void checkTransaction(Map<String, Object> payload) {

        log.info("🔥 FRAUD CHECK STARTED");

        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        // fetch real balance from account Service

        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);
        log.info("Checking transaction: {} account: {} amount: {} balance: {}",
                transactionId, accountNumber,amount,senderBalance);

        FraudCheckResult result = performFraudChecks(accountNumber,amount,senderBalance);

        if(result.isFraud()) {
            log.info("Suspicious activity detected - account: {} " +
                            "reason: {} - requesting OTP verification",
                    accountNumber,result.getReason());
            Map<String, Object> verifiactionEvent  = new HashMap<>();
            verifiactionEvent.put("transactionId", transactionId);
            verifiactionEvent.put("accountNumber", accountNumber);
            verifiactionEvent.put("amount", amount);
            verifiactionEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFIACTION_REQUIRED_TOPIC,transactionId, verifiactionEvent);

        }else  {
            log.info("Transaction clean");

            Map<String, Object> transactionCleanEvent  = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC,transactionId, transactionCleanEvent);

        }

    }

    private FraudCheckResult performFraudChecks (
            String accountNumber,
            BigDecimal amount,
            BigDecimal senderBalance ) {
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(
                    true,"To many transaction in 60 seconds"+
                    "- velocity limit exceeded" );
        }

        if(isAmountSuspicious(accountNumber,amount)) {
            return new FraudCheckResult(
                    true, "Unusual transaction amount "+
                    "- exceeds 3x your average" );
        }
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBalance,amount)) {
            return new FraudCheckResult(
                    true, "Transaction exceed 90% of account balance" );
        }
        return new FraudCheckResult(false,null);
    }

    private boolean isVelocityExceeded(String accountNumber) {

        String key = "fraud:velocity:" + accountNumber;

        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity Check - account: {} count: {}/{}",
                accountNumber, count,maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;

    }

    private boolean isAmountSuspicious(
            String accountNumber,
            BigDecimal amount) {

        String totalKey =
                "fraud:avg_amount:" + accountNumber + ":total";

        String countKey =
                "fraud:avg_amount:" + accountNumber + ":count";

        String totalStr =
                redisTemplate.opsForValue().get(totalKey);

        String countStr =
                redisTemplate.opsForValue().get(countKey);

        // First transaction
        if (totalStr == null || countStr == null) {

            redisTemplate.opsForValue()
                    .set(totalKey, amount.toString());

            redisTemplate.opsForValue()
                    .set(countKey, "1");

            log.info(
                    "First transaction for account {}. Amount: {}",
                    accountNumber,
                    amount
            );

            return false;
        }

        BigDecimal totalAmount =
                new BigDecimal(totalStr);

        long transactionCount =
                Long.parseLong(countStr);

        // Calculate actual historical average
        BigDecimal avgAmount =
                totalAmount.divide(
                        BigDecimal.valueOf(transactionCount),
                        2,
                        RoundingMode.HALF_UP
                );

        // Calculate suspicious threshold
        BigDecimal threshold =
                avgAmount.multiply(
                        BigDecimal.valueOf(suspiciousAmountMultiplier)
                );

        boolean suspicious =
                amount.compareTo(threshold) > 0;

        log.info(
                "Amount Check - account: {}, amount: {}, average: {}, threshold: {}, suspicious: {}",
                accountNumber,
                amount,
                avgAmount,
                threshold,
                suspicious
        );

        // -------------------------------------------------
        // Update historical statistics
        // IMPORTANT:
        // Update AFTER checking current transaction
        // -------------------------------------------------

        BigDecimal newTotal =
                totalAmount.add(amount);

        long newCount =
                transactionCount + 1;

        redisTemplate.opsForValue()
                .set(
                        totalKey,
                        newTotal.toString()
                );

        redisTemplate.opsForValue()
                .set(
                        countKey,
                        String.valueOf(newCount)
                );

        return suspicious;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {

        BigDecimal maxAllowed = senderBalance.multiply(
                BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {} maxallowed: {} suspicious: {}",
                amount,maxAllowed,amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;

    }

}