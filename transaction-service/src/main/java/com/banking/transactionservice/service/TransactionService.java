package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.AccountStatusResponse;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransaferRequest;
import com.banking.transactionservice.entity.*;
import com.banking.transactionservice.exception.customexcepation.InsufficientBalanceException;
import com.banking.transactionservice.exception.customexcepation.ResourceNotFoundException;
import com.banking.transactionservice.repository.TransactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.*;


@Service
@Slf4j
@RequiredArgsConstructor

public class TransactionService  {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final TransactionPersistenceService transactionPersistenceService;
    private final TransactionCompletionService transactionCompletionService;
    private final TransactionInitiationService transactionInitiationService;
    private final TransactionCompensationService compensationService;
    private final ObjectMapper objectMapper;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String OTP_KEY_PREFIX = "transaction:otp:";
    private static final String OTP_ATTEMPT_KEY_PREFIX = "transaction:otp:attempts:";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";



    public TransactionResponse transfer(
            TransaferRequest request,
            String idempotencyKey) {

        log.info(
                "SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount()
        );

        // 1. Validate Idempotency-Key

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException(
                    "Idempotency-Key is required"
            );
        }

        // 2. Idempotency Check

        Optional<Transaction> existingTransaction =
                transactionRepository.findByIdempotencyKey(idempotencyKey);

        if (existingTransaction.isPresent()) {

            log.info(
                    "Duplicate request detected for idempotency key: {}",
                    idempotencyKey
            );

            return mapToResponse(
                    existingTransaction.get()
            );
        }

        // 3. Business Validation

        if (request.getSenderAccountNumber() == null ||
                request.getSenderAccountNumber().isBlank()) {

            throw new IllegalArgumentException(
                    "Sender account number is required"
            );
        }

        if (request.getReceiverAccountNumber() == null ||
                request.getReceiverAccountNumber().isBlank()) {

            throw new IllegalArgumentException(
                    "Receiver account number is required"
            );
        }

        if (request.getSenderAccountNumber()
                .equals(request.getReceiverAccountNumber())) {

            throw new IllegalArgumentException(
                    "Sender and receiver account cannot be same"
            );
        }

        if (request.getAmount() == null ||
                request.getAmount().signum() <= 0) {

            throw new IllegalArgumentException(
                    "Amount must be greater than zero"
            );
        }

        // 4. Validate Sender Account

        AccountStatusResponse senderAccount =
                accountServiceClient.getAccount(
                        request.getSenderAccountNumber()
                );

        if (senderAccount == null) {

            throw new ResourceNotFoundException(
                    "Sender account not found: "
                            + request.getSenderAccountNumber()
            );
        }

        if (!"ACTIVE".equals(senderAccount.getStatus())) {

            throw new IllegalStateException(
                    "Sender account is "
                            + senderAccount.getStatus()
            );
        }

        // 5. Create Transaction FIRST

        Transaction transaction = new Transaction();

        transaction.setSenderAccountNumber(
                request.getSenderAccountNumber()
        );

        transaction.setReceiverAccountNumber(
                request.getReceiverAccountNumber()
        );

        transaction.setAmount(
                request.getAmount()
        );

        transaction.setType(
                TransactionType.TRANSFER
        );

        transaction.setDescription(
                request.getDescription()
        );

        transaction.setReferenceNumber(
                UUID.randomUUID().toString()
        );

        transaction.setIdempotencyKey(
                idempotencyKey
        );

        Transaction savedTransaction =
                transactionPersistenceService
                        .createPendingTransaction(transaction);

        log.info(
                "Transaction created with PENDING status: {}",
                savedTransaction.getId()
        );

        // 6. Saga Step 1 - Deduct Sender Balance

        try {

            accountServiceClient.deductBalance(
                    savedTransaction.getSenderAccountNumber(),
                    savedTransaction.getAmount(),
                    savedTransaction.getId()
            );

            log.info(
                    "Sender balance deducted successfully: transaction={}",
                    savedTransaction.getId()
            );

        } catch (FeignException.BadRequest e) {

            log.warn(
                    "Failed to deduct sender balance. " +
                            "Insufficient balance: transaction={}",
                    savedTransaction.getId()
            );

            transactionPersistenceService.markFailed(
                    savedTransaction,
                    "Insufficient balance"
            );

            throw new InsufficientBalanceException(
                    "Insufficient balance for sender account"
            );

        } catch (Exception e) {

            log.error(
                    "Failed to deduct sender balance: transaction={}",
                    savedTransaction.getId(),
                    e
            );

            transactionPersistenceService.markFailed(
                    savedTransaction,
                    "Unable to deduct sender account balance"
            );

            throw new RuntimeException(
                    "Unable to process transfer",
                    e
            );
        }

        // 7. Saga Step 2 - Mark Processing + Create Outbox

        try {

            savedTransaction =
                    transactionInitiationService
                            .markProcessingAndCreateOutbox(
                                    savedTransaction
                            );

            log.info(
                    "Transaction marked PROCESSING and " +
                            "outbox event created: transaction={}",
                    savedTransaction.getId()
            );

        } catch (Exception e) {

            log.error(
                    "Failed to mark transaction processing/create outbox. " +
                            "transactionId={}",
                    savedTransaction.getId(),
                    e
            );

            transactionPersistenceService.markFailed(
                    savedTransaction,
                    "Failed to create transaction initiated event"
            );

            /*
             * IMPORTANT:
             *
             * Balance was already deducted before this failure.
             * Therefore, production-level implementation should
             * compensate/refund the deducted amount here.
             */

            throw new RuntimeException(
                    "Unable to process transfer",
                    e
            );
        }

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction (String transactionId) {
        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found")));
    }

    public List<TransactionResponse> getTransactionHistory(
            String accountNumber) {

        return transactionRepository
                .findBySenderAccountNumberOrReceiverAccountNumberOrderByCreatedAtDesc(
                        accountNumber,
                        accountNumber
                )
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {

        log.info("OTP verification for transaction: {}", transactionId);

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() ->
                        new RuntimeException(
                                "Transaction not found: " + transactionId
                        ));

        // OTP verification allowed only for
        // PENDING_VERIFICATION transaction
        if (transaction.getStatus() != TransactionStatus.PENDING_VERIFICATION) {

            throw new IllegalStateException(
                    "Transaction is not waiting for OTP verification. " +
                            "Current status: " + transaction.getStatus()
            );
        }

        // OTP Redis key
        String otpKey = OTP_KEY_PREFIX + transactionId;

        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        // 1. OTP EXPIRED

        if (storedOtp == null) {

            log.warn(
                    "OTP expired for transaction: {}",
                    transactionId
            );

            compensationService.compensateTransaction(
                    transaction,
                    "OTP expired - transaction cancelled and amount refunded"
            );

            return mapToResponse(transaction);
        }

        // 2. WRONG OTP

        if (!storedOtp.equals(otp)) {

            String attemptKey = OTP_ATTEMPT_KEY_PREFIX + transactionId;

            Long attempts = redisTemplate.opsForValue()
                    .increment(attemptKey);

            if (attempts >= 3) {

                // Maximum attempts reached
                redisTemplate.delete(otpKey);
                redisTemplate.delete(attemptKey);

                log.warn(
                        "Maximum OTP attempts exceeded for transaction: {}",
                        transactionId
                );

                blockAccountAndCompensate(
                        transaction,
                        "Maximum OTP attempts exceeded"
                );

            } else {

                // User can retry OTP
                log.warn(
                        "Wrong OTP for transaction: {}. Attempt {}/3",
                        transactionId,
                        attempts
                );
            }

            // IMPORTANT:
            // Don't delete OTP here.
            // Don't complete transaction here.
            return mapToResponse(transaction);
        }


        // 3. CORRECT OTP

        log.info(
                "OTP verified successfully for transaction: {}",
                transactionId
        );

        // Delete OTP
        redisTemplate.delete(otpKey);

        // Delete attempt counter
        String attemptKey = OTP_ATTEMPT_KEY_PREFIX + transactionId;

        redisTemplate.delete(attemptKey);

        // Complete transaction
        completeTransaction(transaction);

        return mapToResponse(transaction);
    }


    private void blockAccountAndCompensate(
            Transaction transaction,
            String reason) {

        log.warn(
                "FRAUD DETECTED - transaction: {} account: {}",
                transaction.getId(),
                transaction.getSenderAccountNumber()
        );

        // 1. First refund the deducted amount
        compensationService.compensateTransaction(transaction, reason);

        // 2. Then notify Account Service to block the account
        Map<String, Object> fraudEvent = new HashMap<>();

        fraudEvent.put(
                "transactionId",
                transaction.getId()
        );

        fraudEvent.put(
                "accountNumber",
                transaction.getSenderAccountNumber()
        );

        fraudEvent.put(
                "reason",
                reason
        );

        try {

            // Convert HashMap -> JSON String
            String fraudEventJson =
                    objectMapper.writeValueAsString(fraudEvent);

            kafkaTemplate.send(
                    FRAUD_DETECTED_TOPIC,
                    transaction.getSenderAccountNumber(),
                    fraudEventJson
            ).get();

            log.info(
                    "Fraud detected event published. Account block requested: {}",
                    transaction.getSenderAccountNumber()
            );

        } catch (Exception e) {

            log.error(
                    "Failed to publish fraud.detected event. transactionId={}",
                    transaction.getId(),
                    e
            );

            throw new RuntimeException(
                    "Failed to publish fraud detected event",
                    e
            );
        }
    }

    private TransactionResponse completeTransaction(Transaction transaction) {

        Transaction completedTransaction =
                transactionCompletionService.completeTransaction(transaction);

        return mapToResponse(completedTransaction);
    }

    public void processCleanResult(String transactionId) {

        Transaction transaction =
                transactionRepository.findById(transactionId)
                        .orElseThrow(() ->
                                new ResourceNotFoundException(
                                        "Transaction not found: " + transactionId
                                )
                        );

        if (transaction.getStatus() != TransactionStatus.PROCESSING) {

            log.info(
                    "Ignoring fraud clean result. transactionId={}, status={}",
                    transactionId,
                    transaction.getStatus()
            );

            return;
        }

        completeTransaction(transaction);
    }

    private TransactionResponse mapToResponse(Transaction transaction) {

        TransactionResponse response = new TransactionResponse();

        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
        response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setFailureReason(transaction.getFailureReason());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompletedAt(transaction.getCompletedAt());

        return response;
    }
}
