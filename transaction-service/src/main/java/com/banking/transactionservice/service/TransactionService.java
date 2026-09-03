package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransaferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor

public class TransactionService  {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";


    /*

        SAGA Step -1 : Initiate transfer
        Deducts from sender via feign
        saves transaction as Processing
        publish event to kafka for fraud check
        returns.

     */

    public TransactionResponse transfer(TransaferRequest request) {

        log.info(
                "SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount()
        );

        // 1. Basic business validation
        if (request.getSenderAccountNumber()
                .equals(request.getReceiverAccountNumber())) {

            throw new IllegalArgumentException(
                    "Sender and receiver accounts must be different"
            );
        }

        if (request.getAmount() == null ||
                request.getAmount().signum() <= 0) {

            throw new IllegalArgumentException(
                    "Amount must be greater than zero"
            );
        }

        // 2. Create transaction FIRST
        Transaction transaction = new Transaction();

        transaction.setSenderAccountNumber(
                request.getSenderAccountNumber());

        transaction.setReceiverAccountNumber(
                request.getReceiverAccountNumber());

        transaction.setAmount(request.getAmount());

        transaction.setType(TransactionType.TRANSFER);

        transaction.setStatus(TransactionStatus.PENDING);

        transaction.setDescription(request.getDescription());

        transaction.setReferenceNumber(
                UUID.randomUUID().toString()
        );

        Transaction savedTransaction =
                transactionRepository.save(transaction);

        log.info(
                "Transaction created: {} status=PENDING",
                savedTransaction.getId()
        );

        try {

            // 3. Saga Step 1 - Deduct sender
            accountServiceClient.deductBalance(
                    savedTransaction.getSenderAccountNumber(),
                    savedTransaction.getAmount()
            );

            // 4. Deduction successful
            savedTransaction.setStatus(
                    TransactionStatus.PROCESSING
            );

            transactionRepository.save(savedTransaction);

            log.info(
                    "Sender balance deducted successfully. Transaction: {}",
                    savedTransaction.getId()
            );

            // 5. Saga Step 2 - Fraud check
            TransactionInitiatedEvent event =
                    new TransactionInitiatedEvent(
                            savedTransaction.getId(),
                            savedTransaction.getSenderAccountNumber(),
                            savedTransaction.getReceiverAccountNumber(),
                            savedTransaction.getAmount(),
                            savedTransaction.getDescription()
                    );

            kafkaTemplate.send(
                    TRANSACTION_INITIATED_TOPIC,
                    savedTransaction.getId(),
                    event
            );

            log.info(
                    "TransactionInitiatedEvent published: {}",
                    savedTransaction.getId()
            );

            return mapToResponse(savedTransaction);

        } catch (Exception e) {

            log.error(
                    "SAGA FAILED - Transaction: {}",
                    savedTransaction.getId(),
                    e
            );

            // Mark transaction failed
            savedTransaction.setStatus(
                    TransactionStatus.FAILED
            );

            savedTransaction.setFailureReason(
                    "Unable to deduct sender balance"
            );

            transactionRepository.save(savedTransaction);

            throw new RuntimeException(
                    "Transaction failed",
                    e
            );
        }
    }

    public TransactionResponse getTransaction (String transactionId) {
        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found")));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository.
                findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verifcation for the trasaction: {}", transactionId);

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() ->
                        new RuntimeException("Transaction not found" + transactionId));

        String otpKey = "verifaction:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if (transaction.getStatus()
                != TransactionStatus.PENDING_VERIFICATION) {

            throw new IllegalStateException(
                    "Transaction is not waiting for OTP verification"
            );
        }

        if(storedOtp == null) {
            // otp expired
            log.warn("OTP expired for transaction {}", transactionId);
            compensateTransaction(transaction,"OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)) {
            // Block account And Refund
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction,
                    "Wrong otp entered - transaction cancelled, "+
                    "account blocked are security");
            return mapToResponse(transaction);
        }

        log.info("OTP verified - completing transaction {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);

    }

    private void compensateTransaction(
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
            accountServiceClient.creditBalance(
                    transaction.getSenderAccountNumber(),
                    transaction.getAmount()
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

            // Step 3: Notification event
            Map<String, Object> refundEvent = new HashMap<>();

            refundEvent.put(
                    "transactionId",
                    transaction.getId()
            );

            refundEvent.put(
                    "senderAccountNumber",
                    transaction.getSenderAccountNumber()
            );

            refundEvent.put(
                    "amount",
                    transaction.getAmount()
            );

            refundEvent.put(
                    "reason",
                    reason
            );

            kafkaTemplate.send(
                    TRANSACTION_REFUNDED_TOPIC,
                    transaction.getId(),
                    refundEvent
            );

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

    private void blockAccountAndCompensate(
            Transaction transaction,
            String reason) {

        log.warn(
                "FRAUD DETECTED - transaction: {} account: {}",
                transaction.getId(),
                transaction.getSenderAccountNumber()
        );

        // 1. Ask Account Service to block account
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

        kafkaTemplate.send(
                FRAUD_DETECTED_TOPIC,
                transaction.getSenderAccountNumber(),
                fraudEvent
        );

        // 2. Refund deducted amount
        compensateTransaction(
                transaction,
                reason
        );
    }

    private void completeTransaction(
            Transaction transaction) {

        transaction.setStatus(
                TransactionStatus.COMPLETED
        );

        transaction.setCompletedAt(
                LocalDateTime.now()
        );

        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent =
                new TransactionCompletedEvent(
                        transaction.getId(),
                        transaction.getSenderAccountNumber(),
                        transaction.getReceiverAccountNumber(),
                        transaction.getAmount(),
                        transaction.getDescription()
                );

        kafkaTemplate.send(
                TRANSACTION_COMPLETED_TOPIC,
                transaction.getId(),
                completedEvent
        );

        log.info(
                "SAGA COMPLETE - transaction: {}",
                transaction.getId()
        );
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found" + transactionId));

        if(transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("Transaction {} not PROCESSING - skipping",transactionId);
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
