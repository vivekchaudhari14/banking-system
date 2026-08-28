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

    private static final String TRANSACTION_INITIATED_TOPIC = "Transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "Transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "Transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";


    /*

        SAGA Step -1 : Initiate transfer
        Deducts from sender via feign
        saves transaction as Processing
        publish event to kafka for fraud check
        returns.

     */

    public TransactionResponse transafer(TransaferRequest request) {
        log.info("SAGA START - Transafer: {} -> amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // saga step 1 : Deduct from sender

        accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount()
        );

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

       Transaction savedTransaction = transactionRepository.save(transaction);
       log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

       // saga step - 2 Publish for Fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent published : {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction (String transactionId) {
        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found")));
    }

    public List<TransactionResponse> getTransactionsHistory(String accountNumber) {
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

    private void compensateTransaction (Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amout: {} ",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());
        // Credit Money back to sender syn

        accountServiceClient.creditBalence(
                transaction.getSenderAccountNumber(),
                transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                " - SAGA Compensation executed, amount refunded at "
                + LocalDateTime.now());

         transactionRepository.save(transaction);

         //public refund event  - Notification service will alert user

        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("reason",reason);

        KafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA Compensation Completed - {} refunded to {} ",
                transaction.getAmount(),transaction.getSenderAccountNumber());

    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {

        //publish fraud.detected  -> account service will block account

        Map<String,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);

        log.warn("fraud.detected published - account: {} will be blocked ,kindly contact to the bank",
                transaction.getSenderAccountNumber());

        //saga compensation -> refun sender

        compensateTransaction(transaction,reason);

    }

    private void completeTransaction(Transaction transaction) {
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),completedEvent);

        log.info("SAGA COMPLETE - Transaction {} completed ",transaction.getId());


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
