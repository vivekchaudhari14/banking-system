package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TransactionPersistenceService {

    private final TransactionRepository transactionRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Transaction createPendingTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.PENDING);

        return transactionRepository.saveAndFlush(transaction);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Transaction markProcessing(Transaction transaction) {

        transaction.setStatus(TransactionStatus.PROCESSING);

        return transactionRepository.saveAndFlush(transaction);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Transaction markFailed(
            Transaction transaction,
            String reason) {

        transaction.setStatus(TransactionStatus.FAILED);
        transaction.setFailureReason(reason);

        return transactionRepository.saveAndFlush(transaction);
    }
}