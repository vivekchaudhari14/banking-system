package com.banking.accountservice.repository;

import com.banking.accountservice.entity.AccountOperation;
import com.banking.accountservice.entity.ProcessedTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedTransactionRepository
        extends JpaRepository<ProcessedTransaction, String> {

    boolean existsByTransactionIdAndOperation(
            String transactionId,
            AccountOperation operation
    );
}