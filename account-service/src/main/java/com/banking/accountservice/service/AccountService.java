package com.banking.accountservice.service;

import com.banking.accountservice.exception.customexceptions.AccountBlockedException;
import com.banking.accountservice.exception.customexceptions.BadRequestException;
import com.banking.accountservice.exception.customexceptions.InsufficientBalanceException;
import com.banking.accountservice.exception.customexceptions.ResourceNotFoundException;
import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.*;
import com.banking.accountservice.repository.AccountRepository;
import com.banking.accountservice.repository.ProcessedTransactionRepository;
import org.apache.kafka.common.errors.DuplicateResourceException;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class AccountService {

    private final AccountRepository accountRepository;
    private final ProcessedTransactionRepository processedTransactionRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request) {

        log.info("Creating account request for email={}", request.getEmail());

        if (accountRepository.existsByEmail(request.getEmail())) {

            throw new DuplicateResourceException(
                    "Account already exists for email: " + request.getEmail()
            );
        }



        Account account = new Account();

        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhoneNumber(request.getPhoneNumber());
        account.setAccountType(request.getAccountType());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generateAccountNumber());

        account.setDailyTransactionLimit(
                request.getAccountType() == AccountType.SAVINGS
                        ? new BigDecimal("100000")
                        : new BigDecimal("500000")
        );

        Account savedAccount = accountRepository.save(account);

        log.info(
                "Account created successfully: {}",
                savedAccount.getAccountNumber()
        );

        return mapToResponse(savedAccount);
    }

    public AccountResponse getAccount (String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        return account.getBalance();
    }

    /*
        blocked Account - called by Fraud Detection Service via kafka
        @Param accountNumber;
     */

    public void blockAccount(String accountNumber) {
        log.info("Blocking account {}", accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new ResourceNotFoundException(
                                "Account not found: " + accountNumber
                        ));

        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("Account blocked {}", accountNumber);

    }



    public void deductBalance(
            String accountNumber,
            BigDecimal amount,
            String transactionId) {

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BadRequestException("Amount must be greater than zero");
        }

        // Idempotency check
        if (processedTransactionRepository
                .existsByTransactionIdAndOperation(
                        transactionId,
                        AccountOperation.DEBIT)) {

            return;
        }

        Account account = accountRepository
                .findByAccountNumberForUpdate(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException("Account not found"));

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new AccountBlockedException(
                    "Account is not active: " + accountNumber
            );
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new InsufficientBalanceException(
                    "Insufficient balance for account: " + accountNumber
            );
        }

        account.setBalance(
                account.getBalance().subtract(amount)
        );

        accountRepository.save(account);

        ProcessedTransaction processed = ProcessedTransaction.builder()
                .transactionId(transactionId)
                .operation(AccountOperation.DEBIT)
                .processedAt(Instant.now())
                .build();

        processedTransactionRepository.save(processed);
    }


    public void creditBalance(
            String accountNumber,
            BigDecimal amount,
            String transactionId
    ) {

        if (accountNumber == null || accountNumber.isBlank()) {
            throw new BadRequestException("Account number is required");
        }

        if (transactionId == null || transactionId.isBlank()) {
            throw new BadRequestException("Transaction ID is required");
        }

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BadRequestException("Amount must be greater than zero");
        }

        // Idempotency check
        if (processedTransactionRepository
                .existsByTransactionIdAndOperation(
                        transactionId,
                        AccountOperation.CREDIT
                )) {

            log.info(
                    "Transaction {} already credited. Skipping duplicate request.",
                    transactionId
            );

            return;
        }

        Account account = accountRepository
                .findByAccountNumberForUpdate(accountNumber)
                .orElseThrow(() ->
                        new ResourceNotFoundException(
                                "Account not found: " + accountNumber
                        ));

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new AccountBlockedException(
                    "Account is not active: " + accountNumber
            );
        }

        // Credit money
        account.setBalance(
                account.getBalance().add(amount)
        );

        accountRepository.save(account);

        // Save idempotency record
        ProcessedTransaction processed = ProcessedTransaction.builder()
                .transactionId(transactionId)
                .operation(AccountOperation.CREDIT)
                .processedAt(Instant.now())
                .build();

        processedTransactionRepository.save(processed);

        log.info(
                "Amount {} credited to account {} for transaction {}",
                amount,
                accountNumber,
                transactionId
        );
    }

    private String generateAccountNumber() {
        String accountNumber;
        do {
            long number =  secureRandom.nextLong(1_000_000_000_000L);

            accountNumber = String.format("%012d", number);

        }while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }


    public void refundBalance(
            String accountNumber,
            BigDecimal amount,
            String transactionId) {

        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException(
                    "Refund amount must be greater than zero"
            );
        }

        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException(
                    "Transaction ID is required"
            );
        }

        // Idempotency check
        if (processedTransactionRepository
                .existsByTransactionIdAndOperation(
                        transactionId,
                        AccountOperation.REFUND)) {

            log.info(
                    "Refund already processed. transactionId={}",
                    transactionId
            );

            return;
        }

        // IMPORTANT:
        // Pessimistic lock
        Account account = accountRepository
                .findByAccountNumberForUpdate(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException(
                                "Account not found: " + accountNumber
                        )
                );

        // Do NOT reject BLOCKED account here.
        account.setBalance(
                account.getBalance().add(amount)
        );

        accountRepository.save(account);

        ProcessedTransaction processed =
                new ProcessedTransaction();

        processed.setTransactionId(transactionId);
        processed.setOperation(AccountOperation.REFUND);
        processed.setProcessedAt(Instant.now());

        processedTransactionRepository.save(processed);

        log.info(
                "Refund successful. account={}, amount={}, transactionId={}",
                accountNumber,
                amount,
                transactionId
        );
    }

    private AccountResponse mapToResponse(Account account) {
        AccountResponse accountResponse = new AccountResponse();

        accountResponse.setId(account.getId());
        accountResponse.setAccountNumber(account.getAccountNumber());
        accountResponse.setAccountHolderName(account.getAccountHolderName());
        accountResponse.setEmail(account.getEmail());
        accountResponse.setPhoneNumber(account.getPhoneNumber());
        accountResponse.setStatus(account.getStatus());

        accountResponse.setAccountType(account.getAccountType());
        accountResponse.setBalance(account.getBalance());
        accountResponse.setDailyTransactionLimit(account.getDailyTransactionLimit());
        accountResponse.setCreatedAt(account.getCreatedAt());

        return accountResponse;

    }

}
