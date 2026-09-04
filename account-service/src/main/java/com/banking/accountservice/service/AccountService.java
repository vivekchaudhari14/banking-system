package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.entity.ProcessedTransaction;
import com.banking.accountservice.repository.AccountRepository;
import com.banking.accountservice.repository.ProcessedTransactionRepository;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.security.auth.login.AccountException;
import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final ProcessedTransactionRepository processedTransactionRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account request {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account already exists for this email"+request.getEmail());
        }

        Account  account = new Account();
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
        log.info("Account created: {}", savedAccount.getAccountNumber());
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
                .orElseThrow(() -> new RuntimeException("Account not found"));

        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("Account blocked {}", accountNumber);

    }

    @Transactional
    public void deductBalance(
            String accountNumber,
            BigDecimal amount,
            String transactionId
    ) {

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Amount must be greater than zero");
        }

        // Idempotency check
        if (processedTransactionRepository
                .existsByTransactionIdAndOperation(transactionId, "DEBIT")) {

            log.info(
                    "Transaction {} already debited. Skipping duplicate request.",
                    transactionId
            );

            return;
        }

        // Pessimistic lock
        Account account = accountRepository
                .findByAccountNumberForUpdate(accountNumber)
                .orElseThrow(() ->
                        new ResourceNotFoundException(
                                "Account not found: " + accountNumber
                        )
                );

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new IllegalStateException("Account is not active");
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new IllegalStateException("Insufficient balance");
        }

        // Deduct money
        account.setBalance(
                account.getBalance().subtract(amount)
        );

        accountRepository.save(account);

        // Save idempotency record
        ProcessedTransaction processedTransaction =
                new ProcessedTransaction(
                        transactionId,
                        "DEBIT"
                );

        processedTransactionRepository.save(processedTransaction);

        log.info(
                "Amount {} deducted from account {} for transaction {}",
                amount,
                accountNumber,
                transactionId
        );
    }

    @Transactional
    public void creditBalance(
            String accountNumber,
            BigDecimal amount,
            String transactionId
    ) {

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Amount must be greater than zero");
        }

        // Idempotency check
        if (processedTransactionRepository
                .existsByTransactionIdAndOperation(transactionId, "CREDIT")) {

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
                        )
                );

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new IllegalStateException("Account is not active");
        }

        // Credit money
        account.setBalance(
                account.getBalance().add(amount)
        );

        accountRepository.save(account);

        // Save idempotency record
        ProcessedTransaction processedTransaction =
                new ProcessedTransaction(
                        transactionId,
                        "CREDIT"
                );

        processedTransactionRepository.save(processedTransaction);

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
