package com.banking.accountservice.dto;

import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class AccountResponse {

    String id;
    String accountNumber;
    String accountHolderName;
    String email;
    String phoneNumber;
    AccountType accountType;
    AccountStatus status;
    BigDecimal balance;
    BigDecimal dailyTransactionLimit;
    LocalDateTime createdAt;


}
