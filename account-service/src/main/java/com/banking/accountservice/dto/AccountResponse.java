package com.banking.accountservice.dto;

import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor

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
