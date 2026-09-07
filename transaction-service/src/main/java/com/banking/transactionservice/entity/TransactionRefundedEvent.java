package com.banking.transactionservice.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TransactionRefundedEvent {

    private String transactionId;
    private String accountNumber;
    private BigDecimal amount;
    private String reason;
}