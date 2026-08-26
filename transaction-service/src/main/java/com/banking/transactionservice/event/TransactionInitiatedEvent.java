package com.banking.transactionservice.event;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class TransactionInitiatedEvent {

    String transactionId;
    String senderAccountNumber;
    String receiverAccountNumber;
    BigDecimal amount;
    String description;


}
