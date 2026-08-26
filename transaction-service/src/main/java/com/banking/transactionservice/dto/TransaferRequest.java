package com.banking.transactionservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
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
public class TransaferRequest {

    @NotNull(message = "Sender account number is required")
    String senderAccountNumber;

    @NotBlank(message = "Receiver account number is required")
    String receiverAccountNumber;

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be positive ")
    BigDecimal amount;

    String description;


}
