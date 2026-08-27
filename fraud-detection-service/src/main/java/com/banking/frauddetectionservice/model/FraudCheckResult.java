package com.banking.frauddetectionservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class FraudCheckResult {
    private boolean fraud;
    private String reason;
}
