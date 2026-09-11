package com.banking.transactionservice.dto;

import lombok.Data;

@Data
public class AccountStatusResponse {

    private String accountNumber;
    private String status;
}