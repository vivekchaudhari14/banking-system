package com.banking.transactionservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

public interface AccountServiceClient {

    @FeignClient(name = "account-service" , url = "${account.service.url}")
    @PutMapping("/api/v1/accounts/{accountNumber}/deduct")
    String deductBalance(@PathVariable("accountNumber") String accountNumber
            , @RequestParam("amount") BigDecimal amount);


}
