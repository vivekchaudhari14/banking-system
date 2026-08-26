package com.banking.accountservice.controller;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("api/v1/accounts")
@Slf4j
@RequiredArgsConstructor

public class AccountController {

    private final AccountService accountService;

    @PostMapping()
    public ResponseEntity<AccountResponse> createAccount(
            @Valid @RequestBody CreateAccountRequest request) {
        return ResponseEntity.status(HttpStatusCode.CREATED)
                .body(accountService.createAccount(request));

    }

    @GetMapping("{accountNumber}/balance")
    public ResponseEntity<AccountResponse> getAccount(
            @PathVariable String accountNumber ){
        return  ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("{accountNumber}")
    public ResponseEntity<BigDecimal> getBalance(
            @PathVariable String accountNumber ){
        return  ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(
            @PathVariable String accountNumber){
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked Successfully");
    }

    /*
        saga Step1 - deduct Balence
        called by Transaction service When transfer is Initiated
     */

    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance(
            @PathVariable String accountNumber,@RequestParam BigDecimal amount){
        accountService.deductBalance(accountNumber.amount);
        return ResponseEntity.ok("Account deducted Successfully");
    }

    /*
        saga Step 4 - compensating transaction endPoint
        called by Transaction Service int two Scenarios
        1. fraud detected -> refund sender(undo step 1)
        2. Transaction completed -> Credit Receiver

     */

    @PutMapping("{accountNumber}/credit")
    public ResponseEntity<String> creditBalence(
            @PathVariable String accountNumber,@RequestParam BigDecimal amount){
        accountService.creditBalance(accountNumber,amount);
        return ResponseEntity.ok("Balance credited Successfully");

    }

}
