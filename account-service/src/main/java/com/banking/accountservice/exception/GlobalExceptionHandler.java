package com.banking.accountservice.exception;

import com.banking.accountservice.exception.customexceptions.AccountBlockedException;
import com.banking.accountservice.exception.customexceptions.BadRequestException;
import com.banking.accountservice.exception.customexceptions.InsufficientBalanceException;
import com.banking.accountservice.exception.customexceptions.ResourceNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<?> handleResourceNotFound(
            ResourceNotFoundException ex) {

        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(Map.of(
                        "timestamp", Instant.now(),
                        "status", 404,
                        "error", "NOT_FOUND",
                        "message", ex.getMessage()
                ));
    }

    @ExceptionHandler(BadRequestException.class)
    public ResponseEntity<?> handleBadRequest(
            BadRequestException ex) {

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(Map.of(
                        "timestamp", Instant.now(),
                        "status", 400,
                        "error", "BAD_REQUEST",
                        "message", ex.getMessage()
                ));
    }

    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<?> handleInsufficientBalance(
            InsufficientBalanceException ex) {

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(Map.of(
                        "timestamp", Instant.now(),
                        "status", 400,
                        "error", "INSUFFICIENT_BALANCE",
                        "message", ex.getMessage()
                ));
    }

    @ExceptionHandler(AccountBlockedException.class)
    public ResponseEntity<?> handleAccountBlocked(
            AccountBlockedException ex) {

        return ResponseEntity
                .status(HttpStatus.FORBIDDEN)
                .body(Map.of(
                        "timestamp", Instant.now(),
                        "status", 403,
                        "error", "ACCOUNT_BLOCKED",
                        "message", ex.getMessage()
                ));
    }
}