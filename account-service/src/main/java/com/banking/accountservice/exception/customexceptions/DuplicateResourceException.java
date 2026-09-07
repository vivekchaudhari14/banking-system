package com.banking.accountservice.exception.customexceptions;



public class DuplicateResourceException extends RuntimeException {

    public DuplicateResourceException(String message) {

        super(message);
    }
}
