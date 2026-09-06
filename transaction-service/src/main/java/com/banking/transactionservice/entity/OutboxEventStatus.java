package com.banking.transactionservice.entity;

public enum OutboxEventStatus {

    PENDING,
    PUBLISHED,
    FAILED
}