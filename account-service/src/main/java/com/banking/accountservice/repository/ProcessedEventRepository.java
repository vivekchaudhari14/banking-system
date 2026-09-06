package com.banking.accountservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.banking.accountservice.entity.ProcessedEvent;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEvent, String> {

    boolean existsByEventIdAndConsumerName(
            String eventId,
            String consumerName
    );
}