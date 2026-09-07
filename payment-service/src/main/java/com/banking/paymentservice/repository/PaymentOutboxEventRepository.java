package com.banking.paymentservice.repository;

import com.banking.paymentservice.entity.EventStatus;
import com.banking.paymentservice.entity.PaymentOutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PaymentOutboxEventRepository
        extends JpaRepository<PaymentOutboxEvent, String> {

    List<PaymentOutboxEvent>
    findTop100ByStatusOrderByCreatedAtAsc(
            EventStatus status
    );
}