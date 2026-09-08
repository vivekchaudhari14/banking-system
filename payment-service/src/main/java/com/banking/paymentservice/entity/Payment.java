package com.banking.paymentservice.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "payments")
@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    String id;

    @Column(name = "razorpay_order_id", nullable = false, unique = true)
    private String razorpayOrderId;

    @Column(name = "razorpay_payment_id", unique = true)
    private String razorpayPaymentId;

    @Column(nullable = false)
    String accountNumber;

    @Column(nullable = false, precision = 15, scale = 2)
    BigDecimal amount;

    @Column(nullable = false)
    String currency;

    @Enumerated(EnumType.STRING)
    PaymentStatus status;

    /*
     * Kafka event publishing status
     *
     * PENDING    -> event अजून publish केलेला नाही
     * PUBLISHED  -> event successfully Kafka ला publish झाला
     * FAILED     -> Kafka publish fail झाला
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    EventStatus eventStatus;

    String description;

    String failureReason;

    @CreationTimestamp
    LocalDateTime createdAt;

    @UpdateTimestamp
    LocalDateTime updatedAt;
}