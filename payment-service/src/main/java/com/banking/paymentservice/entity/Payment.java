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

    String razorpayOrderId;
    String razorpayPaymentId;

    @Column(nullable = false)
    String accountNumber;

    @Column(nullable = false,precision=15,scale=2)
    BigDecimal amount;

    @Column(nullable = false)
    String currency;

    @Enumerated(EnumType.STRING)
    PaymentStatus status;

    String description;

    String failureReason;

    @CreationTimestamp
    LocalDateTime createdAt;

    @UpdateTimestamp
    LocalDateTime updatedAt;



}
