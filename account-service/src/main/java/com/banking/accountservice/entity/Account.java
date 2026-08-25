package com.banking.accountservice.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.hibernate.annotations.CreationTimestamp;
import org.springframework.cglib.core.Local;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "accounts")
@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Account {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    String id;

    @Column(nullable = false, unique = true)
    String accountNumber;

    @Column(nullable = false)
    String accountHolderName;

    @Column(nullable = false)
    String email;

    @Column(nullable = false)
    String phoneNumber;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    AccountType accountType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    AccountStatus status;

    @Column(nullable = false, precision = 15, scale = 2)
    BigDecimal balance;

    @Column(nullable = false, precision = 15, scale = 2)
    BigDecimal dailyTransactionLimit;

    @CreationTimestamp
    LocalDateTime createdAt;

    @CreationTimestamp
    LocalDateTime updatedAt;

}
