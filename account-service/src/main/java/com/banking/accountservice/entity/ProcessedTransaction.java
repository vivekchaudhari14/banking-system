package com.banking.accountservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(
        name = "processed_transactions",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_transaction_operation",
                        columnNames = {"transaction_id", "operation"}
                )
        }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "transaction_id", nullable = false)
    private String transactionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private AccountOperation operation;

    @Column(nullable = false)
    private Instant processedAt;
}