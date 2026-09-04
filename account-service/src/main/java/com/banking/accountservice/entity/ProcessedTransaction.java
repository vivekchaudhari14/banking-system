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
public class ProcessedTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "transaction_id", nullable = false)
    private String transactionId;

    @Column(name = "operation", nullable = false)
    private String operation;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    public ProcessedTransaction(String transactionId, String operation) {
        this.transactionId = transactionId;
        this.operation = operation;
        this.processedAt = Instant.now();
    }
}