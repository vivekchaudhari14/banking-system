package com.banking.accountservice.entity;

import java.time.Instant;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(
        name = "processed_events",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_event_consumer",
                        columnNames = {"event_id", "consumer_name"}
                )
        }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "consumer_name", nullable = false)
    private String consumerName;

    @CreationTimestamp
    private Instant processedAt;
}