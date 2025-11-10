package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "actions_rep")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Action implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "operation_type", nullable = false, length = 255)
    private String operationType;

    @Column(name = "entity_name", nullable = false, length = 50)
    private String entityName;

    @Column(columnDefinition = "TEXT")
    private String details;

    @Column(name = "additional_info", columnDefinition = "TEXT")
    private String additionalInfo;

    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;
}
