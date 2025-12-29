package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_partition_rule", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGPartitionRule implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    private Long oid;
    
    private Long parchildrelid;

    private Long paroid;

    private String parrangeevery;

    private Integer parruleord;

    @Override
    public String toString() {
        return String.format(
            "PGPartitionRule{oid=%s, parchildrelid=%s, paroid=%s}", 
            oid, parchildrelid, paroid);
    }
}
