package com.gpb.jdata.models.replication;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;


@Data
@Entity
@Table(name = "pg_partition_rule_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGPartitionRuleReplication implements Serializable {
    private static final long serialVersionUID = 2L;
    @Id
    @Column(name = "oid")
    private Long oid;

    @Column(name = "parchildrelid")
    private Long parchildrelid;

    @Column(name = "paroid")
    private Long paroid;

    @Column(name = "db")
    private String db;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PGPartitionRuleReplication that = (PGPartitionRuleReplication) o;
        return Objects.equals(oid, that.oid) && Objects.equals(parchildrelid, that.parchildrelid) && Objects.equals(paroid, that.paroid) && Objects.equals(db, that.db);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, parchildrelid, paroid, db);
    }

    @Override
    public String toString() {
        return String.format(
            "PGPartitionRuleReplication{oid=%s, parchildrelid=%s, paroid=%s, db='%s'}", 
            oid, parchildrelid, paroid, db);
    }
}
