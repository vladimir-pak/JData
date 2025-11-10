package com.gpb.jdata.models.replication;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

@Data
@Entity
@Table(name = "pg_sequence_rep")
@AllArgsConstructor
@NoArgsConstructor
public class PGSequenceReplication implements Serializable {
    private static final long serialVersionUID = 2L;
    @Id
    @Column(name = "seqrelid")
    private Long seqrelid;

    @Column(name = "seqstart")
    private BigDecimal seqstart;

    @Column(name = "seqincrement")
    private BigDecimal seqincrement;

    @Column(name = "seqmin")
    private BigDecimal seqmin;

    @Column(name = "seqmax")
    private BigDecimal seqmax;

    @Column(name = "seqcache")
    private BigDecimal seqcache;

    @Column(name = "seqcycle")
    private Boolean seqcycle;

    @Column(name = "db", length = 255)
    private String db;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PGSequenceReplication that = (PGSequenceReplication) o;
        return Objects.equals(seqrelid, that.seqrelid) && Objects.equals(seqstart, that.seqstart) && Objects.equals(seqincrement, that.seqincrement) && Objects.equals(seqmin, that.seqmin) && Objects.equals(seqmax, that.seqmax) && Objects.equals(seqcache, that.seqcache) && Objects.equals(seqcycle, that.seqcycle) && Objects.equals(db, that.db);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqrelid, seqstart, seqincrement, seqmin, seqmax, seqcache, seqcycle, db);
    }

    @Override
    public String toString() {
        return String.format(
            "PGSequenceReplication{seqrelid=%s"
            + ", seqstart=%s"
            + ", seqincrement=%s"
            + ", seqmin=%s"
            + ", seqmax=%s"
            + ", seqcache=%s"
            + ", seqcycle=%s"
            + ", db='%s'}", 
            seqrelid,
            seqstart,
            seqincrement,
            seqmin,
            seqmax,
            seqcache,
            seqcycle,
            db);
    }
}
