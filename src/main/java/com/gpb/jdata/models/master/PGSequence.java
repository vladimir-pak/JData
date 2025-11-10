package com.gpb.jdata.models.master;

import java.io.Serializable;
import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_sequence", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGSequence implements Serializable {
    private static final long serialVersionUID = 1L;

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

    @Override
    public String toString() {
        return String.format(
            "PGSequence{seqrelid=%s"
            + ", seqstart=%s"
            + ", seqincrement=%s"
            + ", seqmin=%s"
            + ", seqmax=%s"
            + ", seqcache=%s"
            + ", seqcycle=%s}", 
            seqrelid,
            seqstart,
            seqincrement,
            seqmin,
            seqmax,
            seqcache,
            seqcycle);
    }
}
