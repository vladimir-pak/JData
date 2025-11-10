package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_constraint", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGConstraint implements Serializable {
    @Serial
    private static final Long serialVersionUID = 7855669283195397791L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long oid;

    private String conname;

    private Long connamespace;

    private String contype;

    private Long conrelid;

    private Long confrelid;

    private Long contypid;

    private List<Integer> conkey;

    private List<Integer> confkey;

    @Override
    public String toString() {
        return String.format(
            "PGConstraint{oid=%s"
            + ", conname='%s'"
            + ", connamespace=%s"
            + ", contype='%s'"
            + ", conrelid=%s"
            + ", confrelid=%s"
            + ", contypid=%s"
            + ", conkey=%s"
            + ", confkey=%s}", 
            oid,
            conname,
            connamespace,
            contype,
            conrelid,
            confrelid,
            contypid,
            conkey,
            confkey);
    }
}
