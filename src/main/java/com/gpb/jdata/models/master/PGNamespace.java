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
@Table(name = "pg_namespace", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGNamespace implements Serializable {
    @Serial
    private static final Long serialVersionUID = 8319959539865187067L;

    private Long oid;

    @Id
    private String nspname;

    private Long nspowner;

    @Override
    public String toString() {
        return String.format(
            "PGNamespace{oid=%s, nspname='%s', nspowner=%s}", oid, nspname, nspowner);
    }
}
