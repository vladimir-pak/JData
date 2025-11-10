package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_database_rep")
@AllArgsConstructor
@NoArgsConstructor
public class PGDatabaseReplication implements Serializable {
    private static final long serialVersionUID = 2L;

    @Id
    @Column(name = "oid")
    private Long oid;

    @Column(name = "datname")
    private String datname;

    @Column(name = "db")
    private String db;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PGDatabaseReplication that = (PGDatabaseReplication) o;
        return Objects.equals(oid, that.oid) && Objects.equals(datname, that.datname) && Objects.equals(db, that.db);
    }
    @Override
    public int hashCode() {
        return Objects.hash(oid, datname, db);
    }

     @Override
    public String toString() {
        return String.format(
            "PGDatabaseReplication{oid=%s, datname='%s', db='%s'}", oid, datname, db);
    }
}
