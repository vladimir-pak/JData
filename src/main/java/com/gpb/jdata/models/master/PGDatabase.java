package com.gpb.jdata.models.master;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_database", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGDatabase {
    @Id
    private Long oid;

    private String datname;

    @Override
    public String toString() {
        return String.format(
            "PGDatabase{oid=%s, datname='%s'}", oid, datname);
    }
}
