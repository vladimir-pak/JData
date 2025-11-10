package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_attrdef", schema = "pg_catalog")
@IdClass(PGAttrdefId.class)
@AllArgsConstructor
@NoArgsConstructor
public class PGAttrdef implements Serializable {
    @Serial
    private static final long serialVersionUID = 1255262471511740689L;

    @Id
    private Long adrelid;

    @Id
    private Long adnum;

    @Column(name = "adbin", length = 10000000)
    private String adbin;

    @Override
    public String toString() {
        return "PGAttrdef{" +
                "adrelid=" + adrelid +
                ", adnum=" + adnum +
                ", adbin='" + adbin + '\'' +
                '}';
    }
}
