package com.gpb.jdata.models.replication;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import com.gpb.jdata.models.master.PGAttrdefId;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Репликация pg_attrdef
 */
@Data
@Entity
@Table(name = "pg_attrdef_rep")
@AllArgsConstructor
@NoArgsConstructor
public class PGAttrdefReplication implements Serializable {
	@Serial
	private static final long serialVersionUID = -2037414706747042309L;

	@EmbeddedId
	private PGAttrdefId id;

	@Column(name = "adbin", length = 10000000)
	private String adbin;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGAttrdefReplication that = (PGAttrdefReplication) o;
		return Objects.equals(id, that.id) && Objects.equals(adbin, that.adbin) && Objects.equals(db, that.db);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, adbin, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGAttrdefReplication{id=%s, adbin='%s', db='%s'}", 
            id, adbin, db);
    }
}
