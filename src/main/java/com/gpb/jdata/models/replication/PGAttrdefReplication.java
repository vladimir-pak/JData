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
@Table(name = "pg_attrdef_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGAttrdefReplication implements Serializable {
	@Serial
	private static final long serialVersionUID = -2037414706747042309L;

	@EmbeddedId
	private PGAttrdefId id;

	@Column(name = "adbin", length = 10000000)
	private String adbin;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGAttrdefReplication that = (PGAttrdefReplication) o;
		return Objects.equals(id, that.id) && Objects.equals(adbin, that.adbin);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, adbin);
	}

    @Override
    public String toString() {
        return String.format(
            "PGAttrdefReplication{id=%s, adbin='%s'}", 
            id, adbin);
    }
}
