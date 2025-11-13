package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.Objects;

import com.gpb.jdata.models.master.PGDescriptionId;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *  Репликация pg_description
 */
@Data
@Entity
@Table(name = "pg_description_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGDescriptionReplication implements Serializable {
	private static final long serialVersionUID = -8354799663985339319L;

	@EmbeddedId
	private PGDescriptionId id;

	/**
	 * Произвольный текст, служащий описанием данного объекта
	 */
	@Column(name = "description", length = 1000000)
	private String description;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGDescriptionReplication that = (PGDescriptionReplication) o;
		return Objects.equals(id, that.id) && Objects.equals(description, that.description) && Objects.equals(db, that.db);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, description, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGDescription{id=%s, description='%s', db='%s'}", id, description, db);
    }
}
