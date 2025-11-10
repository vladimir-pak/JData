package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.Objects;

import com.gpb.jdata.models.master.PGViewsId;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_views_rep")
@AllArgsConstructor
@NoArgsConstructor
public class PGViewsReplication implements Serializable {
	private static final long serialVersionUID = -8758577843133218687L;

	@EmbeddedId
	private PGViewsId id;

	/**
	 * Определение представления (реконструированный запрос SELECT)
	 */
	@Column(name = "definition", length = 10000000)
	private String definition;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGViewsReplication that = (PGViewsReplication) o;
		return Objects.equals(id, that.id) && Objects.equals(definition, that.definition) && Objects.equals(db, that.db);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, definition, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGViewsReplication{id=%s, definition='%s', db='%s'}", 
            id, definition, db);
    }
}
