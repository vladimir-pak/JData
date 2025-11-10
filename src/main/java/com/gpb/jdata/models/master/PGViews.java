package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Класс, хранящий определение представлений
 */
@Data
@Entity
@Table(name = "pg_views", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGViews implements Serializable {
	@Serial
	private static final long serialVersionUID = 1606922335829491559L;

	@EmbeddedId
	private PGViewsId id;

	/**
	 * Определение представления (реконструированный запрос SELECT)
	 */
	@Column(name = "definition", length = 10000000)
	private String definition;

    @Override
    public String toString() {
        return String.format(
            "PGViews{id=%s, definition='%s'}", id, definition);
    }
}
