package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import org.hibernate.validator.constraints.Length;

import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Embeddable
@AllArgsConstructor
@NoArgsConstructor
public class PGViewsId implements Serializable {
	@Serial
	private static final long serialVersionUID = -4675110148789615254L;
	/**
	 * Имя схемы.
	 */
	@Length(max = 1000)
	private String schemaname;
	/**
	 * Имя представления.
	 */
	@Length(max = 1000)
	private String viewname;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGViewsId pgViewsId = (PGViewsId) o;
		return Objects.equals(schemaname, pgViewsId.schemaname) && Objects.equals(viewname, pgViewsId.viewname);
	}
	@Override
	public int hashCode() {
		return Objects.hash(schemaname, viewname);
	}
}
