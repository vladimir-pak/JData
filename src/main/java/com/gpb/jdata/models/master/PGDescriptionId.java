package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Embeddable
@AllArgsConstructor
@NoArgsConstructor
public class PGDescriptionId implements Serializable {
    @Serial
    private static final Long serialVersionUID = 7481655631217844775L;

    private Long objoid;

    private int objsubid;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGDescriptionId that = (PGDescriptionId) o;
		return objsubid == that.objsubid && Objects.equals(objoid, that.objoid);
	}
	@Override
	public int hashCode() {
		return Objects.hash(objoid, objsubid);
	}
}
