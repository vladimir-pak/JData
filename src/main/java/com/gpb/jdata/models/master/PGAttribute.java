package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "pg_attribute", schema = "pg_catalog")
@IdClass(PGAttributeId.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PGAttribute implements Serializable {
    
    @Serial
    private static final Long serialVersionUID = 8783278769346297009L;

    @Id
    private Long attrelid;

    private Long attnum;

    private String attname;

    private boolean atthasdef;

    private boolean attnotnull;

    private Long atttypid;

    private int atttypmod;

    @Override
	public boolean equals(Object o) {
		if (o == null || getClass() != o.getClass()) return false;
		PGAttribute that = (PGAttribute) o;
		return atthasdef == that.atthasdef 
                && attnotnull == that.attnotnull 
                && atttypmod == that.atttypmod 
                && Objects.equals(attrelid, that.attrelid) 
                && Objects.equals(attnum, that.attnum) 
                && Objects.equals(attname, that.attname) 
                && Objects.equals(atttypid, that.atttypid);
	}

	@Override
	public int hashCode() {
		return Objects.hash(attrelid, attnum, attname, atthasdef, attnotnull, atttypid, atttypmod);
	}

    @Override
    public String toString() {
        return String.format("PGAttribute{attrelid=%s"
                + ", attnum=%s"
                + ", attname='%s'"
                + ", atthasdef=%s"
                + ", attnotnull=%s"
                + ", atttypid=%s"
                + ", atttypmod=%s}",
                attrelid,
                attnum,
                attname,
                atthasdef,
                attnotnull,
                atttypid,
                atttypmod
                );
    }

}
