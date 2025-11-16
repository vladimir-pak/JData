package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.Objects;

import com.gpb.jdata.models.master.PGAttributeId;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Репликация pg_attribute
 */
@Data
@Entity
@Table(name = "pg_attribute_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGAttributeReplication implements Serializable {
	@EmbeddedId
	private PGAttributeId id;

	/**
	 * Порядковый номер столбца.
	 */
	@Column(name = "attnum")
	private Long attnum;
    
	/**
	 * Столбец имеет значение по умолчанию или генерирующее выражение,
	 * в этом случае в каталоге pg_attrdef будет соответствующая запись,
	 * собственно задающая это выражение.
	 */
	@Column(name = "atthasdef")
	private boolean atthasdef;

	/**
	 * Представляет ограничение NOT NULL.
	 */
	@Column(name = "attnotnull")
	private boolean attnotnull;

	/**
	 * Тип данных этого столбца
	 */
	@Column(name = "atttypid")
	private Long atttypid;

	/**
	 * В поле atttypmod записывается дополнительное число,
	 * связанное с определённым типом данных, указываемое при создании таблицы
	 */
	@Column(name = "atttypmod")
	private int atttypmod;

	// @Column(name = "db")
	// private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGAttributeReplication that = (PGAttributeReplication) o;
		return atthasdef == that.atthasdef && attnotnull == that.attnotnull && atttypmod == that.atttypmod && Objects.equals(id, that.id) && Objects.equals(attnum, that.attnum) && Objects.equals(atttypid, that.atttypid);
	}
	@Override
	public int hashCode() {
		return Objects.hash(id, attnum, atthasdef, attnotnull, atttypid, atttypmod);
	}

    @Override
    public String toString() {
        return String.format("PGAttributeReplication{id=%s"
                + ", attnum='%s'"
                + ", atthasdef='%s'"
                + ", attnotnull=%s"
                + ", atttypid=%s"
                + ", atttypmod=%s}",
                id,
                attnum,
                atthasdef,
                attnotnull,
                atttypid,
                atttypmod
                );
    }
}
