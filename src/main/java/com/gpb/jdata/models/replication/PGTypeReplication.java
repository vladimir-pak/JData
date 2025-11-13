package com.gpb.jdata.models.replication;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

@Data
@Entity
@Table(name = "pg_type_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGTypeReplication implements Serializable {
	private static final long serialVersionUID = 7799357190536186164L;

	/**
	 * 	Идентификатор строки (скрытый атрибут; должен выбираться явно)
	 */
	@Id
	@Column(name = "oid")
	private Long oid;

	/**
	 * Имя типа данных
	 */
	@Column(name = "typname")
	private String typname;

	/**
	 * 	OID пространства имён, содержащего этот тип
	 */
	@Column(name = "typnamespace")
	private Long typnamespace;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGTypeReplication that = (PGTypeReplication) o;
		return Objects.equals(oid, that.oid) && Objects.equals(typname, that.typname) && Objects.equals(typnamespace, that.typnamespace) && Objects.equals(db, that.db);
	}

	@Override
	public int hashCode() {
		return Objects.hash(oid, typname, typnamespace, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGTypeReplication{oid=%s, typname='%s', typnamespace=%s, db='%s'}", 
            oid, typname, typnamespace, db);
    }
}
