package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
Репликация pg_class
**/
@Data
@Entity
@Table(name = "pg_class_rep")
@AllArgsConstructor
@NoArgsConstructor
public class PGClassReplication implements Serializable {
	private static final long serialVersionUID = -2617868825070792833L;

	/**
	 * Идентификатор строки
	 * **/
	@Id
	@Column(name = "oid")
	private Long oid;

	/**
	 * Имя таблицы, индекса, представления и т. п.
	 * **/
	@Column(name = "relname")
	private String relname;

	/**
	 * OID пространства имён, содержащего это отношение (pg_namespace.oid)
	 * **/
	@Column(name = "relnamespace")
	private BigInteger relnamespace;

	/**
	 * r = обычная таблица, i = индекс (index), S = последовательность (sequence), 
	 * v = представление (view), m = материализованное представление (materialized view), 
	 * c = составной тип (composite), t = таблица TOAST, f = сторонняя таблица (foreign)
	 * **/
	@Column(name = "relkind")
	private String relkind;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGClassReplication that = (PGClassReplication) o;
		return Objects.equals(oid, that.oid) && Objects.equals(relname, that.relname) && Objects.equals(relnamespace, that.relnamespace) && Objects.equals(relkind, that.relkind);
	}

	@Override
	public int hashCode() {
		return Objects.hash(oid, relname, relnamespace, relkind);
	}

    @Override
    public String toString() {
        return String.format(
            "PGClassReplication{oid=%s, relname='%s'"
            + ", relnamespace=%s, relkind='%s'}", 
            oid, relname, relnamespace, relkind);
    }
}
