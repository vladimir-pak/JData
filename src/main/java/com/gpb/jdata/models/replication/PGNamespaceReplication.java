package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
Репликация pg_namespace
**/
@Data
@Entity
@Table(name = "pg_namespace_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGNamespaceReplication implements Serializable {
	private static final long serialVersionUID = 3366546312262329382L;

	/**
	 * Идентификатор строки.
	 * **/
	@Column(name = "oid")
	private Long oid;

	/**
	 * Имя пространства имён.
	 * **/
	@Id
	@Column(name = "nspname")
	private String nspname;

	@Column(name = "nspowner")
	private Long nspowner;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGNamespaceReplication that = (PGNamespaceReplication) o;
		return Objects.equals(oid, that.oid) && Objects.equals(nspname, that.nspname) && Objects.equals(nspowner, that.nspowner) && Objects.equals(db, that.db);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(oid, nspname, nspowner, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGNamespaceReplication{oid=%s, nspname='%s', nspowner=%s, db='%s'}", 
            oid, nspname, nspowner, db);
    }
}
