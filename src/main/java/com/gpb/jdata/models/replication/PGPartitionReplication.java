package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Репликация pg_partition
 **/
@Data
@Entity
@Table(name = "pg_partition_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGPartitionReplication implements Serializable {
	private static final long serialVersionUID = 512345123456789L;
	/**
	 * Идентификатор строки
	 **/
	@Id
	@Column(name = "oid")
	private Long oid;

	/**
	 * OID родительской таблицы
	 **/
	@Column(name = "parrelid")
	private Long parrelid;

	/**
	 * Количество партиционирующих столбцов
	 **/
	@Column(name = "parnatts")
	private Integer parnatts;

	/**
	 * Метод партиционирования
	 **/
	@Column(name = "parkind")
	private String parkind;

	/**
	 * Номера партиционирующих столбцов
	 **/
	@Column(name = "paratts")
	private int[] paratts;

	/**
	 * Название базы данных
	 **/
	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PGPartitionReplication)) return false;
		PGPartitionReplication that = (PGPartitionReplication) o;
		return Objects.equals(oid, that.oid) &&
				Objects.equals(parrelid, that.parrelid) &&
				Objects.equals(parnatts, that.parnatts) &&
				Objects.equals(parkind, that.parkind) &&
				Objects.equals(db, that.db) &&
				Arrays.equals(paratts, that.paratts);
	}
	
	@Override
	public int hashCode() {
		int result = Objects.hash(oid, parrelid, parnatts, parkind, db);
		result = 31 * result + Arrays.hashCode(paratts);
		return result;
	}

    @Override
    public String toString() {
        return String.format(
            "PGPartitionReplication{oid=%s, parrelid=%s"
            + ", parnatts=%s, parkind='%s', paratts=%s, db='%s'}", 
            oid, parrelid, parnatts, parkind, paratts, db);
    }
}
