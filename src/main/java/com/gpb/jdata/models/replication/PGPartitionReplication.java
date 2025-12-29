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

	@Column(name = "parlevel")
	private Integer parlevel;

	@Column(name = "paristemplate")
	private Boolean paristemplate;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PGPartitionReplication)) return false;
		PGPartitionReplication that = (PGPartitionReplication) o;
		return Objects.equals(oid, that.oid) &&
				Objects.equals(parrelid, that.parrelid) &&
				Objects.equals(parnatts, that.parnatts) &&
				Objects.equals(parkind, that.parkind) &&
				Arrays.equals(paratts, that.paratts) &&
				Objects.equals(parlevel, that.parlevel);
	}
	
	@Override
	public int hashCode() {
		int result = Objects.hash(oid, parrelid, parnatts, parkind, parlevel);
		result = 31 * result + Arrays.hashCode(paratts);
		return result;
	}

    @Override
    public String toString() {
        return String.format(
            "PGPartitionReplication{oid=%s, parrelid=%s"
            + ", parnatts=%s, parkind='%s', paratts=%s}", 
            oid, parrelid, parnatts, parkind, paratts);
    }
}
