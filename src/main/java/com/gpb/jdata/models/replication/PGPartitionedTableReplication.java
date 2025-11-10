package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
Репликация pg_partitioned_table
**/
@Data
@Entity
@Table(name = "pg_partitioned_table_rep")
@AllArgsConstructor
@NoArgsConstructor
public class PGPartitionedTableReplication implements Serializable {
	private static final long serialVersionUID = -1689354425796939396L;
	/**
	 * OID записи в pg_class для этой секционированной таблицы.
	 * **/
	@Id
	@Column(name = "partrelid")
	private Long partrelid;

	/**
	 * Число столбцов в ключе разбиения.
	 * **/
	@Column(name = "partnatts")
	private short partnatts;

	/**
	 * Стратегия секционирования; 
	 * l = секционирование по списку (List), r = секционирование по диапазонам (Range).
	 * **/
	@Column(name = "partstrat")
	private String partstrat;

	/**
	 * Это массив из partnatts значений, указывающих, какие столбцы таблицы входят в ключ разбиения.
	 * **/
	@Column(name = "partattrs")
	private List<Integer> partattrs;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGPartitionedTableReplication that = (PGPartitionedTableReplication) o;
		return partnatts == that.partnatts && Objects.equals(partrelid, that.partrelid) && Objects.equals(partstrat, that.partstrat) && Objects.equals(partattrs, that.partattrs) && Objects.equals(db, that.db);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(partrelid, partnatts, partstrat, partattrs, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGPartitionedTable{partrelid=%s, partnatts=%s"
            + ", partstrat='%s', partattrs=%s, db='%s'}", 
            partrelid, partnatts, partstrat, partattrs, db);
    }
}
