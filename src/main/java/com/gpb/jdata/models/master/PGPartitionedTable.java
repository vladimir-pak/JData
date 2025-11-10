package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_partitioned_table", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGPartitionedTable implements Serializable {
    @Serial
    private static final Long serialVersionUID = -3318281852444059230L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long partrelid;

    private short partnatts;

    /**
	 * Стратегия секционирования; 
	 * l = секционирование по списку (List), r = секционирование по диапазонам (Range).
	 * **/
	private String partstrat;
	/**
	 * Это массив из partnatts значений, указывающих, какие столбцы таблицы входят в ключ разбиения.
	 * **/
	private List<Integer> partattrs;

    @Override
    public String toString() {
        return String.format(
            "PGPartitionedTable{partrelid=%s, partnatts=%s"
            + ", partstrat='%s', partattrs=%s}", 
            partrelid, partnatts, partstrat, partattrs);
    }
}
