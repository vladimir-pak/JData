package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.sql.Timestamp;

import jakarta.persistence.Column;
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
@Table(name = "summary_statistics_rep")
@AllArgsConstructor
@NoArgsConstructor
public class Statistics implements Serializable {
	private static final long serialVersionUID = 8348851738046072205L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	private Long id;

	@Column(name = "db")
	private String db;

	@Column(name = "schema")
	private String schema;

	@Column(name = "table_name")
	private String table_name;

	@Column(name = "sum")
	private Long sum;

	@Column(name = "timestamp")
	private Timestamp timestamp;
}
