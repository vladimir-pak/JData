package com.gpb.jdata.models.master;

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
@Table(name = "transactions_journal")
@AllArgsConstructor
@NoArgsConstructor
public class TransactionsJournal implements Serializable {
	private static final long serialVersionUID = 5593842846947497275L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	private Long id;

	@Column(name = "request")
	private String request;

	@Column(name = "flag")
	private Boolean flag;

	@Column(name = "response")
	private Long response;

	@Column(name = "message")
	private String message;

	@Column(name = "db")
	private String db;

	@Column(name = "timestamp")
	private Timestamp timestamp;
}
