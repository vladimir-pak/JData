package com.gpb.jdata.models.master;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PGViewTableId {
	private String db;
	private String schema;
	private String tableName;
}
