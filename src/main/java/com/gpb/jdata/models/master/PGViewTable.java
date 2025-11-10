package com.gpb.jdata.models.master;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PGViewTable {
	private PGViewTableId id;
	private String connector;
	private List<PGColumns> columns;
	private String tableDescription;
	private String tableType;
	private String constraint;
	private List<String> constraints;
	private String viewDefinition;
}
