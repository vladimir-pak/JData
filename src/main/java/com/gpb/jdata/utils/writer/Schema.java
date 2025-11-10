package com.gpb.jdata.utils.writer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Schema {
	private String database;
	private String description;
	private String displayName;
	private String name;
}
