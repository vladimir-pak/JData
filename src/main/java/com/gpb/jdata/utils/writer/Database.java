package com.gpb.jdata.utils.writer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Database {
	private String name;
	private String displayName;
	private String service;
	private String description;
}
