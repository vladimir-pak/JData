package com.gpb.jdata.utils.writer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DatabaseService {
	private String name;
	private String displayName;
	private String serviceType;
	private String description;
}
