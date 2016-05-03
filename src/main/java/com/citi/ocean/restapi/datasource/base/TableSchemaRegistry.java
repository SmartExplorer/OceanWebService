package com.citi.ocean.restapi.datasource.base;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableSchemaRegistry {
	
	private List<TableSchema> tableSchemas;
	
	public TableSchemaRegistry() {
	}
	
	public void setTableSchemas(List<TableSchema> tableSchemas) {
		this.tableSchemas = tableSchemas;
	}
	
	public TableSchema getTableSchema(String tableName) {
		Optional<TableSchema> res = tableSchemas
			.stream()
			.filter(x -> x.getTableName().equals(tableName))
			.findFirst();
		return res.isPresent() ? res.get() : null;
	}
	
	public List<String> getTableNames() {
		return tableSchemas.stream().map(x -> x.getTableName()).collect(Collectors.toList());
	}
}
