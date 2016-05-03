package com.citi.ocean.restapi.datasource.base;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TableSchema {

	private String tableName;
	private Map<String, String> columnTypes;
	private String[] columnList;
	private Map<String, String> viewOnlyColumns;
	
	public TableSchema() {
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setColumnTypes(Map<String, String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public String getColumnType(String columnName) {
		if (columnTypes.containsKey(columnName)) {
			return columnTypes.get(columnName);
		} else {
			return null;
		}
	}

	public String[] getColumnListInOrder() {
		return columnList;
	}

	public Map<String, String> getColumnTypes() {
		return columnTypes;
	}

	public String getColumnList() {
		return columnTypes.keySet().stream().collect(Collectors.joining(","));
	}
		
	public void setColumnList(String[] columnList) {
		this.columnList = columnList;
	}
	
	public Set<String> getColumnSet() {
		return columnTypes.keySet();
	}

	public Map<String, String> getViewOnlyColumns() {
		return viewOnlyColumns;
	}

	public void setViewOnlyColumns(Map<String, String> viewOnlyColumns) {
		this.viewOnlyColumns = viewOnlyColumns;
	}
}