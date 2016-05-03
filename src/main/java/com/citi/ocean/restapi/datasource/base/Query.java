package com.citi.ocean.restapi.datasource.base;

public interface Query {

	public Shard getShard();
	public String getQuery();
	public default String getCountQuery() {
		return "";
	}
}
