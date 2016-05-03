package com.citi.ocean.restapi.datasource.base;

import java.util.List;

public class NetezzaShard implements Shard {
	
	private String topic;
	// TOREVIEW: Whether we need to associate shard with table name
	private String tableName;
	private List<ShardEndpoint> endpoints;
	
	public NetezzaShard() {
	}
	
	public NetezzaShard(String topic, String tableName) {
		this.topic = topic;
		this.tableName = tableName;
	}

	public String getTopic() {
		return topic;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public List<ShardEndpoint> getEndpoints() {
		return endpoints;
	}
	
	public void setEndpoints(List<ShardEndpoint> endpoints) {
		this.endpoints = endpoints;
	}
}
