package com.citi.ocean.restapi.datasource.base;

import java.util.List;
import java.util.Map;

/* This encapsulates KDB shard configuration*/
public class KDBShard implements Shard {

	private String topic;
	// TOREVIEW: Whether we need to associate shard with table name 
	private String tableName;
	private List<ShardEndpoint> endpoints;
	private Map<String, List<String>> sharedCriteria;
	
	public KDBShard() {
	}
	
	public KDBShard(String topic, String tableName, Map<String, List<String>> sharedCriteria) {
		this.topic = topic;
		this.tableName = tableName;
		this.sharedCriteria = sharedCriteria;
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

	public Map<String, List<String>> getSharedCriteria() {
		return sharedCriteria;
	}
	
	public void setSharedCriteria(Map<String, List<String>> sharedCriteria) {
		this.sharedCriteria = sharedCriteria;
	}
	
	@Override
	public List<ShardEndpoint> getEndpoints() {
		return endpoints;
	}
	
	public void setEndpoints(List<ShardEndpoint> endpoints) {
		this.endpoints = endpoints;
	}
}