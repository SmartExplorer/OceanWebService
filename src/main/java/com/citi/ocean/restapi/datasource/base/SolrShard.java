package com.citi.ocean.restapi.datasource.base;

import java.util.List;

public class SolrShard implements Shard {

	private String topic;
	private String tableName;
	private List<ShardEndpoint> endpoints;

	public SolrShard() {
		super();
	}

	public SolrShard(String topic, String tableName) {
		super();
		this.topic = topic;
		this.tableName = tableName;
	}

	@Override
	public String getTopic() {
		// TODO Auto-generated method stub
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return tableName;
	}

	public void setEndpoints(List<ShardEndpoint> endpoints) {
		this.endpoints = endpoints;
	}

	@Override
	public List<ShardEndpoint> getEndpoints() {
		return endpoints;
	}
}
