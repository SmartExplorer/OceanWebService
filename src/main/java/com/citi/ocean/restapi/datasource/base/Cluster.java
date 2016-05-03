package com.citi.ocean.restapi.datasource.base;

import java.beans.ConstructorProperties;
import java.util.List;

public class Cluster {
	
	public static String CLUSTER_TYPE_KDB = "KDB";
	public static String CLUSTER_TYPE_NETEZZA = "NETEZZA";
	public static String CLUSTER_TYPE_IMPALA = "IMPALA";
	public static String CLUSTER_TYPE_SOLR = "SOLR";

	private String name;
	private String type;
	private List<Shard> shards;
	
	@ConstructorProperties({"name", "type", "shards"})
	public Cluster(String name, String type, List<Shard> shards) {
		this.name = name;
		this.shards = shards;
		this.type = type;
	}

	public String getName() {
		return name;
	}
	
	public String getType() {
		return type;
	}
	
	public List<Shard> getShards() {
		return shards;
	}
}
