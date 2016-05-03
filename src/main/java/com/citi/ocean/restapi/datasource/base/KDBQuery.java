package com.citi.ocean.restapi.datasource.base;

public class KDBQuery implements Query{

	private KDBShard shard;
	private String query;
	
	public KDBQuery(KDBShard shard){
		this.shard = shard;
	}

	public Shard getShard(){
		return shard;
	}
	
	public String getQuery() {
		return query;
	}
	
}
