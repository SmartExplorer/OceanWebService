package com.citi.ocean.restapi.datasource.base;

public class SolrQuery implements Query {

	private Shard shard;
	private String query;
	private int limit;

	public SolrQuery(Shard shard, String query, int limit) {
		super();
		this.shard = shard;
		this.query = query;
		this.limit = limit;
	}

	@Override
	public Shard getShard() {
		// TODO Auto-generated method stub
		return shard;
	}

	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return query;
	}

	public int getLimit() {
		return limit;
	}
	
	

}
