package com.citi.ocean.restapi.tuple;

import com.citi.ocean.restapi.datasource.base.Shard;
import com.citi.ocean.restapi.datasource.base.ShardEndpoint;

public class ShardEndpointDetails<T extends ShardEndpoint> {
	
	private Shard shard;
	private T endpoint;
	
	public ShardEndpointDetails(Shard shard, T endpoint) {
		this.shard = shard;
		this.endpoint = endpoint;
	}
	
	public Shard getShard() {
		return shard;
	}
	
	public T getEndpoint() {
		return endpoint;
	}
}
