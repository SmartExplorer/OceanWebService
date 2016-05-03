package com.citi.ocean.restapi.datasource.base;

import java.util.List;
import java.util.stream.Collectors;

import com.citi.ocean.restapi.tuple.ShardEndpointDetails;

public interface Shard {

	String getTopic();
	String getTableName();
	List<ShardEndpoint> getEndpoints();

	@SuppressWarnings("unchecked")
	public static <T extends ShardEndpoint> List<ShardEndpointDetails<T>> getShardEndpointsDetails(Shard shard, Class<T> clazz) {
		return shard.getEndpoints().stream().map(x -> new ShardEndpointDetails<T>(shard, (T)x)).collect(Collectors.toList());
	}
}