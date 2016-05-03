package com.citi.ocean.restapi.datasource;

import com.citi.ocean.restapi.datasource.base.ShardEndpoint;

public class SolrEndpoint implements ShardEndpoint{

	private String url;
	private int connectionsCount = 0;
	
	@Override
	public int getConnectionsCount() {
		// TODO Auto-generated method stub
		return connectionsCount;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setConnectionsCount(int connectionsCount) {
		this.connectionsCount = connectionsCount;
	}
}
