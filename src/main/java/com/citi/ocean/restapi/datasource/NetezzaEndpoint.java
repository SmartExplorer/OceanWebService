package com.citi.ocean.restapi.datasource;

import com.citi.ocean.restapi.datasource.base.ShardEndpoint;

public class NetezzaEndpoint implements ShardEndpoint {
	
	private String url;
	private String user;
	private String password;
	private int connectionsCount;
	
	public NetezzaEndpoint() {
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setConnectionsCount(int connectionsCount) {
		this.connectionsCount = connectionsCount;
	}

	@Override
	public int getConnectionsCount() {
		return connectionsCount;
	}
}
