package com.citi.ocean.restapi.datasource;

import com.citi.ocean.restapi.datasource.base.ShardEndpoint;

public class KDBEndpoint implements ShardEndpoint {
	
	private String host;
	private int port;
	private String user;
	private String password;
	private int connectionsCount;
	
	public KDBEndpoint() {
	}
	
	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
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
