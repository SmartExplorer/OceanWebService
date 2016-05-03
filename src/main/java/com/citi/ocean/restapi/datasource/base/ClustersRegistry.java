package com.citi.ocean.restapi.datasource.base;

import java.util.List;

public class ClustersRegistry {
	
	private List<Cluster> clustersList;
	
	public ClustersRegistry(List<Cluster> clustersList) {
		this.clustersList = clustersList;
	}
	
	public List<Cluster> getClustersList() {
		return clustersList;
	}
}
