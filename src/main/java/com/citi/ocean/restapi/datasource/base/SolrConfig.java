package com.citi.ocean.restapi.datasource.base;

public class SolrConfig {
	private String solrUrl;
	private String solrWorkerTopic;
	private int instance;
	
	
	public String getSolrUrl() {
		return solrUrl;
	}
	public void setSolrUrl(String solrUrl) {
		this.solrUrl = solrUrl;
	}
	public int getInstance() {
		return instance;
	}
	public void setInstance(int instance) {
		this.instance = instance;
	}
	public String getSolrWorkerTopic() {
		return solrWorkerTopic;
	}
	public void setSolrWorkerTopic(String solrWorkerTopic) {
		this.solrWorkerTopic = solrWorkerTopic;
	}
	
	

}
