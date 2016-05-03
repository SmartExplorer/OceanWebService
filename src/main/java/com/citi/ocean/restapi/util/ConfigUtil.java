package com.citi.ocean.restapi.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.citi.ocean.restapi.datasource.NetezzaEndpoint;
import com.citi.ocean.restapi.datasource.SolrEndpoint;
import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.ClustersRegistry;
import com.citi.ocean.restapi.datasource.base.Shard;
import com.citi.ocean.restapi.datasource.base.TableSchemaRegistry;
import com.citi.ocean.restapi.handler.AggTradesHandler;
import com.citi.ocean.restapi.handler.FilterValuesHandler;
import com.citi.ocean.restapi.handler.QueryHintHandler;
import com.citi.ocean.restapi.handler.QueryTradesHandler;
import com.citi.ocean.restapi.tuple.ShardEndpointDetails;

public class ConfigUtil {

	private static ApplicationContext appContext = null;
	private static TableSchemaRegistry tableRegistry = null;
	private static ClustersRegistry clusterRegistry = null;

	public final static String NETEZZA_WORKER_TOPIC = "NetezzaQuery";
	public final static String SOLR_WORKER_TOPIC = "SolrQuery";

	public final static String REPLY_TOPIC = "ReplyTopic";
	public final static String LIMIT = "limit";
	
	public final static  String URL = "url";
	public final static  String USER = "user";
	public final static  String PASSWORD = "password";

	public final static Set<String> SCHEMA_LIST = new HashSet<String>(getTableSchemaRegistry().getTableNames());

	/* Get cluster list from configuration for this handler */
	// TODO Should be integrated with Spring ClustersRegistry config
	public static List<Cluster> getCluster(Class<?> clazz) {
		List<Cluster> clusters = new ArrayList<Cluster>();
		// TODO need to handle agg cluster and query cluster separatedly
		if (clazz.equals(QueryTradesHandler.class) || clazz.equals(AggTradesHandler.class)
				|| clazz.equals(FilterValuesHandler.class)) {
			clusters.add(getCluster(Cluster.CLUSTER_TYPE_NETEZZA));
		} else if (clazz.equals(QueryHintHandler.class)) {
			clusters.add(getCluster(Cluster.CLUSTER_TYPE_SOLR));
		}

		return clusters;
	}

	public static Cluster getCluster(String type) {
		return getClustersRegistry().getClustersList().stream().filter(cluster -> cluster.getType().equals(type))
				.findFirst().get();
	}

	public static ClustersRegistry getClustersRegistry() {
		if (clusterRegistry == null) {
			clusterRegistry = getApplicationContext().getBean(ClustersRegistry.class);
		}
		return clusterRegistry;
	}

	public static TableSchemaRegistry getTableSchemaRegistry() {
		if (tableRegistry == null) {
			tableRegistry = getApplicationContext().getBean(TableSchemaRegistry.class);
		}
		return tableRegistry;
	}

	private static synchronized ApplicationContext getApplicationContext() {
		if (appContext == null) {
			appContext = new ClassPathXmlApplicationContext("application-context.xml");
		}
		return appContext;
	}

	public static List<ShardEndpointDetails<SolrEndpoint>> getSolrConfig() {
		List<ShardEndpointDetails<SolrEndpoint>> result = new ArrayList<ShardEndpointDetails<SolrEndpoint>>();		
		getCluster(Cluster.CLUSTER_TYPE_SOLR).getShards().stream()
				.map(shard -> Shard.getShardEndpointsDetails(shard, SolrEndpoint.class))				
				.forEach(x -> result.addAll(x));
		return result;
	}
	
	public static List<ShardEndpointDetails<NetezzaEndpoint>> getNetezzaConfig() {		
		List<ShardEndpointDetails<NetezzaEndpoint>> result = new ArrayList<ShardEndpointDetails<NetezzaEndpoint>>();		
		getCluster(Cluster.CLUSTER_TYPE_NETEZZA).getShards().stream()
				.map(shard -> Shard.getShardEndpointsDetails(shard, NetezzaEndpoint.class))				
				.forEach(x -> result.addAll(x));
		return result;
	}
}
