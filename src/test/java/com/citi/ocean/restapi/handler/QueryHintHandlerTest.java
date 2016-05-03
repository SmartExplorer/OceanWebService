package com.citi.ocean.restapi.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.datasource.base.SolrQuery;
import com.citi.ocean.restapi.datasource.base.SolrShard;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.validator.Validator;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

public class QueryHintHandlerTest {
	@Before
	public void setup() {


	}
	
	@Test
	public void testParseHttpRequestPass() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam(Validator.HTTP_PARAM_VALUE)).thenReturn("test");
		when(request.getParam(Validator.HTTP_PARAM_NAME_USERID)).thenReturn("et12757");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				StaticTestMap.entry(Validator.HTTP_PARAM_NAME_USERID, "et12757"),
				StaticTestMap.entry(Validator.HTTP_PARAM_VALUE, "test")).
                collect(StaticTestMap.entriesToMap())));
		when(request.params()).thenReturn(map);		
		QueryTradesParam queryParam = QueryHintHandler.parseHttpRequest(routingContext);
		Assert.assertEquals(1, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("value"));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testParseHttpRequestFailMissingParams() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(new HashMap());
		when(request.params()).thenReturn(map);
		
		QueryTradesParam queryParam = QueryHintHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testValidatePass() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		Map<String,FilterParam> filters = mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		FilterParam value = new FilterParam("value", new String[] {"test"});
		when(filters.get("value")).thenReturn(value);

		
		QueryHintHandler.validate(param);
	}
	
	@Test
	public void testSelectCluster() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		Map<String,FilterParam> filters =  mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		FilterParam value = new FilterParam("value", new String[] {"test"});
		when(filters.get("value")).thenReturn(value);
		
		Cluster cluster = QueryHintHandler.selectCluster(param);
		Assert.assertEquals("SOLR", cluster.getName());
		Assert.assertEquals("SOLR", cluster.getType());
	}
	@Test
	public void testBuildQuery() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		
		Map<String,FilterParam> filters =  mock(Map.class);
		FilterParam value = new FilterParam("value", new String[] {"test"});
		when(filters.get("value")).thenReturn(value);

		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = mock(Cluster.class);
		when(cluster.getShards()).thenReturn(Arrays.asList(new SolrShard(ConfigUtil.SOLR_WORKER_TOPIC,null)));
		when(cluster.getType()).thenReturn("SOLR");
		List<Query> queries = QueryHintHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof SolrQuery);

		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("test"));
	}

}
