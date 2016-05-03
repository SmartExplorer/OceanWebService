package com.citi.ocean.restapi.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.NetezzaQuery;
import com.citi.ocean.restapi.datasource.base.NetezzaShard;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.tuple.AggQueryTradesParam;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.FilterParam.DateFilterParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.util.ExceptionUtil;
import com.citi.ocean.restapi.validator.Validator;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

public class AggTradesHandlerTest {

	
	@Before
	public void setup() {


	}
	
	@Test
	public void testSelectCluster() {
		AggQueryTradesParam param = mock(AggQueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		@SuppressWarnings("unchecked")
		Map<String,FilterParam> filters = mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		DateFilterParam date = new DateFilterParam("TRADE_DATE_FROM", new String[] {"20150901","20150930"});
		when(filters.get("TRADE_DATE_FROM")).thenReturn(date);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		when(filters.get("FIRM_GFCID")).thenReturn(gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		when(filters.get("UITID")).thenReturn(uitid);
		
		Cluster cluster = AggTradesHandler.selectCluster(param);
		Assert.assertEquals("NETEZZA", cluster.getName());
		Assert.assertEquals("NETEZZA", cluster.getType());
	}
	
	@Test
	public void testBuildQuery() {
		AggQueryTradesParam param = mock(AggQueryTradesParam.class);
		
		when(param.getAggFields()).thenReturn(new String[] {"FIRM_GFCID", "TRADE_DATE"});
		when(param.getAggValues()).thenReturn(new String[] {"EOD_VALUE_USD"});
		when(param.getAggOperations()).thenReturn(new String[] {"sum"});
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(null));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(null));
		
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();
//		DateFilterParam dateFilter_from = new DateFilterParam("TRADE_DATE_FROM", new String[] {"20150901"});
//		DateFilterParam dateFilter_to = new DateFilterParam("TRADE_DATE_TO", new String[] {"20150930"});
		DateFilterParam dateFilter = new DateFilterParam("TRADE_DATE", new String[] {"20150901","20150930"});
//		filters.put("TRADE_DATE_FROM", dateFilter_from);
//		filters.put("TRADE_DATE_TO", dateFilter_to);
		filters.put("TRADE_DATE", dateFilter);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		filters.put("FIRM_GFCID", gfcid);
		
		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = AggTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("SELECT FIRM_GFCID,TRD_DATE,SUM(EOD_VALUE_USD) FROM OTC_DERIV_TRADE_MASTER WHERE TRD_DATE BETWEEN '20150901' AND '20150930' AND FIRM_GFCID IN ('AAA','BBB','CCC') GROUP BY FIRM_GFCID,TRD_DATE"));
	}
	
	@Test
	public void testBuildOrderQuery() {
		AggQueryTradesParam param = mock(AggQueryTradesParam.class);
		
		when(param.getAggFields()).thenReturn(new String[] {"FIRM_GFCID", "TRADE_DATE"});
		when(param.getAggValues()).thenReturn(new String[] {"EOD_VALUE_USD"});
		when(param.getAggOperations()).thenReturn(new String[] {"sum"});
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(null));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(new String[] {"FIRM_GFCID", "TRADE_DATE"}));
		
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();
		DateFilterParam dateFilter = new DateFilterParam("TRADE_DATE", new String[] {"20150901","20150930"});
		filters.put("TRADE_DATE", dateFilter);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		filters.put("FIRM_GFCID", gfcid);
		
		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = AggTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("SELECT FIRM_GFCID,TRD_DATE,SUM(EOD_VALUE_USD) FROM OTC_DERIV_TRADE_MASTER WHERE TRD_DATE BETWEEN '20150901' AND '20150930' AND FIRM_GFCID IN ('AAA','BBB','CCC') GROUP BY FIRM_GFCID,TRD_DATE ORDER BY FIRM_GFCID DESC,TRD_DATE DESC"));
	}

	@Test
	public void testBuildDescAscOrderQuery() {
		AggQueryTradesParam param = mock(AggQueryTradesParam.class);
		
		when(param.getAggFields()).thenReturn(new String[] {"FIRM_GFCID", "TRADE_DATE"});
		when(param.getAggValues()).thenReturn(new String[] {"EOD_VALUE_USD"});
		when(param.getAggOperations()).thenReturn(new String[] {"sum"});
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(new String[] {"SUM(EOD_VALUE_USD)"}));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(new String[] {"FIRM_GFCID", "TRADE_DATE"}));
		
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();
		DateFilterParam dateFilter = new DateFilterParam("TRADE_DATE", new String[] {"20150901","20150930"});
		filters.put("TRADE_DATE", dateFilter);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		filters.put("FIRM_GFCID", gfcid);
		
		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = AggTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("SELECT FIRM_GFCID,TRD_DATE,SUM(EOD_VALUE_USD) FROM OTC_DERIV_TRADE_MASTER WHERE TRD_DATE BETWEEN '20150901' AND '20150930' AND FIRM_GFCID IN ('AAA','BBB','CCC') GROUP BY FIRM_GFCID,TRD_DATE ORDER BY SUM(EOD_VALUE_USD),FIRM_GFCID DESC,TRD_DATE DESC"));
	}
	
	@Test
	public void testParseDescAscOrderQuery_exception() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_INVALID_ORDER_FIELD);
		
		HttpServerRequest request = mock(HttpServerRequest.class);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry(Validator.HTTP_PARAM_AGGREGATE_FIELDS, "FIRM_GFCID,TRADE_DATE"),
				entry(Validator.HTTP_PARAM_AGGREGATE_VALUES, "NOTIONAL1_CRCY"),
				entry(Validator.HTTP_PARAM_AGGREGATE_OPERATIONS, "sum"),
				entry(Validator.HTTP_PARAM_CATEGORY, "OTC_DERIV_TRADE_MASTER"),
				entry(Validator.HTTP_PARAM_ORDER_BY_DESC, "NOTIONAL1_CRCY,TRADE_DATE"))
                .collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseDescAscOrderQuery_pass() {
		
		HttpServerRequest request = mock(HttpServerRequest.class);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry(Validator.HTTP_PARAM_AGGREGATE_FIELDS, "FIRM_GFCID,TRADE_DATE"),
				entry(Validator.HTTP_PARAM_AGGREGATE_VALUES, "NOTIONAL2_AMT"),
				entry(Validator.HTTP_PARAM_AGGREGATE_OPERATIONS, "sum"),
				entry(Validator.HTTP_PARAM_CATEGORY, "OTC"),
				entry(Validator.HTTP_PARAM_TRADE_DATE_RANGE, "10"),
				entry(Validator.HTTP_PARAM_ORDER_BY_DESC, "SUM(NOTIONAL2_AMT),TRADE_DATE"),
				entry(Validator.HTTP_PARAM_NAME_USERID, "userid"))
                .collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
	}	
	@Test
	public void testBuildAverageQuery() {
		AggQueryTradesParam param = mock(AggQueryTradesParam.class);
		
		when(param.getAggFields()).thenReturn(new String[] {"TRD_DATE","SRC_SYS"});
		when(param.getAggValues()).thenReturn(new String[] {"NOTIONAL1_CRCY", "NOTIONAL1_AMT"});
		when(param.getAggOperations()).thenReturn(new String[] {"COUNT","AVERAGE"});
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(null));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(null));
		
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();
		DateFilterParam dateFilter = new DateFilterParam("TRADE_DATE", new String[] {"20151111","20151118"});
		filters.put("TRADE_DATE", dateFilter);
		
		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = AggTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("select TRD_DATE,SRC_SYS,COUNT(NOTIONAL1_CRCY),AVG(NOTIONAL1_AMT) from OTC_DERIV_TRADE_MASTER where TRD_DATE between '20151111' and '20151118' group by TRD_DATE,SRC_SYS"));
	}
	@Test
	public void testQueryShard() {
		
	}
	
	@Test
	public void testQueryShards() {
		
	}
	
	@Test
	public void testSendData() {
		
	}
	@Rule
	public ExpectedException expectedException = org.junit.rules.ExpectedException.none();
	
	/**
	 * Aggregation Query test
	 */
	@Test
	public void testParseAggHttpRequestPass() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertNotNull(queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE));
		Assert.assertEquals("20150901", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getStartDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals("20150930", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getEndDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals(-1, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), -1);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}
	
	@Test
	public void testParseAggHttpRequestFailIllegalAggField() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_AGGFIELD);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE_TO","TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count", "sum", "average", "sum");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE_TO,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average,sum"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertNotNull(queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE));
		Assert.assertEquals("20150901", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getStartDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals("20150930", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getEndDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals(-1, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), -1);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}
	
	public void testParseAggHttpRequestFailMismatchAggField() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISMATCH);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE_TO","TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count", "sum", "average", "sum");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE_TO,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average,sum"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertNotNull(queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE));
		Assert.assertEquals("20150901", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getStartDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals("20150930", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getEndDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals(-1, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), -1);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}
	
	public void testParseAggHttpRequestFailIllegalAggValue() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_AGGREGATION_VALUE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE_TO","TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count", "sum", "average", "sum");
		when(request.getParam("aggValues")).thenReturn("abcdefg,TRADE_QUANTITY");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE_TO,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average,sum"),
        		entry("aggValues","abcdefg,TRADE_QUANTITY")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertNotNull(queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE));
		Assert.assertEquals("20150901", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getStartDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals("20150930", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getEndDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals(-1, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), -1);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}
	
	public void testParseAggHttpRequestFailIllegalAggoperation() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_AGGOPERATION);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE_TO","TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("hahahah");
		when(request.getParam("aggValues")).thenReturn("TRADE_QUANTITY");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE_TO,TRADE_TYPE"),
        		entry("aggOperations", "hahahah"),
        		entry("aggValues","TRADE_QUANTITY")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		AggQueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertNotNull(queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE));
		Assert.assertEquals("20150901", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getStartDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals("20150930", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getEndDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals(-1, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), -1);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}
	
	/**
	 * Test on QueryParameters, adopted from QueryTradeTest and QueryTradeTestAdv
	 * 
	 */
	
	@Test
	public void testParseHttpRequestFailDateOrderAdv() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATE_RANGE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20250901");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_TO", "20150930"),
				entry("TRADE_DATE_FROM", "20250901")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	@Test
	public void testParseAggHttpRequestPass1() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		QueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertNotNull(queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE));
		Assert.assertEquals("20150901", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getStartDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals("20150930", 
				((DateFilterParam)queryParam.getFilters().get(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.getEndDate().get().format(Validator.DATE_FORMAT).toString());
		Assert.assertEquals(-1, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), -1);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}

	
	@Test
	public void testParseAggHttpRequestFailDateFormat() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("2015-09-01");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("2015-09-30");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"),
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "2015-09-01"),
                entry("TRADE_DATE_TO", "2015-09-30"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseAggHttpRequestFailMissingAggParams() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISSING);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("userId")).thenReturn("hw72786");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"),
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseAggHttpRequestFailMissingDate() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_DATE_RANGE_INCOMPLETE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"),
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_TO", "20150930"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseAggHttpRequestDefaultRowLimitPass() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		QueryTradesParam queryParam = AggTradesHandler.parseHttpRequest(routingContext);
		Assert.assertEquals(-1, queryParam.getLimit());
	}
	
	@Test
	public void testParseAggHttpRequestFailDateValue() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(Validator.HTTP_PARAM_START_DATE
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"),
				entry("category", "OTC"),
				entry("TRADE_DATE_TO", "20151430"),
				entry("TRADE_DATE_FROM", "20152501"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	/**
	 * Trade_date_range match integer
	 */
	public void testParseAggHttpRequestFailParamDataType1() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(Validator.HTTP_PARAM_TRADE_DATE_RANGE
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("TRADE_DATE_RANGE", "20151430abc"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	/**
	 * OCEAN_PROCESS_DATE match date
	 */
	@Test
	public void testParseAggHttpRequestFailParamDataType2() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("OCEAN_PROCESS_DATE"
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"),
				entry("category", "OTC"),
				entry("OCEAN_PROCESS_DATE", "2015-14-30"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	/**
	 * OCEAN_PROCESS_DATE match date
	 */
	@Test
	public void testParseAggHttpRequestPassParamDataType() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("OCEAN_PROCESS_DATE", "20151230"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}

	/**
	 * maxRows match int
	 */
	@Test
	public void testParseAggHttpRequestPassParamDataType2() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
//		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
//		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
//		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("maxRows", "123"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	/**
	 * maxRows match int
	 */
	@Test
	public void testParseAggHttpRequestFailParamDataType3() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(Validator.HTTP_PARAM_TRADE_DATE_RANGE
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"),
				entry("category", "OTC"),
				entry("TRADE_DATE_RANGE", "as"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseAggHttpRequestFailInvalidParams() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("aggFields")).thenReturn("TRADE_DATE,TRADE_TYPE");
		when(request.getParam("aggOperations")).thenReturn("count,sum,average");
		when(request.getParam("aggValues")).thenReturn("NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("NAME_OF_CITI_CEO", "MIKE"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
        		entry("aggFields", "TRADE_DATE,TRADE_TYPE"),
        		entry("aggOperations", "count,sum,average"),
        		entry("aggValues","NOTIONAL1_AMT,TRADE_QUANTITY,SECURITY_MULTIPLIER"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		AggTradesHandler.parseHttpRequest(routingContext);
	}
	
	
	
	
	public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue());
    }

    public static <K, U> Collector<Map.Entry<K, U>, ?, ConcurrentMap<K, U>> entriesToConcurrentMap() {
        return Collectors.toConcurrentMap((e) -> e.getKey(), (e) -> e.getValue());
    }
}
