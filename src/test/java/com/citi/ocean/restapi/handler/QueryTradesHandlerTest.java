package com.citi.ocean.restapi.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.NetezzaQuery;
import com.citi.ocean.restapi.datasource.base.NetezzaShard;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.datasource.base.TableSchema;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.FilterParam.DateFilterParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.ConfigUtil;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class QueryTradesHandlerTest {

	
	@Before
	public void setup() {


	}
	
	@Test
	public void testParseHttpRequestPass() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				StaticTestMap.entry("userId", "et12757"),
                StaticTestMap.entry("maxRows", "1000"),
                StaticTestMap.entry("TRADE_DATE_FROM", "20150901"),
                StaticTestMap.entry("TRADE_DATE_TO", "20150930"),
                StaticTestMap.entry("UITID", "12345,67890"),
                StaticTestMap.entry("FIRM_GFCID", "AAA,BBB,CCC"),
                StaticTestMap.entry("category", "OTC")).
                collect(StaticTestMap.entriesToMap())));
		when(request.params()).thenReturn(map);		
		QueryTradesParam queryParam = QueryTradesHandler.parseHttpRequest(routingContext);
		Assert.assertTrue("UserId does not match.", "et12757".equals(queryParam.getUserId()));
		//TODO: discuss columns
		Assert.assertNull(queryParam.getColumns());
		Assert.assertEquals(1000, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), 1000);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
		Assert.assertEquals("OTC_DERIV_TRADE_MASTER", queryParam.getCategory());
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testParseHttpRequestFailDateFormat() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("2015-09-01");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("2015-09-30");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
                StaticTestMap.entry("maxRows", "1000"),
                StaticTestMap.entry("TRADE_DATE_FROM", "2015-09-01"),
                StaticTestMap.entry("TRADE_DATE_TO", "2015-09-30")).
                collect(StaticTestMap.entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesParam queryParam = QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testParseHttpRequestFailMissingParams() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(new HashMap());
		when(request.params()).thenReturn(map);
		
		QueryTradesParam queryParam = QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testParseHttpRequestFailMissingDate() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
                StaticTestMap.entry("maxRows", "1000"),
                StaticTestMap.entry("TRADE_DATE_TO", "20150930")).
				collect(StaticTestMap.entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesParam queryParam = QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testValidatePass() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		Map<String,FilterParam> filters = mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		DateFilterParam dateFrom = new DateFilterParam("TRADE_DATE_FROM", new String[] {"20150901",""});
		when(filters.get("TRADE_DATE_FROM")).thenReturn(dateFrom);
		DateFilterParam dateTo = new DateFilterParam("TRADE_DATE_TO", new String[] {"","20150930"});
		when(filters.get("TRADE_DATE_TO")).thenReturn(dateTo);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		when(filters.get("FIRM_GFCID")).thenReturn(gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		when(filters.get("UITID")).thenReturn(uitid);

		
		QueryTradesHandler.validate(param);
	}
	
	@Test(expected=Exception.class)
	public void testValidateFailMissingDate() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		Map<String,FilterParam> filters = mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		when(filters.get("FIRM_GFCID")).thenReturn(gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		when(filters.get("UITID")).thenReturn(uitid);
		
		QueryTradesHandler.validate(param);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testValidateFailDateFormat() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		Map<String,FilterParam> filters = mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		DateFilterParam date = new DateFilterParam("TRADE_DATE_RANGE", new String[] {"2015-09-01","2015-09-30"});
		when(filters.get("TRADE_DATE_FROM")).thenReturn(date);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		when(filters.get("FIRM_GFCID")).thenReturn(gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		when(filters.get("UITID")).thenReturn(uitid);

		
		QueryTradesHandler.validate(param);
	}
	
	@Test
	public void testSelectCluster() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		
		Map<String,FilterParam> filters = mock(Map.class);
		when(param.getFilters()).thenReturn(filters);
		
		DateFilterParam date = new DateFilterParam("TRADE_DATE_FROM", new String[] {"20150901","20150930"});
		when(filters.get("TRADE_DATE_FROM")).thenReturn(date);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		when(filters.get("FIRM_GFCID")).thenReturn(gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		when(filters.get("UITID")).thenReturn(uitid);
		
		Cluster cluster = QueryTradesHandler.selectCluster(param);
		Assert.assertEquals("NETEZZA", cluster.getName());
		Assert.assertEquals("NETEZZA", cluster.getType());
	}
	@Test
	public void testBuildQuery() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		when(param.getColumns()).thenReturn(new String[0]);
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(null));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(null));
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();
		DateFilterParam dateFilter = new DateFilterParam("TRADE_DATE", new String[] {"20150901", "20150930"});
		filters.put("TRADE_DATE", dateFilter);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		filters.put("FIRM_GFCID", gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		filters.put("UITID", uitid);

		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = QueryTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema("OTC_DERIV_TRADE_MASTER");
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("SELECT "+schema.getColumnList()+" FROM OTC_DERIV_TRADE_MASTER WHERE TRD_DATE BETWEEN '20150901' AND '20150930' AND FIRM_GFCID IN ('AAA','BBB','CCC') AND UITID IN ('12345','67890') LIMIT 100"));
		Assert.assertTrue(queries.get(0).getCountQuery().equalsIgnoreCase("SELECT COUNT(1) FROM OTC_DERIV_TRADE_MASTER WHERE TRD_DATE BETWEEN '20150901' AND '20150930' AND FIRM_GFCID IN ('AAA','BBB','CCC') AND UITID IN ('12345','67890')"));
	}

	@Test
	public void testBuildQuery_OceanProcessDate() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		when(param.getColumns()).thenReturn(new String[]{"OCEAN_PROCESS_DATE"});
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(null));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(null));
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();
		DateFilterParam dateFilter = new DateFilterParam("TRADE_DATE", new String[] {"20150901", "20150930"});
		filters.put("TRADE_DATE", dateFilter);
		
		FilterParam gfcid = new FilterParam("FIRM_GFCID", new String[] {"AAA","BBB", "CCC"});
		filters.put("FIRM_GFCID", gfcid);
		
		FilterParam uitid = new FilterParam("UITID", new String[] {"12345","67890"});
		filters.put("UITID", uitid);

		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = QueryTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("SELECT date(OCEAN_CREATE_TS) FROM OTC_DERIV_TRADE_MASTER WHERE TRD_DATE BETWEEN '20150901' AND '20150930' AND FIRM_GFCID IN ('AAA','BBB','CCC') AND UITID IN ('12345','67890') LIMIT 100"));
	}	
	@Test
	public void testBuildQuery_noCondition() {
		QueryTradesParam param = mock(QueryTradesParam.class);
		when(param.getLimit()).thenReturn(100);
		when(param.getColumns()).thenReturn(new String[0]);
		when(param.getCategory()).thenReturn("OTC_DERIV_TRADE_MASTER");
		when(param.getOrderByAsc()).thenReturn(Optional.ofNullable(null));
		when(param.getOrderByDesc()).thenReturn(Optional.ofNullable(null));
		
		Map<String,FilterParam> filters = new LinkedHashMap<String,FilterParam>();

		when(param.getFilters()).thenReturn(filters);

		Cluster cluster = new Cluster("NETEZZA", "NETEZZA", Arrays.asList(new NetezzaShard(ConfigUtil.NETEZZA_WORKER_TOPIC,"OTC_DERIV_TRADE_MASTER")));
		List<Query> queries = QueryTradesHandler.buildQuery(cluster, param);
		Assert.assertEquals(1, queries.size());
		Assert.assertTrue(queries.get(0) instanceof NetezzaQuery);
		TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema("OTC_DERIV_TRADE_MASTER");
		
		Assert.assertTrue(queries.get(0).getQuery().equalsIgnoreCase("SELECT "+schema.getColumnList()+" FROM OTC_DERIV_TRADE_MASTER LIMIT 100"));
	}


	@Test
	public void testQueryShard() {
		
	}
	
	@Test
	public void testQueryShards() {
		
	}
	
	@Test
	public void testSendData() {
		JsonObject root = new JsonObject();
		
		JsonObject dataRows = new JsonObject();		
		JsonArray columns = new JsonArray();
		
		columns.add("1").add("2").add("3456").add("AAA").add("BB").add("CZZZ");
		
		JsonArray rows = new JsonArray();
		rows.add((new JsonArray()).add("ID1").add("SIDE1").add(1234).add("20151130"));
		rows.add((new JsonArray()).add("ID2").add("SIDE11").add(0).add("20151130"));
		rows.add((new JsonArray()).add("ID3").add("SIDE3").add(999).add("20160102"));
		rows.add((new JsonArray()).add("ID4").add("SIDE4").add(-1).add("20000101"));
		
		dataRows.put("columns", columns);
		dataRows.put("rows", rows);
		
		root.put("TotalCount", 10);
		root.put("ExecutionTime", "20151030");
		root.put("DataRows", dataRows);
		
		System.out.println(root.encode());
	}
	
	@Test
	public void testBeanMapSequence() {
		TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema("OTC_DERIV_TRADE_MASTER");
		Map<String, String> m = schema.getColumnTypes();
		m.entrySet().stream().forEach(System.out::println);
		System.out.println(m.keySet().stream().collect(Collectors.joining(",")));
		Assert.assertTrue("OCEAN_ID,UITID,SRC_SYS_CONTRACT_ID,SRC_SYS_CONTRACT_VER,OASYS_DEAL_ID,SRC_SYS,PRODUCT_ID,BASE_PRODUCT_TYPE,PRIMARY_ASSET_CLASS,LIFECYCLE_STATE,EVENT_TYPE,ACTIVE_FLAG,TRD_DATE,TERM_DATE,MODIFICATION_TS,MESSAGE_CREATE_TS,OCEAN_CREATE_TS,NOTIONAL1_AMT,NOTIONAL1_CRCY,NOTIONAL2_AMT,NOTIONAL2_CRCY,FIRM_BUY_SELL_IND,FIRM_ACCT_MNEMONIC,MGD_SEG_LEVEL6,MGD_SEG_LEVEL7,MGD_SEG_LEVEL8,MGD_SEG_LEVEL9,MGD_SEG_LEVEL6_DESCR,MGD_SEG_LEVEL7_DESCR,MGD_SEG_LEVEL8_DESCR,MGD_SEG_LEVEL9_DESCR,FIRM_BRANCH_CODE,FIRM_GFCID,COUNTER_PARTY_ACCT_MNEMONIC,COUNTER_PARTY_GFCID,COUNTER_PARTY_BASE_NUMBER,TRADER_SOEID,SALESPERSON_SOEID,EVPERSON_SOEID"
				.equalsIgnoreCase(m.keySet().stream().collect(Collectors.joining(","))));
	}
	
	
	
	
}
