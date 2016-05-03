package com.citi.ocean.restapi.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.citi.ocean.restapi.tuple.FilterParam.DateFilterParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.ExceptionUtil;
import com.citi.ocean.restapi.validator.Validator;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

public class QueryTradesHandlerTestAdv {
	/**
	 * @author hw72786
	 */
	
	@Before
	public void setup() {


	}
	@Rule
	public ExpectedException expectedException = org.junit.rules.ExpectedException.none();
	
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
	public void testParseHttpRequestPassAdv() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		QueryTradesParam queryParam = QueryTradesHandler.parseHttpRequest(routingContext);
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
		Assert.assertEquals(1000, queryParam.getLimit());
		Assert.assertEquals(3, queryParam.getFilters().size());
		Assert.assertNotNull(queryParam.getFilters().get("FIRM_GFCID"));
		Assert.assertEquals(queryParam.getLimit(), 1000);
		Assert.assertEquals(3, queryParam.getFilters().get("FIRM_GFCID").getVals().length);
	}

	
	@Test
	public void testParseHttpRequestFailDateFormatAdv() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("2015-09-01");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("2015-09-30");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_FROM", "2015-09-01"),
                entry("TRADE_DATE_TO", "2015-09-30")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseHttpRequestFailMissingParamsAdv() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_MISSING_USERID);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(new HashMap());
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseHttpRequestFailMissingDateAdv() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_DATE_RANGE_INCOMPLETE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("maxRows", "1000"),
                entry("TRADE_DATE_TO", "20150930")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseHttpRequestDefaultRowLimitPass() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_FROM")).thenReturn("20150901");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		when(request.getParam("UITID")).thenReturn("12345,67890");
		when(request.getParam("FIRM_GFCID")).thenReturn("AAA,BBB,CCC");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"),
                entry("UITID", "12345,67890"),
                entry("FIRM_GFCID", "AAA,BBB,CCC")).
                collect(entriesToMap())));
		when(request.params()).thenReturn(map);		
		QueryTradesParam queryParam = QueryTradesHandler.parseHttpRequest(routingContext);
		Assert.assertEquals(10, queryParam.getLimit());
	}
	
	@Test
	public void testParseHttpRequestFailDateValueAdv() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(Validator.HTTP_PARAM_START_DATE
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		when(request.getParam("maxRows")).thenReturn("1000");
		when(request.getParam("TRADE_DATE_TO")).thenReturn("20150930");
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("TRADE_DATE_TO", "20151430"),
				entry("TRADE_DATE_FROM", "20152501"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	/**
	 * Trade_date_range match integer
	 */
	public void testParseHttpRequestFailParamDataType1() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(Validator.HTTP_PARAM_TRADE_DATE_RANGE
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("TRADE_DATE_RANGE", "20151430abc"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	/**
	 * OCEAN_PROCESS_DATE match date
	 */
	@Test
	public void testParseHttpRequestFailParamDataType2() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("OCEAN_PROCESS_DATE"
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("OCEAN_PROCESS_DATE", "2015-14-30"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	/**
	 * OCEAN_PROCESS_DATE match date
	 */
	@Test
	public void testParseHttpRequestPassParamDataType() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				//entry("OCEAN_PROCESS_DATE", "20151230"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}

	/**
	 * maxRows match int
	 */
	@Test
	public void testParseHttpRequestPassParamDataType2() {
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("maxRows", "123"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	/**
	 * maxRows match int
	 */
	@Test
	public void testParseHttpRequestFailParamDataType3() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(Validator.HTTP_PARAM_MAX_ROWS
				+ ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("maxRows", "12.453"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
	}
	
	@Test
	public void testParseHttpRequestFailInvalidParams() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM);
		HttpServerRequest request = mock(HttpServerRequest.class);
		RoutingContext routingContext = mock(RoutingContext.class);
		when(routingContext.request()).thenReturn(request);
		MultiMap map = new StaticTestMap(Collections.unmodifiableMap(Stream.of(
				entry("userId", "et12757"), 
				entry("category", "OTC"),
				entry("NAME_OF_CITI_CEO", "MIKE"),
                entry("TRADE_DATE_FROM", "20150901"),
                entry("TRADE_DATE_TO", "20150930"))
				.collect(entriesToMap())));
		when(request.params()).thenReturn(map);
		
		QueryTradesHandler.parseHttpRequest(routingContext);
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
//	
//	public class StaticTestMap implements MultiMap{
//
//		private Map<String, String> map;
//		
//		public StaticTestMap (Map<String, String> map) {
//			this.map = map;
//		}
//		
//		
//		@Override
//		public Iterator<Entry<String, String>> iterator() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public String get(CharSequence name) {
//			// TODO Auto-generated method stub
//			return map.get(name);
//		}
//
//		@Override
//		public String get(String name) {
//			// TODO Auto-generated method stub
//			return map.get(name);
//		}
//
//		@Override
//		public List<String> getAll(String name) {
//			// TODO Auto-generated method stub
//			return Arrays.asList(map.get(name));
//		}
//
//		@Override
//		public List<String> getAll(CharSequence name) {
//			// TODO Auto-generated method stub
//			return Arrays.asList(map.get(name));
//		}
//
//		@Override
//		public List<Entry<String, String>> entries() {
//			// TODO Auto-generated method stub
//			return new ArrayList<Entry<String, String>>(map.entrySet());
//		}
//
//		@Override
//		public boolean contains(String name) {
//			// TODO Auto-generated method stub
//			return map.containsValue(name);
//		}
//
//		@Override
//		public boolean contains(CharSequence name) {
//			// TODO Auto-generated method stub
//			return map.containsValue(name);
//		}
//
//		@Override
//		public boolean isEmpty() {
//			// TODO Auto-generated method stub
//			return map.isEmpty();
//		}
//
//		@Override
//		public Set<String> names() {
//			// TODO Auto-generated method stub
//			return map.keySet();
//		}
//
//		@Override
//		public MultiMap add(String name, String value) {
//			// TODO Auto-generated method stub
//			map.put(name, value);
//			return this;
//		}
//
//		@Override
//		public MultiMap add(CharSequence name, CharSequence value) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap add(String name, Iterable<String> values) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap add(CharSequence name, Iterable<CharSequence> values) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap addAll(MultiMap map) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap addAll(Map<String, String> headers) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap set(String name, String value) {
//			map.put(name, value);
//			return this;
//		}
//
//		@Override
//		public MultiMap set(CharSequence name, CharSequence value) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap set(String name, Iterable<String> values) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap set(CharSequence name, Iterable<CharSequence> values) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap setAll(MultiMap map) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap setAll(Map<String, String> headers) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public MultiMap remove(String name) {
//			// TODO Auto-generated method stub
//			map.remove(name);
//			return this;
//		}
//
//		@Override
//		public MultiMap remove(CharSequence name) {
//			// TODO Auto-generated method stub
//			map.remove(name);
//			return this;
//		}
//
//		@Override
//		public MultiMap clear() {
//			// TODO Auto-generated method stub
//			map.clear();
//			return this;
//		}
//
//		@Override
//		public int size() {
//			// TODO Auto-generated method stub
//			return map.size();
//		}
//		
//	}
}
