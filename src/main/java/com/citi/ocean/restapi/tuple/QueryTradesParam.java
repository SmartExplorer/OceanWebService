package com.citi.ocean.restapi.tuple;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import com.citi.ocean.restapi.datasource.base.NetezzaQuery;

import io.vertx.ext.web.RoutingContext;

public class QueryTradesParam {

	private RoutingContext routingContext;
	private String userId;
	private String category;
	private String[] columns;
	private int limit;
	private Map<String, FilterParam> filters;
	private Optional<String[]> orderByAsc;
	private Optional<String[]> orderByDesc;
	private long requestTime;

	public QueryTradesParam(RoutingContext routingContext, String userId, String category, String[] columns, int limit,
			Map<String, FilterParam> filters) {
		this.routingContext = routingContext;
		this.userId = userId;
		this.category = category;
		this.columns = columns != null
				? Arrays.asList(columns).stream().map(NetezzaQuery::getColumnName).toArray(size -> new String[size])
				: null;
		this.limit = limit;
		this.filters = filters;
		this.orderByAsc = Optional.ofNullable(null);
		this.orderByDesc = Optional.ofNullable(null);
		this.requestTime = System.currentTimeMillis();
	}

	public QueryTradesParam(RoutingContext routingContext, String userId, String category, String[] columns, int limit,
			Map<String, FilterParam> filters, Optional<String[]> orderByAsc, Optional<String[]> orderByDesc) {
		this(routingContext, userId, category, columns, limit, filters);
		this.orderByAsc = orderByAsc;
		this.orderByDesc = orderByDesc;
	}

	public RoutingContext getRoutingContext() {
		return routingContext;
	}

	public String getUserId() {
		return userId;
	}

	public String[] getColumns() {
		return columns;
	}

	public int getLimit() {
		return limit;
	}

	public Map<String, FilterParam> getFilters() {
		return filters;
	}

	public String getCategory() {
		return category;
	}

	public Optional<String[]> getOrderByAsc() {
		return orderByAsc;
	}

	public Optional<String[]> getOrderByDesc() {
		return orderByDesc;
	}

	public long getRequestTime() {
		return requestTime;
	}

}
