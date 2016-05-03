package com.citi.ocean.restapi.tuple;

import java.util.Map;

import io.vertx.ext.web.RoutingContext;

public class FilterValuesParam extends QueryTradesParam{

	private String filterField;
	
	public FilterValuesParam(RoutingContext routingContext, String userId,String category, String filterField,
			Map<String, FilterParam> filters) {
		super(routingContext, userId,category, null, 0, filters);
		this.filterField = filterField;
	}

	public String getFilterField() {
		return filterField;
	}

	public void setFilterField(String filterField) {
		this.filterField = filterField;
	}
	
	

}
