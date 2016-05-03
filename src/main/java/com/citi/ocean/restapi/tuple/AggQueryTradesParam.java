package com.citi.ocean.restapi.tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.citi.ocean.restapi.datasource.base.NetezzaQuery;

import io.vertx.ext.web.RoutingContext;

public class AggQueryTradesParam extends QueryTradesParam {
	private String[] aggFields;
	private String[] aggValues;
	private String[] aggOperations;

//	public AggQueryTradesParam(RoutingContext routingContext, String userId, String category, List<String> aggFields,
//			List<String> aggValues, List<String> aggOperations, Map<String, FilterParam> filters) {
//		super(routingContext, userId, category, null, -1, filters);
//		this.aggFields = aggFields.stream().map(NetezzaQuery::getColumnName)
//				.toArray(size -> new String[size]);
//		this.aggValues = aggValues.stream().map(NetezzaQuery::getColumnName)
//				.toArray(size -> new String[size]);
//		this.aggOperations = aggOperations.stream().map(String::toUpperCase)
//				.toArray(size -> new String[size]);
//	}

	public AggQueryTradesParam(RoutingContext routingContext, String userId, String category, List<String> aggFields,
			List<String> aggValues, List<String> aggOperations, Map<String, FilterParam> filters, Optional<String[]> orderByAsc,
			Optional<String[]> orderByDesc) {
		super(routingContext, userId, category, null, -1, filters, orderByAsc, orderByDesc);
		this.aggFields = aggFields.stream().map(NetezzaQuery::getColumnName)
				.toArray(size -> new String[size]);
		this.aggValues = aggValues.stream().map(NetezzaQuery::getColumnName)
				.toArray(size -> new String[size]);
		this.aggOperations = aggOperations.stream().map(String::toUpperCase)
				.toArray(size -> new String[size]);

	}

	/**
	 * @return the aggOperations
	 */
	public String[] getAggOperations() {
		return aggOperations;
	}

	/**
	 * @return the aggFields
	 */
	public String[] getAggFields() {
		return aggFields;
	}

	/**
	 * @return the aggValues
	 */
	public String[] getAggValues() {
		return aggValues;
	}

}
