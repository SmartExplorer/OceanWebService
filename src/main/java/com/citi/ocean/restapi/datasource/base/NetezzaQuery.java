package com.citi.ocean.restapi.datasource.base;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.citi.ocean.restapi.tuple.AggQueryTradesParam;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.FilterParam.DateFilterParam;
import com.citi.ocean.restapi.tuple.FilterValuesParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.ConfigUtil;

public class NetezzaQuery implements Query {

	private Shard shard;
	private String query;
	private String countQuery;

	/**
	 * this internal map to support the query builder only, not mapping from API
	 * to table
	 */
	@SuppressWarnings("serial")
	private static final Map<String, String> API_TO_TABLE_FIELDS = new HashMap<String, String>() {
		{
			put("TRADE_DATE_RANGE", "TRD_DATE");
			put("TRADE_DATE", "TRD_DATE");
			put("OCEAN_PROCESS_DATE", "date(OCEAN_CREATE_TS)");
		}
	};

	@SuppressWarnings("serial")
	public static String[] getColumnSchemaMap(String key, Map<String, String> columnTypeSchema){
		Map<String, String[]> map = new HashMap<String, String[]>() {
			{
				put("date(OCEAN_CREATE_TS)", new String[] {
						"OCEAN_PROCESS_DATE",
						"java.time.LocalDate"
				});
			}
		};
		return map.containsKey(key) ? map.get(key) : new String[] {
				key,
				columnTypeSchema.get(key)
		};
	}
	
	@SuppressWarnings("serial")
	public static String getColumnSchemaMap(String aggOps, String aggValue, Map<String, String> columnTypeSchema) {
		Map<String, Function<String, String>> map = new HashMap<String, Function<String, String>>() {
			{
				put("SUM", columnTypeSchema::get);
				put("MIN", columnTypeSchema::get);
				put("MAX", columnTypeSchema::get);
				put("AVERAGE", columnTypeSchema::get);
				put("COUNT", (String str) -> {
					return "java.lang.Long";
				});
			}
		};

		return map.get(aggOps).apply(aggValue);
	}
	
	public NetezzaQuery(Shard shard) {
		this.shard = shard;
	}

	public Shard getShard() {
		return shard;
	}

	@Override
	public String getQuery() {
		return query;
	}

	private void setQuery(String query) {
		this.query = query;
	}

	public String getCountQuery() {
		return countQuery;
	}

	private void setCountQuery(String countQuery) {
		this.countQuery = countQuery;
	}

	/* Helper methods */
	public Query buildCountQuery(QueryTradesParam param) {
		Map<String, FilterParam> filters = param.getFilters();
		String tableName = shard.getTableName();

		Select query = new Select();
		query.select("count(1)").from(tableName);
		if (!filters.isEmpty()) {
			query.where(buildCondition(filters));
		}
		this.setCountQuery(query.toString());
		return this;
	}

	public Query build(QueryTradesParam param) {

		int limit = param.getLimit();
		Map<String, FilterParam> filters = param.getFilters();
		String tableName = shard.getTableName();

		Select query = new Select();

		if (param.getColumns() == null || param.getColumns().length == 0) {
			TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema(tableName);
			query.select(schema.getColumnList());
		} else {
			List<String> columns = Arrays.asList(param.getColumns());
			query.select(columns.stream().map(i -> getColumnName(i)).collect(Collectors.toList()));
		}

		query.from(tableName);
		
		if (!filters.isEmpty()) {
			query.where(buildCondition(filters));
		}
		param.getOrderByAsc().ifPresent(order -> query.orderBy(Arrays.asList(order), false));
		param.getOrderByDesc().ifPresent(order -> query.orderBy(Arrays.asList(order), true));
		
		if (limit > 0) {
			query.limit(Integer.toString(limit));
		}
		setQuery(query.toString());
		return this;

	}

	public Query build(AggQueryTradesParam param) {

		String[] aggFields = param.getAggFields();
		String[] aggValues = param.getAggValues();
		String[] aggOperations = param.getAggOperations();
		Map<String, FilterParam> filters = param.getFilters();

		String tableName = shard.getTableName();

		Select query = new Select();
		
		String aggFieldsString = Arrays.asList(aggFields).stream().map(i -> getColumnName(i))
				.collect(Collectors.joining(","));

		if (aggValues == null || aggValues.length == 0) {
			TableSchema schema = ConfigUtil.getTableSchemaRegistry().getTableSchema(tableName);
			query.select(Arrays.asList(schema.getColumnListInOrder()));
		} else {
			if (aggFields.length != 0) {
				query.select(aggFieldsString);
			}
			query.select(IntStream.range(0, aggOperations.length)
					.mapToObj(i -> buildAgg(aggOperations[i], aggValues[i])).collect(Collectors.toList()));
		}

		query.from(tableName);
		
		if (filters != null && !filters.isEmpty()) {
			query.where(buildCondition(filters));
		}
		
		if (aggFields != null && aggFields.length != 0) {
			query.groupBy(aggFieldsString);
		}

		param.getOrderByAsc().ifPresent(order -> query.orderBy(Arrays.asList(order), false));
		
		param.getOrderByDesc().ifPresent(order -> query.orderBy(Arrays.asList(order), true));

		setQuery(query.toString());

		return this;
	}

	public Query build(FilterValuesParam param) {

		String filterField = param.getFilterField();
		Map<String, FilterParam> filters = param.getFilters();
		String tableName = shard.getTableName();

		StringBuilder query = new StringBuilder(200).append("select distinct ");

		query.append(getColumnName(filterField));

		query.append(" from ").append(tableName);
		if (!filters.isEmpty()) {
			query.append(" where ").append(buildCondition(filters));
		}
		query.append(" order by ").append(getColumnName(filterField));
		setQuery(query.toString());
		return this;

	}

	public static String buildAgg(String op, String field) {
		if (op.equalsIgnoreCase("average")) {
			op = "AVG";
		}
		return new StringBuilder(20).append(op).append("(").append(field).append(")").toString();
	}

	private String buildCondition(Map<String, FilterParam> filters) {
		return filters.values().stream().map(i -> getFilterString(i)).collect(Collectors.joining(" and "));
	}

	private String getFilterString(FilterParam filter) {
		StringBuilder condition = new StringBuilder(20);
		String key = getColumnName(filter.getKey());

		String op, val;
		if (filter instanceof DateFilterParam) {
			op = " between ";
			val = "'" + formatDate(((DateFilterParam) filter).getStartDate().get()) + "' and '"
					+ formatDate(((DateFilterParam) filter).getEndDate().get()) + "'";
		} else {
			op = " in ";
			val = Arrays.asList(filter.getVals()).stream().collect(Collectors.joining("','", "('", "')"));
		}
		return condition.append(key).append(op).append(val).toString();
	}

	public static String getColumnName(String apiName) {
		return API_TO_TABLE_FIELDS.containsKey(apiName) ? API_TO_TABLE_FIELDS.get(apiName) : apiName;
	}

	private String formatDate(LocalDate date) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
		return date.format(dtf);
	}

}

enum SQLKeyWord {
	SELECT("SELECT"), 
	FROM("FROM"),
	WHERE("WHERE"), 
	GROUP_BY("GROUP BY"), 
	ORDER_BY("ORDER BY"),
	LIMIT("LIMIT");
	private String value;
	
	private SQLKeyWord(String value) {
		this.value = value;
	}
	
	public String toString() {
		return this.value;
	}
}


class Select {
	
	Map<SQLKeyWord, List<String>> optionalParams = new LinkedHashMap<SQLKeyWord, List<String>>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put(SQLKeyWord.SELECT, new ArrayList<String>());
			put(SQLKeyWord.FROM, new ArrayList<String>());
			put(SQLKeyWord.WHERE, new ArrayList<String>());
			put(SQLKeyWord.GROUP_BY, new ArrayList<String>());
			put(SQLKeyWord.ORDER_BY, new ArrayList<String>());
			put(SQLKeyWord.LIMIT, new ArrayList<String>());
		}
	};
	

	public Select select(String attr) {
		optionalParams.get(SQLKeyWord.SELECT).add(attr);
		return this;
	}
	
	public Select select(List<String> attr) {
		optionalParams.get(SQLKeyWord.SELECT).addAll(attr);
		return this;
	}
	
	public Select from(String table) {
		optionalParams.get(SQLKeyWord.FROM).add(table);
		return this;
	}
	
	public Select from(List<String> table) {
		optionalParams.put(SQLKeyWord.FROM, table);
		return this;
	}
	
	public Select where(String cond) {
		optionalParams.get(SQLKeyWord.WHERE).add(cond);
		return this;		
	}

	public Select where(List<String> cond) {
		optionalParams.put(SQLKeyWord.WHERE, cond);
		return this;		
	}

	public Select groupBy(String group) {
		optionalParams.get(SQLKeyWord.GROUP_BY).add(group);
		return this;		
	}
	
	public Select groupBy(List<String> group) {
		optionalParams.put(SQLKeyWord.GROUP_BY, group);
		return this;		
	}
	
	public Select orderBy(List<String> order, boolean isDesc) {
		if (isDesc) {
			order = order.stream().map(o -> NetezzaQuery.getColumnName(o) + " desc").collect(Collectors.toList());
		}
		optionalParams.get(SQLKeyWord.ORDER_BY).addAll(order);
		return this;		
	}
	
	public Select limit(String limit) {
		if (optionalParams.get(SQLKeyWord.LIMIT).isEmpty()) {
			optionalParams.get(SQLKeyWord.LIMIT).add(limit);
			System.out.println("limit " + limit);
		} else {
			throw new IllegalArgumentException("Can't assign more than 1 LIMIT");
		}
		
		return this;		
	}
	
	public String build() {
		StringBuilder sb = new StringBuilder(50);
		
		for (Entry<SQLKeyWord, List<String>> entry : optionalParams.entrySet()) {
			if (!entry.getValue().isEmpty()) {
				sb
				.append(" ")
				.append(entry.getKey().toString())
				.append(" ")
				.append(entry.getValue().stream().collect(Collectors.joining(",")));
			}
		}
		return sb.toString().trim();		
	}
	
	public String toString() {
		return build();
	}
}
