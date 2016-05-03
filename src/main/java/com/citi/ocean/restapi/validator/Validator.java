package com.citi.ocean.restapi.validator;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.FilterParam.DateFilterParam;
import com.citi.ocean.restapi.util.ExceptionUtil;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.time.format.DateTimeParseException;
public class Validator {
	public final static String HTTP_PARAM_NAME_USERID = "userId".toUpperCase();
	public final static String HTTP_PARAM_MAX_ROWS = "maxRows".toUpperCase();
	public final static DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
	public final static int DEFAULT_MAX_ROWS = 10; 
	public final static String HTTP_PARAM_START_DATE = "TRADE_DATE_FROM";
	public final static String HTTP_PARAM_TO_DATE = "TRADE_DATE_TO";
	public final static String HTTP_PARAM_TRADE_DATE_RANGE = "TRADE_DATE_RANGE";
	public final static String HTTP_PARAM_AGGREGATE_FIELDS = "aggFields".toUpperCase();
	public final static String HTTP_PARAM_AGGREGATE_VALUES = "aggValues".toUpperCase();
	public final static String HTTP_PARAM_AGGREGATE_OPERATIONS = "aggOperations".toUpperCase();
	public final static String HTTP_PARAM_COLUMNS = "columns".toUpperCase();
	public final static String HTTP_PARAM_FILTER_FIELD = "filterField".toUpperCase();
	public final static String HTTP_PARAM_CATEGORY = "category".toUpperCase();
	public final static String HTTP_PARAM_VALUE = "value".toUpperCase();
	public final static String HTTP_PARAM_ORDER_BY_ASC = "orderByAsc".toUpperCase();
	public final static String HTTP_PARAM_ORDER_BY_DESC = "orderByDesc".toUpperCase();
	
	
	@SuppressWarnings("serial")
	public final static Set<String> NUMERIC_COLUMN_SCHEMA =  new HashSet<String>() {
		{
			   add("NOTIONAL1_AMT");
			   add("NOTIONAL2_AMT"); 
			   add("OCEAN_ID");
			   add("NOTIONAL1_AMT");
			   add("NOTIONAL2_AMT");
			   add("TRADE_QUANTITY");
			   add("TRADE_PRICE"); 
			   add("STRIKE_PRICE");
			   add("SECURITY_MULTIPLIER");
			   add("SIGNED_ADJUSTED_NOMINAL_USD");
			   add("SIGNED_ADJUSTED_TRADE_QUANTITY");
		}
	};
	
	@SuppressWarnings("serial")
	public final static Map<String, String> CATEGORY_SCHEMA = new HashMap<String, String>() {
		{
			put("OTC", "OTC_DERIV_TRADE_MASTER");
			put("CASH", "CASH_ETD_TRADE_MASTER");
		}
	};
	
	public final static Set<String> AGGREGATE_FIELDS_MAP= new HashSet<String>(Arrays.asList(
			"TRADE_DATE",
			"OCEAN_PROCESS_DATE",
			"SRC_SYS",
			"NOTIONAL1_CRCY",
			"NOTIONAL2_CRCY",
			"TRADE_TYPE",
			"PRIMARY_ASSET_CLASS",
			"PRODUCT_ID",
			"BASE_PRODUCT_TYPE",
			"SMCP",
			"CUSIP",
			"TICKER",
			"FII",
			"MGD_SEG_LEVEL8_DESCR",
			"FIRM_GFCID",
			"COUNTER_PARTY_GFCID",
			"FIRM_ACCT_MNEMONIC",
			"COUNTER_PARTY_ACCT_MNEMONIC"			
			));

	public final static Set<String> AGGREGATE_OPERATIONS_MAP= new HashSet<String>(Arrays.asList(
				"COUNT", "SUM", "AVERAGE"		
			));
	
	
	@SuppressWarnings("serial")
	public final static Set<String> FILTERS_IGNORE_HTTP_PARAM = new HashSet<String>() {
		{
			add(HTTP_PARAM_MAX_ROWS);
			add(HTTP_PARAM_NAME_USERID);
			add(HTTP_PARAM_AGGREGATE_FIELDS);
			add(HTTP_PARAM_AGGREGATE_OPERATIONS);
			add(HTTP_PARAM_AGGREGATE_VALUES);
			add(HTTP_PARAM_COLUMNS);
			add(HTTP_PARAM_FILTER_FIELD);
			add(HTTP_PARAM_CATEGORY);
			add(HTTP_PARAM_ORDER_BY_ASC);
			add(HTTP_PARAM_ORDER_BY_DESC);
		}
	};
	
	@SuppressWarnings("serial")
	public final static Set<String> REQUIRED_HTTP_QUERY_PARAM = new HashSet<String>() {
		{
			add(HTTP_PARAM_NAME_USERID);
			add(HTTP_PARAM_CATEGORY);
		}
	};
	
	@SuppressWarnings("serial")
	public final static Set<String> REQUIRED_HTTP_AGG_PARAM = new HashSet<String>() {
		{
			add(HTTP_PARAM_NAME_USERID);
			add(HTTP_PARAM_CATEGORY);
			add(HTTP_PARAM_AGGREGATE_FIELDS);
			add(HTTP_PARAM_AGGREGATE_OPERATIONS);
			add(HTTP_PARAM_AGGREGATE_VALUES);
		}
	};
	
	
	@SuppressWarnings({ "serial"})
	public static final Map<String, Function<Map.Entry, FilterParam>> FILTER_MAP = new HashMap<String, Function<Map.Entry, FilterParam>>() {
		{
			put("OCEAN_PROCESS_DATE", Validator::createSingletonFilterParam);
			put("SRC_SYS", Validator::createSingletonFilterParam);
			put("NOTIONAL1_CRCY", Validator::createSingletonFilterParam);
			put("NOTIONAL2_CRCY", Validator::createSingletonFilterParam);
			put("TRADE_TYPE", Validator::createSingletonFilterParam);
			put("PRIMARY_ASSET_CLASS", Validator::createSingletonFilterParam);
			put("PRODUCT_ID", Validator::createSingletonFilterParam);
			put("BASE_PRODUCT_TYPE", Validator::createSingletonFilterParam);
			put("SMCP", Validator::createSingletonFilterParam);
			put("CUSIP", Validator::createSingletonFilterParam);
			put("TICKER", Validator::createSingletonFilterParam);
			put("FII", Validator::createSingletonFilterParam);
			put("MGD_SEG_LEVEL8_DESCR", Validator::createSingletonFilterParam);
			put("FIRM_GFCID", Validator::createSingletonFilterParam);
			put("COUNTER_PARTY_GFCID", Validator::createSingletonFilterParam);
			put("FIRM_ACCT_MNEMONIC", Validator::createSingletonFilterParam);
			put("COUNTER_PARTY_ACCT_MNEMONIC", Validator::createSingletonFilterParam);
			put("maxRows", Validator::createSingletonFilterParam);
			put("UITID", Validator::createSingletonFilterParam);
			put("TRADE_DATE_TO", Validator::createRangeFilterParam);
			put("TRADE_DATE_FROM", Validator::createRangeFilterParam);
			put("TRADE_DATE_RANGE", Validator::createRangeFilterParam);
		}
	};
	
	/**
	 * 
	 * @author hw72786
	 * 
	 * Data Type Validator
	 */
	public enum NETEZZA_DATATYPE {
		VARCHAR, NUMBER, DATE
	}
	
	@SuppressWarnings("serial")
	public final static Map<String, NETEZZA_DATATYPE> SCHEMA = new HashMap<String, NETEZZA_DATATYPE>() {
		{
			put("OCEAN_PROCESS_DATE", NETEZZA_DATATYPE.DATE);
			put("SRC_SYS", NETEZZA_DATATYPE.VARCHAR);
			put("NOTIONAL1_CRCY", NETEZZA_DATATYPE.VARCHAR);
			put("NOTIONAL2_CRCY", NETEZZA_DATATYPE.VARCHAR);
			put("TRADE_TYPE", NETEZZA_DATATYPE.VARCHAR);	
			put("PRIMARY_ASSET_CLASS", NETEZZA_DATATYPE.VARCHAR);
			put("PRODUCT_ID", NETEZZA_DATATYPE.VARCHAR);
			put("BASE_PRODUCT_TYPE", NETEZZA_DATATYPE.VARCHAR);
			put("SMCP", NETEZZA_DATATYPE.VARCHAR);
			put("CUSIP", NETEZZA_DATATYPE.VARCHAR);
			put("TICKER", NETEZZA_DATATYPE.VARCHAR);
			put("FII", NETEZZA_DATATYPE.VARCHAR);
			put("MGD_SEG_LEVEL8_DESCR", NETEZZA_DATATYPE.VARCHAR);
			put("FIRM_GFCID", NETEZZA_DATATYPE.VARCHAR);
			put("COUNTER_PARTY_GFCID", NETEZZA_DATATYPE.VARCHAR);
			put("FIRM_ACCT_MNEMONIC", NETEZZA_DATATYPE.VARCHAR);
			put("COUNTER_PARTY_ACCT_MNEMONIC", NETEZZA_DATATYPE.VARCHAR);
			put("maxRows", NETEZZA_DATATYPE.NUMBER);
			put("UITID", NETEZZA_DATATYPE.VARCHAR);
			put("TRADE_DATE_TO", NETEZZA_DATATYPE.DATE);
			put("TRADE_DATE_FROM", NETEZZA_DATATYPE.DATE);
			put("TRADE_DATE_RANGE", NETEZZA_DATATYPE.NUMBER); //can TRADE_DATE_RANGE be negative?

		}
	};

	@SuppressWarnings("serial")
	public final static Map<NETEZZA_DATATYPE, Predicate<String>> VALIDATOR_MAP = 
			new HashMap<NETEZZA_DATATYPE, Predicate<String>>() {
		{
			put(NETEZZA_DATATYPE.NUMBER, x-> {
			    if(x.isEmpty()) return false;
			    return x.matches("-?\\d+");
//			    for(int i = 0; i < x.length(); i++) {
//			        if(i == 0 && x.charAt(i) == '-') {
//			            if(x.length() == 1) return false;
//			            else continue;
//			        }
//			        if(Character.digit(x.charAt(i),10) < 0) return false;
//			    }
//			    return true;
			});
			put(NETEZZA_DATATYPE.DATE, x-> {
			    try {
			    	DATE_FORMAT.parse(x);
			        return true;
			    } catch (DateTimeParseException ex) {
					return false;
				}
			});
			put(NETEZZA_DATATYPE.VARCHAR, x -> true);
		}
	};
	public static FilterParam createRangeFilterParam(Map.Entry<String, String> entry) {
		String[] vals = Optional.ofNullable(entry.getValue().split(","))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE));
		if(entry.getKey().contains("FROM")) return new DateFilterParam(entry.getKey(), new String[]{vals[0], ""});
		if(entry.getKey().contains("TO")) return new DateFilterParam(entry.getKey(), new String[]{"", vals[0]});
		if(entry.getKey().contains("RANGE")) return new DateFilterParam(entry.getKey(), 
				new String[]{LocalDate.now().plusDays(-Integer.parseInt(vals[0]))
						.format(DATE_FORMAT).toString(), LocalDate.now().format(DATE_FORMAT).toString()});
		else throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_DATE_RANGE_INCOMPLETE);
	}

	public static FilterParam createSingletonFilterParam(Map.Entry<String, String> entry) {
		String[] vals = entry.getValue().split(",");
		return new FilterParam(entry.getKey(), vals);
	}
	/**
	 * Merge date range filters and validate mandatory date range
	 * @param filters
	 */
	public static void dateFilterMerger(Map<String, FilterParam> filters) {
		if(filters.containsKey(HTTP_PARAM_TRADE_DATE_RANGE) 
				&& !filters.containsKey(HTTP_PARAM_START_DATE) && !filters.containsKey(HTTP_PARAM_TO_DATE)) return;
		
		if(filters.containsKey(HTTP_PARAM_TRADE_DATE_RANGE) 
			&& (filters.containsKey(HTTP_PARAM_START_DATE) || filters.containsKey(HTTP_PARAM_TO_DATE))) {
				throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_MULTIPLE_DATE_RANGE);
		}
		String sDate = (Optional.ofNullable(Optional.ofNullable(((DateFilterParam)filters.get(HTTP_PARAM_START_DATE)))
					         	.orElse((DateFilterParam)filters.get(HTTP_PARAM_TRADE_DATE_RANGE)))
									.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_DATE_RANGE_INCOMPLETE)))
										.getStartDate().get().format(DATE_FORMAT).toString();
		String eDate = (Optional.ofNullable(Optional.ofNullable(((DateFilterParam)filters.get(HTTP_PARAM_TO_DATE)))
	         					.orElse((DateFilterParam)filters.get(HTTP_PARAM_TRADE_DATE_RANGE)))
									.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_DATE_RANGE_INCOMPLETE)))
										.getEndDate().get().format(DATE_FORMAT).toString();
		if(LocalDate.parse(sDate, DATE_FORMAT).compareTo(LocalDate.parse(eDate, DATE_FORMAT)) > 0)
			throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATE_RANGE); 
		FilterParam dateRangeParam = new DateFilterParam(HTTP_PARAM_TRADE_DATE_RANGE, new String[]{sDate, eDate});
		filters.remove(HTTP_PARAM_START_DATE);
		filters.remove(HTTP_PARAM_TO_DATE);
		filters.put(HTTP_PARAM_TRADE_DATE_RANGE, dateRangeParam);
		
	}
	
	public static boolean validateOrderBy(List<String> aggFields, List<String> aggValueOp, String orderedField) {
		if ((aggFields.contains(orderedField)) || aggValueOp.contains(orderedField)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static void sendErrorMsgOnValidationException(RoutingContext routingContext, IllegalArgumentException e) {
		HttpServerResponse response = routingContext.response();
		response.putHeader("content-type", "application/json; charset=utf-8").setChunked(true);
		JsonObject error = new JsonObject();
		error.put("code", 500);
		error.put("message", e.getMessage());
		response.end(error.encode());
	}
}
