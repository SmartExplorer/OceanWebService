package com.citi.ocean.restapi.util;

import java.util.function.Function;
import java.util.function.Predicate;

public class ExceptionUtil {
	/**
	 * Normal Query Exception Message 
	 */
	public static final String EXCEPTION_MSG_MISSING_USERID = "Missing User ID";
	public static final String EXCEPTION_MSG_MISSING_CATEGORY = "Missing category";
	public static final String EXCEPTION_MSG_ILLEGAL_DATATYPE = " has illegal data type";
	public static final String EXCEPTION_MSG_ILLEGAL_CATEGORY = " Illegal category";
	public static final String EXCEPTION_MSG_DATE_RANGE_INCOMPLETE = "Date Range is incomplete";
	public static final String EXCEPTION_MSG_ILLEGAL_DATE_RANGE = "Illegal date range";
	public static final String EXCEPTION_MSG_ILLEGAL_QUERY_PARAM = "Query parameters is illegal";
	public static final String EXCEPTION_MSG_MULTIPLE_DATE_RANGE = "Can't indicate both date range and from/to date";
	/**
	 * Aggregate Query Exception Message
	 */
	public static final String EXCEPTION_MSG_ILLEGAL_AGGOPERATION = "Aggregate operation is illegal";
	public static final String EXCEPTION_MSG_ILLEGAL_AGGREGATION_VALUE = "Aggregate value is illegal";
	public static final String EXCEPTION_MSG_ILLEGAL_AGGFIELD = "Aggregate field is illegal";
	public static final String EXCEPTION_MSG_AGGREGATION_PARAMS_MISSING = "Aggregation params missing";
	public static final String EXCEPTION_MSG_AGGREGATION_PARAMS_MISMATCH = "Aggregation params mismatch";
	public static final String EXCEPTION_MSG_INVALID_ORDER_FIELD = "Field used in 'Order by' must be AggValue or AggOps(AggFields), i.e. sum(NOTIONAL_AMT)";
	/**
	 * illegal Column Exception message
	 * 
	 */
	public static final String EXCEPTION_MSG_ILLEGAL_COLUMNS = "Illegal columns";
	public static final String EXCEPTION_MSG_INCOMPATIBLE_CROSS_TABLE_FILTERS = "Incompatible cross table query filters";
	public static void throwIllegalARgExc(String message) {
		throw new IllegalArgumentException(message);
	}
	
	@FunctionalInterface
	public interface Function_WithIllegalArgExceptions<T, R> {
	    R apply(T t);
	}


	public static <T, R> Function<T, R> rethrowIllegalArgExc(Function_WithIllegalArgExceptions<T, R> function, String message) {
		return t -> {
			try {
				return function.apply(t);
			} catch (IllegalArgumentException ex) {
				throw new IllegalArgumentException(message, ex);
			}
		};
	}

	public static <T> Predicate<T> ifFalseThrowIllegalArgExc(Predicate<T> function, String message) {
		return t -> {
			if(!function.test(t)){
				throw new IllegalArgumentException(message);
			}else{
				return true;
			}
		};
	}
	
	public static boolean ifFalseThrowIllegalArgExc(boolean bool, String message) {
		if(!bool){
			throw new IllegalArgumentException(message);
		}else{
			return true;
		}
	}
}
