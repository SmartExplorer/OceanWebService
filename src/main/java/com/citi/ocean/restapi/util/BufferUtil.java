package com.citi.ocean.restapi.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.providers.QueryExecutor;
import com.citi.ocean.restapi.handler.QueryTradesHandler;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;


public class BufferUtil {
	int pos = 0;
	Buffer buffer;
	List<String[]> columnSchema;
	private static final Logger log = Logger.getLogger(BufferUtil.class);
	
	public BufferUtil(Buffer buffer, List<String[]> columnSchema) {
		this.buffer = buffer;
		this.columnSchema = columnSchema;
	}
	public String get(String fieldName, String fieldType) {
		String reply = "";

		if (fieldType == null) {
			log.warn("FieldType is null. fieldName is " + fieldName);
		}
		switch (fieldType) {
			case "java.lang.Long":
				reply = "" + buffer.getLong(pos);
				pos += Long.BYTES;
				break;
			case "java.time.Instant":
				reply = new Timestamp(buffer.getLong(pos)).toString();
				pos += Long.BYTES;
				break;
			case "java.time.LocalDate":
				reply = new Date(buffer.getLong(pos)).toString();
				pos += Long.BYTES;
				break;
			case "java.lang.String":
				long size = buffer.getLong(pos);
				pos += Long.BYTES;
				reply = buffer.getString(pos, pos + Math.toIntExact(size));
				pos += Math.toIntExact(size);
				break;
			case "java.lang.Double":
				reply = "" + buffer.getDouble(pos);
				pos += Double.BYTES;
				break;
			case "char":
				return "" + (char)buffer.getByte(pos++);
			default:
				//TODO how to deseriallize the agg func - assuming all agg return Long
				reply = "" + buffer.getLong(pos);
				pos += Long.BYTES;
				break;
		}
		return reply;
	}

	public List<String[]> deserialize() {

		List<String[]> result = new LinkedList<>();

		while (pos < buffer.length()) {
			result.add(columnSchema.stream()
					.map(x -> get(x[0], x[1]))
					.collect(Collectors.toList()).toArray(new String[0]));
		}

		return result;    
	}
	public static boolean checkIfMessage(Message<Buffer> msg) {
		return !checkIfEOF(msg) && !checkIfError(msg) && !checkIfExecutionTimeMessage(msg);
	}
	
	public static boolean checkIfContainsMessage(List<Message<Buffer>> msgs) {
		return msgs.stream()
				.anyMatch(x -> !checkIfEOF(x) && !checkIfError(x) && !checkIfExecutionTimeMessage(x));
	}
	public static boolean checkIfEOF(Message<Buffer> msg) {
		return msg.headers().contains(QueryExecutor.EB_HEADER_EOR);
	}
	
	public static boolean checkIfError(Message<Buffer> msg) {
		return msg.headers().contains(QueryExecutor.EB_HEADER_ERROR);
	}

	public static boolean checkIfNotError(Message<Buffer> msg) {
		return !checkIfError(msg);
	}
	
	
	public static boolean checkIfCountMessage(Message<Buffer> msg) {
		return msg.headers().get(QueryExecutor.COUNT_HEADER) != null;
	}
	
	public static boolean checkIfContainsCountMessage(List<Message<Buffer>> msgs) {
		return msgs.stream()
			.anyMatch(x -> x.headers().get(QueryExecutor.COUNT_HEADER) != null);
	}
	public static boolean checkIfNotCountMessage(Message<Buffer> msg) {
		return !checkIfCountMessage(msg);
	}

	public static boolean checkIfExecutionTimeMessage(Message<Buffer> msg) {
		return msg.headers().contains(QueryExecutor.EXECUTION_TIME_HEADER);
	}
	
	public static Map<String, String> getColumnTypeSchema(String tableName) {
		Map<String, String> columnTypeSchema = ConfigUtil.getTableSchemaRegistry()
				.getTableSchema(tableName).getColumnTypes();
		return columnTypeSchema;
	}

}
