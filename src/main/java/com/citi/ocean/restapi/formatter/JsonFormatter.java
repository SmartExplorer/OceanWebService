package com.citi.ocean.restapi.formatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JsonFormatter implements BaseFormatter {

	private final String DELIMITER_HDR_CONTENT = ","; 
	@Override
	public byte[] format(List<Message<Buffer>> message) {
		// TODO Auto-generated method stub
		return null;
	}

	private List<Object> parse(List<String> element) {
		List<Object> result = new ArrayList<Object>();
		Object tmp;

		//TODO how to get the scale of numeric(38,4)
		//double is displayed in scientific notation E now..
//		DecimalFormat df = new DecimalFormat("#");
//		df.setMaximumFractionDigits(4);
		
		for (int i=0; i<element.size(); i++) {
			try {
				tmp = Long.parseLong(element.get(i));
			} catch (NumberFormatException e) {
				try {
					tmp = Double.parseDouble(element.get(i));
				} catch (NumberFormatException e1) {
					tmp = element.get(i);
				}
			}
			result.add(tmp);
		}
		return result;
	}

	public String formatDeserializedHeaderBuffer(List<String[]> schema, long count) {
//		JsonObject message = new JsonObject();
//		if (count >= 0) {
//			message.put("TotalCount", count);
//		}
//
//		JsonObject body = new JsonObject();
//		body.put("columns", new JsonArray(schema.stream()
//												.map(x -> x[0])
//												.collect(Collectors.toList())));
//		body.put("rows", new JsonArray());
//		message.put("Datarows", body);
//		String encodedMsg = message.encode();
//		return encodedMsg.substring(0, encodedMsg.length() - 3);
		return formatDeserializedHeaderBuffer(new ArrayList<String[]>(), schema, count);
	}
	
	public String formatDeserializedHeaderBuffer(List<String[]> block, List<String[]> schema, long count) {
		JsonObject message = new JsonObject();
		if (count >= 0) {
			message.put("TotalCount", count);
		}

		JsonObject body = new JsonObject();
		body.put("columns", new JsonArray(schema.stream()
												.map(x -> x[0])
												.collect(Collectors.toList())));
		body.put("rows", 
				getContentBufferJsonArray(block).stream().collect(Collectors.toList()));
		message.put("Datarows", body);
		String encodedMsg = message.encode();
		return encodedMsg.substring(0, encodedMsg.length() - "}}]".length());
	}
	
	private List<JsonArray> getContentBufferJsonArray(List<String[]> block) {
		return block.stream()
				.map(Arrays::asList)
				.map(this::parse)
				.map(io.vertx.core.json.JsonArray::new)
				.collect(Collectors.toList());
	}
	
	public String formatDeserializedContentBuffer(List<String[]> block) {
//		String result = block.stream()
//			.map(Arrays::asList)
//			.map(io.vertx.core.json.JsonArray::new)
//			.map(x -> x.encode())
//			.reduce(new BinaryOperator<String>() {
//				public String apply(String arg0, String arg1) {
//					return arg0 + "," + arg1;
//				}
//			}).get();
		return getContentBufferJsonArray(block)
				.stream()
				.map(x -> x.encode())
				.collect(Collectors.joining(","));
	}

	
	public String formatDeserializedContentBuffer(String prefix, List<String[]> block) {
		return prefix + formatDeserializedContentBuffer(block);
	}
	
	private StringBuilder addJsonElement(StringBuilder current, String elementName, long elementValue) {
		return current.append('"').append(elementName).append('"').append(":")
				.append(elementValue);
	}
	
	public String formatDeserializedEndBuffer() {
		return "]}}";
	}
	
	public String formatDeserializedEndBuffer(long startTime) {
		StringBuilder current =  new StringBuilder(30).append("]},");
		return addJsonElement(current, "ExecutionTime", System.currentTimeMillis() - startTime).append("}").toString();
	}

	public String formatDeserializedEndBuffer(long startTime, long count) {
		StringBuilder current =  new StringBuilder(30).append("]},");
		current = addJsonElement(current, "ExecutionTime", System.currentTimeMillis() - startTime)
				.append(",");
		return addJsonElement(current, "TotalCount", count).append("}").toString();
	}

	@Override
	public String getDelimiterBetweenHdrAndContent() {
		return DELIMITER_HDR_CONTENT;
	}
}
