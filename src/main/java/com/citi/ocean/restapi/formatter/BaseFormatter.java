package com.citi.ocean.restapi.formatter;



import java.util.List;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;

public interface BaseFormatter {

	public String getDelimiterBetweenHdrAndContent();
	
	public byte[] format(List<Message<Buffer>> message);

	public String formatDeserializedHeaderBuffer(List<String[]> schema, long count);
	public String formatDeserializedHeaderBuffer(List<String[]> block, List<String[]> schema, long count);
	
	public String formatDeserializedContentBuffer(List<String[]> block);
	public String formatDeserializedContentBuffer(String prefix, List<String[]> block);
	
	public String formatDeserializedEndBuffer();
	public String formatDeserializedEndBuffer(long startTime);
	public String formatDeserializedEndBuffer(long startTime, long count);
}
