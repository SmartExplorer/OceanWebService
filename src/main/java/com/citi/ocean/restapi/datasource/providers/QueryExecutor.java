package com.citi.ocean.restapi.datasource.providers;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;

public abstract class QueryExecutor implements AutoCloseable {
	
	public final static String EB_HEADER_EOR = "EOR";
	public final static String EB_HEADER_ERROR = "ERROR";
	public final static String COUNT_HEADER = "COUNT";
	public final static String EXECUTION_TIME_HEADER = "TIME";
	
	protected final EventBus outputBus;
	
	public QueryExecutor(EventBus outputBus) {
		this.outputBus = outputBus;
	}
	
	public abstract void executeQuery(String replyTopic, String tableName, String sourceQuery);
	public abstract void close();

	protected void publishEOR(String replyTopic) {
		outputBus.send(replyTopic, Buffer.buffer(), new DeliveryOptions().addHeader(EB_HEADER_EOR, ""));
	}

	protected void publishError(String replyTopic, String error) {
		outputBus.send(replyTopic, Buffer.buffer(), new DeliveryOptions().addHeader(EB_HEADER_ERROR, error));
	}
	
	protected void publishEOR(String replyTopic, long startTime) {
		outputBus.send(replyTopic, Buffer.buffer(), new DeliveryOptions().addHeader(EB_HEADER_EOR, "").addHeader(EXECUTION_TIME_HEADER, Long.toString(startTime)));
	}
}
