package com.citi.ocean.restapi.util;

import org.apache.log4j.Logger;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;

public class MonitorUtil {

	/*
	 * markets.ocean.NAM.UAT.service.http.tradedatamart.failure.error
	 * markets.ocean.NAM.UAT.service.http.tradedatamart.usage.request
	 * markets.ocean.NAM.UAT.service.http.tradedatamart.performance.query
	 * markets.ocean.NAM.UAT.service.http.tradedatamart.performance.endpoint
	 */

	private static final Logger log = Logger.getLogger(MonitorUtil.class);

	public final static String MONITOR_TOPIC = "monitor";

	public static void sendEvent(Vertx vertx, MonitorType event, String value) {
		switch (event) {
		case MONITOR_FAILURE_ERROR:
			vertx.eventBus().send(MONITOR_TOPIC, event.getNamespace() + ":" + value);
			break;
		case MONITOR_USAGE_REQUEST:
			vertx.eventBus().send(MONITOR_TOPIC, event.getNamespace() + " " + value);
			break;
		case MONITOR_PERFORMANCE_QUERY:
			vertx.eventBus().send(MONITOR_TOPIC, event.getNamespace() + " " + value);
			break;
		case MONITOR_PERFORMANCE_ENDPOINT:
			vertx.eventBus().send(MONITOR_TOPIC, event.getNamespace() + " " + value);
			break;
		default:
			break;
		}
		
	}

	public static void logEvent(Vertx vertx) {
		MessageConsumer<String> consumer = vertx.eventBus().consumer(MONITOR_TOPIC, message -> {
			log.info(message.body());
		});
	}

	public enum MonitorType {
		MONITOR_FAILURE_ERROR("markets.ocean.NAM.UAT.service.http.tradedatamart.failure.error"), MONITOR_USAGE_REQUEST(
				"markets.ocean.NAM.UAT.service.http.tradedatamart.usage.request"),MONITOR_USAGE_QUERY(
						"markets.ocean.NAM.UAT.service.http.tradedatamart.usage.query"), MONITOR_PERFORMANCE_QUERY(
						"markets.ocean.NAM.UAT.service.http.tradedatamart.performance.query"), MONITOR_PERFORMANCE_ENDPOINT(
								"markets.ocean.NAM.UAT.service.http.tradedatamart.performance.endpoint");

		private final String namespace;

		private MonitorType(String namespace) {
			this.namespace = namespace;
		}

		public String getNamespace() {
			return namespace;
		}

	}

}
