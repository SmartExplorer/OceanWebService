package com.citi.ocean.restapi.worker;

import java.util.Optional;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.providers.NetezzaQueryExecutor;
import com.citi.ocean.restapi.datasource.providers.QueryExecutor;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.util.MonitorUtil;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.rx.java.RxHelper;

public class NetezzaWorker extends AbstractVerticle {

	private static final Logger log = Logger.getLogger(SolrWorker.class);
	private QueryExecutor queryExecutor = null;

	@Override
	public void start() throws Exception {
		initQueryExecutor();
		String workerTopic = Optional.ofNullable(this.config().getString(ConfigUtil.NETEZZA_WORKER_TOPIC)).orElse(ConfigUtil.NETEZZA_WORKER_TOPIC);
		MessageConsumer<String> consumer = vertx.eventBus().consumer(workerTopic);
		RxHelper.toObservable(consumer).subscribe(x -> doQuery(consumer, x));
		log.info("Started Netezza Consumer on : " + workerTopic);
	}

	private void initQueryExecutor() {
		if (queryExecutor == null) {

			String url = Optional.ofNullable(this.config().getString(ConfigUtil.URL)).orElse(ConfigUtil.URL);
			String user = Optional.ofNullable(this.config().getString(ConfigUtil.USER)).orElse(ConfigUtil.USER);
			String password = Optional.ofNullable(this.config().getString(ConfigUtil.PASSWORD)).orElse(ConfigUtil.PASSWORD);
			log.debug(String.format("Creating Netezza connection with %s, %s, %s", url, user, password));
			queryExecutor = new NetezzaQueryExecutor(vertx.eventBus(), url, user, password);
		}
	}

	private void closeQueryExecutor() {
		if (queryExecutor != null) {
			queryExecutor.close();
			queryExecutor = null;
		}
	}

	public void doQuery(MessageConsumer<String> consumer, Message<String> x) {
		// consumer consume message sent from query handler
		MonitorUtil.sendEvent(vertx, MonitorUtil.MonitorType.MONITOR_USAGE_QUERY, "NetezzaWorker executes query: " + x.body());

		log.debug("Got Query in Worker: " + x.body());
		String replyTopic = x.headers().get(ConfigUtil.REPLY_TOPIC);

		String query = x.body();

		// send message to query executor
		long start = System.currentTimeMillis();
		
		if (x.headers().contains(QueryExecutor.COUNT_HEADER)) {
			// this is count query
			((NetezzaQueryExecutor) queryExecutor).executeCountQuery(replyTopic, x.body(), query);
		} else {
			queryExecutor.executeQuery(replyTopic, "OTC_DERIV_TRADE_MASTER", query);
		}
		long duration = System.currentTimeMillis() - start;
		MonitorUtil.sendEvent(vertx, MonitorUtil.MonitorType.MONITOR_PERFORMANCE_QUERY, Long.toString(duration));
	}

	@Override
	public void stop() {
		closeQueryExecutor();
		log.info("NetezzaWorker verticle has stopped.");
	}
}
