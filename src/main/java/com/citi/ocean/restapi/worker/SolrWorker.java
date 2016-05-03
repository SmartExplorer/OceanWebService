package com.citi.ocean.restapi.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;

import com.citi.ocean.restapi.datasource.providers.QueryExecutor;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.util.MonitorUtil;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.RxHelper;

public class SolrWorker extends AbstractVerticle {
	private static final String TAG = "_od_tg";

	private static final Logger log = Logger.getLogger(SolrWorker.class);

	private HttpSolrClient httpSolrClient;

	@Override
	public void start() throws Exception {
		log.debug("Initialize SolrWorker");
		httpSolrClient = new HttpSolrClient(Optional.ofNullable(this.config().getString(ConfigUtil.URL)).orElse(ConfigUtil.URL));
		String workerTopic = Optional.ofNullable(this.config().getString(ConfigUtil.SOLR_WORKER_TOPIC)).orElse(ConfigUtil.SOLR_WORKER_TOPIC);
		MessageConsumer<String> consumer = vertx.eventBus().consumer(workerTopic);
		RxHelper.toObservable(consumer).subscribe(x -> doQuery(x));

		log.info("Started Solr Consumer on : " + workerTopic);
	}

	public void doQuery(Message<String> x) {
		MonitorUtil.sendEvent(vertx, MonitorUtil.MonitorType.MONITOR_USAGE_QUERY,"SolrWorker executes query: " + x.body());
		log.debug("Got Query in Worker: " + x.body());
		String replyTopic = x.headers().get(ConfigUtil.REPLY_TOPIC);
		String limit = x.headers().get(ConfigUtil.LIMIT);
		String query = x.body();

		SolrQuery solrQuery = buildSolrQuery(limit, query);

		long start = System.currentTimeMillis();

		NoOpResponseParser jsonParser = new NoOpResponseParser();
		jsonParser.setWriterType("json");
		httpSolrClient.setParser(jsonParser);
		QueryRequest req = new QueryRequest(solrQuery);
		String result = null;
		try {
			NamedList<Object> response = httpSolrClient.request(req);
			result = response.get("response").toString();
			String parsedResult = parseData(result);
			vertx.eventBus().send(replyTopic, parsedResult);
		} catch (SolrServerException | IOException e) {
			log.error("Unable to get response from Solr Search. ", e);
		}

		vertx.eventBus().send(replyTopic, QueryExecutor.EB_HEADER_EOR);
		long duration = System.currentTimeMillis() - start;
		MonitorUtil.sendEvent(vertx, MonitorUtil.MonitorType.MONITOR_PERFORMANCE_QUERY, Long.toString(duration));
	}

	private SolrQuery buildSolrQuery(String limit, String query) {
		SolrQuery solrQuery = new SolrQuery();
		solrQuery.set("q", query)
		.set("wt", "json")
		.set("group", "true")
		.set("group.field", "category")
		.set("group.limit", limit)
		.set("group.ngroups", "true")
		.set("omitHeader", "true");
		return solrQuery;
	}

	@Override
	public void stop() {
		log.info( "SolrWorker verticle has stopped.");
		Optional.ofNullable(httpSolrClient).ifPresent(httpSolrClient -> {
			try {
				httpSolrClient.close();
			} catch (IOException e) {
				log.error("Unable to close Solr Connection. ", e);
			}
		});
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public String parseData(String result) {
		JsonObject raw = new JsonObject(result);

		List<JsonObject> jsons = new ArrayList<JsonObject>();
		raw.getJsonObject("grouped").getJsonObject("category").getJsonArray("groups").stream()
				.filter(m -> m instanceof Map).map(m -> ((Map) m).get("doclist")).map(m -> ((Map) m).get("docs"))
				.flatMap(l -> ((List) l).stream()).filter(m -> m instanceof Map)
				// Compatibility issue with mvn build
				.forEach(m -> jsons.add(buildOutput((Map)m)));

		return (new JsonArray(jsons)).encode();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static JsonObject buildOutput(Map m) {
		JsonObject json = new JsonObject(m);
		JsonObject newJson = new JsonObject();
		newJson.put("category", json.getString("category"))
		.put("subCategory", json.getString("subCategory"))
		.put("value", json.getString(json.getString("subCategory") + TAG));

		return newJson;
	}

}
