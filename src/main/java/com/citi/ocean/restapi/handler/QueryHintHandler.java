package com.citi.ocean.restapi.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.datasource.base.Shard;
import com.citi.ocean.restapi.datasource.base.SolrQuery;
import com.citi.ocean.restapi.datasource.providers.QueryExecutor;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.util.ExceptionUtil;
import com.citi.ocean.restapi.util.MonitorUtil;
import com.citi.ocean.restapi.validator.Validator;
import com.citi.ocean.restapi.worker.QueryRespTracker;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.rx.java.RxHelper;
import rx.observables.ConnectableObservable;

public class QueryHintHandler {
	private static final Logger log = Logger.getLogger(QueryHintHandler.class);
	public static List<Cluster> cluster = ConfigUtil.getCluster(QueryHintHandler.class);

	private static int uniqueId = 0;

	@SuppressWarnings("serial")
	public static Map<String, BiPredicate<Cluster, QueryTradesParam>> clusterFuncMap = new HashMap<String, BiPredicate<Cluster, QueryTradesParam>>() {
		{
			put("SOLR", QueryHintHandler::canSolrClusterServiceThisReq);

		}
	};

	@SuppressWarnings("serial")
	public static Map<String, BiFunction<Shard, QueryTradesParam, Query>> queryBuilderFuncMap = new HashMap<String, BiFunction<Shard, QueryTradesParam, Query>>() {
		{

			put("SOLR", QueryHintHandler::buildSolrQuery);

		}
	};

	/* The entry point for user request */
	public static void handleRequest(RoutingContext routingContext) {

		QueryTradesParam queryParam = null;
		try { 
		queryParam = parseHttpRequest(routingContext);
		}catch(IllegalArgumentException e) {
			Validator.sendErrorMsgOnValidationException(routingContext, e);
			return;
		}

		validate(queryParam);
		
		
		
		Cluster cluster = selectCluster(queryParam);

		List<Query> queries = buildQuery(cluster, queryParam);

		QueryRespTracker<String> tracker = queryShards(queries, queryParam);

		sendData(tracker, queryParam);

	}

	/********************************************
	 * Parsing and validating
	 ************************************/

	/*
	 * Parse http request, do basic data type validation while parsing and
	 * create query parameter object
	 */
	@SuppressWarnings("serial")
	public static QueryTradesParam parseHttpRequest(RoutingContext routingContext) {
		HttpServerRequest req = routingContext.request();
		String searchValue = Optional.ofNullable(req.getParam(Validator.HTTP_PARAM_VALUE))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM));
		String userId = Optional.ofNullable(req.getParam(Validator.HTTP_PARAM_NAME_USERID))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_MISSING_USERID));
		String limit = Optional.ofNullable(routingContext.request().getParam("limit")).orElse("5");

		Map<String, FilterParam> filterMap = new HashMap<String, FilterParam>() {
			{
				put("value", new FilterParam("value", new String[] { searchValue }));
			}
		};
		//throw new IllegalArgumentException("huh.");
		return new QueryTradesParam(routingContext, userId, null, (String[]) null, Integer.parseInt(limit), filterMap);
	}

	/* perform inter-field validations and throw appropriate exception */
	public static void validate(QueryTradesParam queryParam) {
	}

	/********************************************
	 * Selecting cluster
	 ****************************************/

	/* Identify the cluster that can service the request */
	public static Cluster selectCluster(QueryTradesParam queryParam) {
		return cluster.stream().filter(cluster -> clusterFuncMap.get(cluster.getType()).test(cluster, queryParam))
				.findFirst().get();
	}

	/* Checks if Solr cluster can service this request */
	public static boolean canSolrClusterServiceThisReq(Cluster cluster, QueryTradesParam queryParam) {
		return true;
	}

	/********************************************
	 * Building Query
	 ****************************************/

	/* Build the query for each shard */
	public static List<Query> buildQuery(Cluster cluster, QueryTradesParam queryParam) {
		return cluster.getShards().stream()
				.map(shard -> queryBuilderFuncMap.get(cluster.getType()).apply(shard, queryParam))
				.collect(Collectors.toList());
	}

	/* Builds Solr query */
	public static SolrQuery buildSolrQuery(Shard shard, QueryTradesParam queryParam) {

		log.debug("Building Solr Query: " + shard.getTopic());
		String query = queryParam.getFilters().get("value").getVals()[0];

		return new SolrQuery(shard, query, queryParam.getLimit());
	}

	/********************************************
	 * Querying Cluster
	 ****************************************/

	/* Query the cluster and get the observable */
	public static QueryRespTracker<String> queryShards(List<Query> queries, QueryTradesParam queryParam) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();

		// Assume all workers send data back to the same topic
		String replyTopic = generateReplyTopic();
		MessageConsumer<String> consumer = eb.consumer(replyTopic);
		queries.stream().forEach(query -> queryShard(query, eb, replyTopic));
		QueryRespTracker<String> tracker = new QueryRespTracker<String>(queries.size(), consumer);
		return tracker;
	}

	public static void queryShard(Query query, EventBus eb, String replyTopic) {
		log.debug("Query Shard: " + query);
		String topic = query.getShard().getTopic();
		/* send the query message to the bus on the shard topic */
		DeliveryOptions options = new DeliveryOptions();
		options.addHeader(ConfigUtil.REPLY_TOPIC, replyTopic);
		if (query instanceof SolrQuery) {
			options.addHeader(ConfigUtil.LIMIT, Integer.toString(((SolrQuery) query).getLimit()));
		}
		eb.send(topic, query.getQuery(), options);
		log.debug("Publish Solr Query to topic: " + topic);
	}

	public static String generateReplyTopic() {
		if (uniqueId > 100000) {
			uniqueId = 0;
		}
		uniqueId++;
		return "QueryHintHandler" + uniqueId;
	}

	/********************************************
	 * Sending data
	 ******************************************/

	public static void sendData(QueryRespTracker<String> tracker, QueryTradesParam queryParam) {
		HttpServerResponse response = queryParam.getRoutingContext().response();
		response.putHeader("content-type", "application/json; charset=utf-8").setChunked(true);

		ConnectableObservable<Message<String>> parentObsv = RxHelper.toObservable(tracker.getConsumer()).publish();
		tracker.addSubscription(parentObsv.filter(QueryHintHandler::checkIfEOF).subscribe(x -> stopOnEOF(tracker, response, queryParam)));
		tracker.addSubscription(parentObsv.filter(QueryHintHandler::checkIfError).subscribe(x -> stopOnError(x, tracker, response)));
		tracker.addSubscription(parentObsv.filter(QueryHintHandler::checkIfMessage).map(x -> x.body()).subscribe(x -> sendToHttp(x, response), 
				e-> log.error("Unexpected Observable error.", e),
				() -> log.debug("Completed reading messages from Observable")));
		parentObsv.connect();

	}

	/* Sends data to http socket */
	public static void sendToHttp(String message, HttpServerResponse response) {
		log.debug("Reply data: " + message);
		response.write(message);
	}

	public static boolean checkIfMessage(Message<String> msg) {
		return !checkIfEOF(msg) && !checkIfError(msg);
	}

	public static boolean checkIfEOF(Message<String> msg) {
		return msg.body().equals(QueryExecutor.EB_HEADER_EOR);
	}

	public static boolean checkIfError(Message<String> msg) {
		return msg.body().equals(QueryExecutor.EB_HEADER_ERROR);
	}

	public static void stopOnEOF(QueryRespTracker<String> tracker, HttpServerResponse response, QueryTradesParam queryParam) {
		tracker.incrementCurrCount();
		if (tracker.getCurentCnt() == tracker.getTotalCnt()) {
			//tracker.getConsumer().unregister();
			tracker.shutdownSubscriptions();
			response.end();
			log.info("Stopping observable after receiving all messages");
			long duration = System.currentTimeMillis() - queryParam.getRequestTime();
			MonitorUtil.sendEvent(queryParam.getRoutingContext().vertx(), MonitorUtil.MonitorType.MONITOR_PERFORMANCE_ENDPOINT, Long.toString(duration));
		}
	}

	public static void stopOnError(Message<String> msg, QueryRespTracker<String> tracker, HttpServerResponse response) {
		tracker.incrementCurrCount();
		tracker.getConsumer().unregister();
		// TODO: Improve error handling
		JsonObject error = new JsonObject();
		error.put("code", 500);
		error.put("message", "Encountered unexpected error while processing request.");
		response.end(error.encode());
		log.info("Stopping observable after receving an error" + msg.body());
	}

}
