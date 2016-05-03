package com.citi.ocean.restapi.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.KDBQuery;
import com.citi.ocean.restapi.datasource.base.NetezzaQuery;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.datasource.base.Shard;
import com.citi.ocean.restapi.tuple.FilterParam;
import com.citi.ocean.restapi.tuple.FilterValuesParam;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.util.BufferUtil;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.util.ExceptionUtil;
import com.citi.ocean.restapi.util.MonitorUtil;
import com.citi.ocean.restapi.validator.Validator;
import com.citi.ocean.restapi.worker.QueryRespTracker;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.rx.java.RxHelper;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.ConnectableObservable;

@SuppressWarnings("unused")
public class FilterValuesHandler {
	private static final String JSON_ARRAY_TAIL = "\"]";

	private static final String JSON_ARRAY_HEAD = "[\"";

	private static final String JSON_STRING_JOIN = "\",\"";

	private static final Logger log = Logger.getLogger(FilterValuesHandler.class);

	private static int uniqueId = 0;

	public static List<Cluster> clusters = ConfigUtil.getCluster(FilterValuesHandler.class);

	@SuppressWarnings("serial")
	public static Map<String, BiPredicate<Cluster, FilterValuesParam>> clusterFuncMap = new HashMap<String, BiPredicate<Cluster, FilterValuesParam>>() {
		{

			put("KDBCLUSTER1", FilterValuesHandler::canKDBClusterServiceThisReq);
			put("NETEZZA", FilterValuesHandler::canNetezzaClusterServiceThisReq);
		}
	};

	@SuppressWarnings("serial")
	public static Map<String, BiFunction<Shard, FilterValuesParam, Query>> queryBuilderFuncMap = new HashMap<String, BiFunction<Shard, FilterValuesParam, Query>>() {
		{
			put("KDB", FilterValuesHandler::buildKDBQuery);
			put("NETEZZA", FilterValuesHandler::buildNetezzaQuery);
		}
	};

	/* The entry point for user request */
	public static void handleRequest(RoutingContext routingContext) {
		FilterValuesParam queryParam = null;
		try { 
		queryParam = parseHttpRequest(routingContext);
		}catch(IllegalArgumentException e) {
			Validator.sendErrorMsgOnValidationException(routingContext, e);
			return;
		}
		Cluster cluster = selectCluster(queryParam);

		List<Query> queries = buildQuery(cluster, queryParam);

		EventBus eb = routingContext.vertx().eventBus();

		QueryRespTracker<Buffer> tracker = queryShards(queries, queryParam);

		sendData(tracker, queryParam, queries.get(0).getShard());

	}

	/********************************************
	 * Parsing and validating
	 ************************************/
	/*
	 * Parse http request, do basic data type validation while parsing and
	 * create query parameter object
	 */
	public static FilterValuesParam parseHttpRequest(RoutingContext routingContext) {
		/**
		 * httpRequest Parsing and validating
		 */
		MultiMap params = routingContext.request().params();
		String userId = Optional.ofNullable(params.get(Validator.HTTP_PARAM_NAME_USERID))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_MISSING_USERID));
		String filterField = Optional.ofNullable(params.get(Validator.HTTP_PARAM_FILTER_FIELD))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM));
//		String category = Optional.ofNullable(params.get(Validator.HTTP_PARAM_CATEGORY))
//				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM));
		String category = Optional
				.ofNullable(
						Validator.CATEGORY_SCHEMA.get(Optional.ofNullable(params.get(Validator.HTTP_PARAM_CATEGORY))
								.orElseThrow(() -> new IllegalArgumentException(
										ExceptionUtil.EXCEPTION_MSG_MISSING_CATEGORY))))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_CATEGORY));
		Map<String, FilterParam> filters = params.entries()
				.stream().filter(
						x -> x != null)
				.filter(x -> !Validator.FILTERS_IGNORE_HTTP_PARAM.contains(x.getKey().toUpperCase()))
				.filter(x -> ExceptionUtil.ifFalseThrowIllegalArgExc(
						Validator.SCHEMA.keySet().contains(x.getKey().toUpperCase()),
						ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM))
				.map(x -> Validator.FILTER_MAP.get(x.getKey()).apply(x))
				.collect(Collectors.toMap(x -> x.getKey(), x -> x));
		FilterValuesParam filterValuesParam = new FilterValuesParam(routingContext, userId, category, filterField, filters);
		validate(filterValuesParam);
		return filterValuesParam;
	}

	/* perform inter-field validations and throw appropriate exception */
	public static void validate(FilterValuesParam queryParam) {
		// date range validation
		if(queryParam.getFilters().keySet().stream()
				.anyMatch(x -> x.equals(Validator.HTTP_PARAM_TRADE_DATE_RANGE) 
						||x.equals(Validator.HTTP_PARAM_START_DATE) || x.equals(Validator.HTTP_PARAM_TO_DATE))) {
			Validator.dateFilterMerger(queryParam.getFilters());
		}
	}

	/********************************************
	 * Selecting cluster
	 ****************************************/

	/* Identify the cluster that can service the request */
	public static Cluster selectCluster(FilterValuesParam queryParam) {
		return clusters.stream().filter(cluster -> clusterFuncMap.get(cluster.getType()).test(cluster, queryParam))
				.findFirst().get();
		// return null;
	}

	/* Checks if KDB cluster can service this request */
	public static boolean canKDBClusterServiceThisReq(Cluster cluster, FilterValuesParam queryParam) {
		return false;
	}

	/* Checks if Netezza cluster can service this request */
	public static boolean canNetezzaClusterServiceThisReq(Cluster cluster, FilterValuesParam queryParam) {
		return true;
	}

	/********************************************
	 * Building Query
	 ****************************************/

	/* Build the query for each shard */
	public static List<Query> buildQuery(Cluster cluster, FilterValuesParam queryParam) {
		return cluster.getShards().stream().filter(x -> x.getTableName().equals(queryParam.getCategory()))
				.map(shard -> queryBuilderFuncMap.get(cluster.getType()).apply(shard, queryParam))
				.collect(Collectors.toList());
	}

	/* Builds KDB query */
	public static KDBQuery buildKDBQuery(Shard shard, FilterValuesParam queryParam) {
		return null;
	}

	/* Builds Netezza query */
	public static NetezzaQuery buildNetezzaQuery(Shard shard, FilterValuesParam queryParam) {
		if (!shard.getTableName().contains(queryParam.getCategory())) {
			return null;
		}
		NetezzaQuery query = new NetezzaQuery(shard);
		query.build(queryParam);
		return query;
	}

	/********************************************
	 * Querying Cluster
	 ****************************************/

	/* Query the cluster and get the observable */
	public static QueryRespTracker<Buffer> queryShards(List<Query> queries, FilterValuesParam queryParam) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();

		String replyTopic = generateReplyTopic();
		queries.stream().forEach(query -> queryShard(query, queryParam, replyTopic));

		MessageConsumer<Buffer> consumer = eb.consumer(replyTopic);
		QueryRespTracker<Buffer> tracker = new QueryRespTracker<Buffer>(queries.size(), consumer);
		return tracker;
	}

	public static String generateReplyTopic() {
		if (uniqueId > 100000) {
			uniqueId = 0;
		}
		uniqueId++;
		return "FilterValuesHandler" + uniqueId;
	}

	public static void queryShard(Query query, FilterValuesParam queryParam, String replyTopic) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();
		/* send the query message to the bus on the shard topic */
		String topic = query.getShard().getTopic();
		/* send the query message to the bus on the shard topic */
		DeliveryOptions options = new DeliveryOptions();
		options.addHeader(ConfigUtil.REPLY_TOPIC, replyTopic);
		eb.send(topic, query.getQuery(), options);
	}

	/********************************************
	 * Sending data
	 ******************************************/

	public static void sendData(QueryRespTracker<Buffer> tracker, FilterValuesParam queryParam, Shard shard) {
		HttpServerResponse response = queryParam.getRoutingContext().response();
		response.putHeader("content-type", "application/json; charset=utf-8").setChunked(true);
		Map<String, String> columnTypeSchema = ConfigUtil.getTableSchemaRegistry().getTableSchema(shard.getTableName())
				.getColumnTypes();

		List<String[]> combinedSchema = new ArrayList<String[]>();
		combinedSchema
				.add(new String[] { queryParam.getFilterField(), columnTypeSchema.get(queryParam.getFilterField()) });

		ConnectableObservable<Message<Buffer>> parentObsv = RxHelper.toObservable(tracker.getConsumer()).publish();
		parentObsv.filter(BufferUtil::checkIfError).subscribe(x -> stopOnError(x, tracker, response));
		Observable<Message<Buffer>> parentObs = parentObsv.takeUntil(new Func1<Message<Buffer>, Boolean>() {

			@Override
			public Boolean call(Message<Buffer> t1) {
				if (BufferUtil.checkIfEOF(t1)) {
					log.info("-------------SENDING EOF-------- ");
					sendToHttp(JSON_ARRAY_TAIL, response);
					stopOnEOF(tracker, response, queryParam);
					return true;
				} else
					return false;
			}

		});
		Observable<List<String>> outputObsv = parentObs.filter(BufferUtil::checkIfMessage)
				.map(message -> new BufferUtil(message.body(), combinedSchema).deserialize())
				.map(lst -> lst.stream().map(row -> row[0]).collect(Collectors.toList()));
		outputObsv.first().forEach(resultList -> {
			log.info("-------------SENDING HEADER and FIRST ");
			String output = resultList.stream().collect(Collectors.joining(JSON_STRING_JOIN));
			sendToHttp(JSON_ARRAY_HEAD + output, response);
		});
		outputObsv.skip(1).forEach(resultList -> {
			log.info("-------------SENDING INTERMEDIATE DATA ");
			String output = resultList.stream().collect(Collectors.joining(JSON_STRING_JOIN));
			sendToHttp(output, response);
		});

		parentObsv.connect();
	}

	/* Sends data to http socket */
	public static void sendToHttp(String message, HttpServerResponse response) {
		response.write(message);
	}

	public static void stopOnEOF(QueryRespTracker<Buffer> tracker, HttpServerResponse response, QueryTradesParam queryParam) {
		tracker.incrementCurrCount();
		if (tracker.getCurentCnt() == tracker.getTotalCnt()) {
			tracker.getConsumer().unregister();
			response.end();
			log.info("Stopping observable after receiving all messages");
			long duration = System.currentTimeMillis() - queryParam.getRequestTime();
			MonitorUtil.sendEvent(queryParam.getRoutingContext().vertx(), MonitorUtil.MonitorType.MONITOR_PERFORMANCE_ENDPOINT, Long.toString(duration));
		}
	}

	public static void stopOnError(Message<Buffer> msg, QueryRespTracker<Buffer> tracker, HttpServerResponse response) {
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
