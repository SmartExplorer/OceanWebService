package com.citi.ocean.restapi.handler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.KDBQuery;
import com.citi.ocean.restapi.datasource.base.NetezzaQuery;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.datasource.base.Shard;
import com.citi.ocean.restapi.datasource.providers.QueryExecutor;
import com.citi.ocean.restapi.formatter.BaseFormatter;
import com.citi.ocean.restapi.formatter.JsonFormatter;
import com.citi.ocean.restapi.tuple.AggQueryTradesParam;
import com.citi.ocean.restapi.tuple.FilterParam;
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
import rx.observables.ConnectableObservable;

public class AggTradesHandler {

	public static List<Cluster> cluster = ConfigUtil.getCluster(AggTradesHandler.class);
	private static final Logger log = Logger.getLogger(AggTradesHandler.class);
	private final static String RESULT_BUS = "result";

	private static int uniqueId = 0;

	@SuppressWarnings("serial")
	public static Map<String, BiPredicate<Cluster, AggQueryTradesParam>> clusterFuncMap = new HashMap<String, BiPredicate<Cluster, AggQueryTradesParam>>() {
		{

			put("KDBCLUSTER1", AggTradesHandler::canKDBClusterServiceThisReq);
			put("NETEZZA", AggTradesHandler::canNetezzaClusterServiceThisReq);

		}
	};

	@SuppressWarnings("serial")
	public static Map<String, BiFunction<Shard, AggQueryTradesParam, Query>> queryBuilderFuncMap = new HashMap<String, BiFunction<Shard, AggQueryTradesParam, Query>>() {
		{

			put("KDB", AggTradesHandler::buildKDBQuery);
			put("NETEZZA", AggTradesHandler::buildNetezzaQuery);

		}
	};

	/* The entry point for user request */
	public static void handleRequest(RoutingContext routingContext) {
		AggQueryTradesParam queryParam = null;

		try {
			queryParam = parseHttpRequest(routingContext);
		} catch (IllegalArgumentException e) {
			Validator.sendErrorMsgOnValidationException(routingContext, e);
			return;
		}

		Cluster cluster = selectCluster(queryParam);

		List<Query> queries = buildQuery(cluster, queryParam);

		EventBus eb = routingContext.vertx().eventBus();

		eb.consumer(RESULT_BUS, message -> {
			routingContext.request().response().end((String) message.body());
		});

		QueryRespTracker<Buffer> tracker = queryShards(queries, queryParam);

		// TODO get shard hard coded here!!!
		sendData(tracker, queryParam, queries.get(0).getShard());

	}

	/********************************************
	 * Parsing and validating
	 ************************************/

	/*
	 * Parse http request, do basic data type validation while parsing and
	 * create query parameter object
	 */
	public static AggQueryTradesParam parseHttpRequest(RoutingContext routingContext) {
		MultiMap params = routingContext.request().params();
		/**
		 * Aggregation params validation
		 */
		List<String> aggFields = Arrays.asList(
				Optional.ofNullable(params.get(Validator.HTTP_PARAM_AGGREGATE_FIELDS))
				.map(String::toUpperCase)
				.map(x -> x.split(","))
				.filter(x -> Arrays.asList(x).stream()
						.allMatch(ExceptionUtil.ifFalseThrowIllegalArgExc(Validator.AGGREGATE_FIELDS_MAP::contains,
								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_AGGFIELD)))
				.orElseThrow(
						() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISSING))
				);

		List<String> aggValues = Arrays.asList(
				Optional
				.ofNullable(
						params.get(Validator.HTTP_PARAM_AGGREGATE_VALUES))
				.map(String::toUpperCase)
				.map(x -> x
						.split(","))
				.filter(x -> Arrays.asList(x).stream()
						.allMatch(z -> ExceptionUtil.ifFalseThrowIllegalArgExc(
								ConfigUtil.SCHEMA_LIST.stream().map(ConfigUtil.getTableSchemaRegistry()::getTableSchema)
										.map(t -> t.getColumnSet()).anyMatch(t -> t.contains(z)),
								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_AGGREGATION_VALUE)))
				.orElseThrow(
						() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISSING))
				);

		List<String> aggOperations = Arrays.asList( 
				Optional.ofNullable(params.get(Validator.HTTP_PARAM_AGGREGATE_OPERATIONS))
				.map(String::toUpperCase)
				.map(x -> x.split(","))
				.filter(x -> Arrays.asList(x).stream()
						.allMatch(ExceptionUtil.ifFalseThrowIllegalArgExc(Validator.AGGREGATE_OPERATIONS_MAP::contains,
								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_AGGOPERATION)))
				.orElseThrow(
						() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISSING)));

		List<String> aggValueOp = IntStream.range(0, aggOperations.size())
				.mapToObj(i -> NetezzaQuery.buildAgg(aggOperations.get(i), aggValues.get(i))).collect(Collectors.toList());
				
		
		Optional<String[]> orderByAsc = Optional
				.ofNullable(
						params.get(Validator.HTTP_PARAM_ORDER_BY_ASC))
				.map(String::toUpperCase)
				.map(x -> x
						.split(","))
				.filter(x -> Arrays.asList(x).stream().allMatch(z -> 
							ExceptionUtil.ifFalseThrowIllegalArgExc(
								Validator.validateOrderBy(aggFields, aggValueOp, z),
								ExceptionUtil.EXCEPTION_MSG_INVALID_ORDER_FIELD)));
		Optional<String[]> orderByDesc = Optional
				.ofNullable(
						params.get(Validator.HTTP_PARAM_ORDER_BY_DESC))
				.map(String::toUpperCase)
				.map(x -> x
						.split(","))
				.filter(x -> Arrays.asList(x).stream().allMatch(z -> 
				ExceptionUtil.ifFalseThrowIllegalArgExc(
					Validator.validateOrderBy(aggFields, aggValueOp, z),
					ExceptionUtil.EXCEPTION_MSG_INVALID_ORDER_FIELD)));
//				.filter(x -> Arrays.asList(x).stream()
//						.allMatch(z -> ExceptionUtil.ifFalseThrowIllegalArgExc(
//								ConfigUtil.SCHEMA_LIST.stream().map(ConfigUtil.getTableSchemaRegistry()::getTableSchema)
//										.map(t -> t.getColumnSet()).anyMatch(t -> t.contains(z)),
//								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_COLUMNS)));

		/**
		 * httpRequest Parsing and validating
		 */

		String userId = Optional.ofNullable(params.get(Validator.HTTP_PARAM_NAME_USERID))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_MISSING_USERID));
		String category = Optional
				.ofNullable(
						Validator.CATEGORY_SCHEMA.get(Optional.ofNullable(params.get(Validator.HTTP_PARAM_CATEGORY))
								.orElseThrow(() -> new IllegalArgumentException(
										ExceptionUtil.EXCEPTION_MSG_MISSING_CATEGORY))))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_CATEGORY));
		Map<String, FilterParam> filters = params.entries().stream().filter(x -> x != null)
				.filter(x -> !Validator.FILTERS_IGNORE_HTTP_PARAM.contains(x.getKey().toUpperCase())) // ignore
																						// non-filter
																						// param
				.filter(x -> ExceptionUtil.ifFalseThrowIllegalArgExc(
						Validator.SCHEMA.keySet().contains(x.getKey().toUpperCase()),
						ExceptionUtil.EXCEPTION_MSG_ILLEGAL_QUERY_PARAM)) // validate
																			// filter
				.filter(x -> ExceptionUtil. // validate input data type
						ifFalseThrowIllegalArgExc(
								Validator.VALIDATOR_MAP.get(Validator.SCHEMA.get(x.getKey())).test(x.getValue()),
								x.getKey() + ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE))
				.map(x -> Validator.FILTER_MAP.get(x.getKey()).apply(x)) // apply
																			// filter
																			// creator
																			// function
				.collect(Collectors.toMap(x -> x.getKey(), x -> x));
		/**
		 * merge date filters into a single date range filter with validation
		 */
		AggQueryTradesParam aggQueryTradesParam = new AggQueryTradesParam(routingContext, userId, category, aggFields,
				aggValues, aggOperations, filters, orderByAsc, orderByDesc);
		validate(aggQueryTradesParam);
		return aggQueryTradesParam;
	}

	/* perform inter-field validations and throw appropriate exception */
	public static void validate(AggQueryTradesParam queryParam) {
		// validate agg params integrity
		String[] aggFields = queryParam.getAggFields();

		String[] aggValues = queryParam.getAggValues();

		String[] aggOperations = queryParam.getAggOperations();

		Validator.dateFilterMerger(queryParam.getFilters());

		if (aggValues.length != aggOperations.length)
			throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISSING);
		IntStream.range(0, aggValues.length).filter(i -> !Validator.NUMERIC_COLUMN_SCHEMA.contains(aggValues[i]))
				.filter(i -> !aggOperations[i].equals("COUNT")).findAny().ifPresent(i -> {
					throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_AGGREGATION_PARAMS_MISMATCH);
				});
		/**
		 * category validation
		 */
		if (queryParam.getFilters().size() > 1 && !queryParam.getFilters().keySet().stream()
				.filter(x -> !x.equals("TRADE_DATE_RANGE"))
				.filter(x -> !x.equals("OCEAN_PROCESS_DATE"))
				.allMatch(ConfigUtil.getTableSchemaRegistry()
						.getTableSchema(queryParam.getCategory()).getColumnSet()::contains)) {

			throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_INCOMPATIBLE_CROSS_TABLE_FILTERS);
		}

	}

	/********************************************
	 * Selecting cluster
	 ****************************************/

	/* Identify the cluster that can service the request */
	public static Cluster selectCluster(AggQueryTradesParam queryParam) {
		return cluster.stream().filter(cluster -> clusterFuncMap.get(cluster.getType()).test(cluster, queryParam))
				.findFirst().get();
	}

	/* Checks if KDB cluster can service this request */
	public static boolean canKDBClusterServiceThisReq(Cluster cluster, AggQueryTradesParam queryParam) {
		return false;
	}

	/* Checks if Netezza cluster can service this request */
	public static boolean canNetezzaClusterServiceThisReq(Cluster cluster, AggQueryTradesParam queryParam) {
		return true;
	}

	/********************************************
	 * Building Query
	 ****************************************/

	/* Build the query for each shard */
	public static List<Query> buildQuery(Cluster cluster, AggQueryTradesParam queryParam) {
		return cluster.getShards().stream().filter(x -> x.getTableName().equals(queryParam.getCategory()))
				.map(shard -> queryBuilderFuncMap.get(cluster.getType()).apply(shard, queryParam))
				.collect(Collectors.toList());
	}

	/* Builds KDB query */
	public static KDBQuery buildKDBQuery(Shard shard, AggQueryTradesParam queryParam) {
		return null;
	}

	/* Builds Netezza query */
	public static NetezzaQuery buildNetezzaQuery(Shard shard, AggQueryTradesParam queryParam) {
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
	public static QueryRespTracker<Buffer> queryShards(List<Query> queries, AggQueryTradesParam queryParam) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();
		// Observable<Message<Buffer>> observable =
		// RxHelper.toObservable(eb.<Buffer>consumer(queries.get(0).getShard().getTopic()));
		/* queries.stream().forEach(query -> queryShard(query)); */

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
		return "QueryAggHandler" + uniqueId;
	}

	public static void queryShard(Query query, AggQueryTradesParam queryParam, String replyTopic) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();
		/* send the query message to the bus on the shard topic */
		String topic = query.getShard().getTopic();
		/* send the query message to the bus on the shard topic */
		DeliveryOptions options = new DeliveryOptions();
		options.addHeader(ConfigUtil.REPLY_TOPIC, replyTopic);
		eb.send(topic, query.getQuery(), options);
	}
	
	/* Create the formatter from the factory */
	public static BaseFormatter getFormatter(QueryTradesParam queryParam) {
		//TODO select the formatter
		BaseFormatter formatter = new JsonFormatter();
		return formatter;
	}
	/********************************************
	 * Sending data
	 ******************************************/

	public static void sendData(QueryRespTracker<Buffer> tracker, AggQueryTradesParam queryParam, Shard shard) {
		BaseFormatter formatter = getFormatter(queryParam);
		HttpServerResponse response = queryParam.getRoutingContext().response();
		response.putHeader("content-type", "application/json; charset=utf-8").setChunked(true);
		Map<String, String> columnTypeSchema = BufferUtil.getColumnTypeSchema(shard.getTableName());

		String[] aggFields = queryParam.getAggFields();
		String[] aggvalues = queryParam.getAggValues();
		String[] aggOps = queryParam.getAggOperations();

		List<String[]> aggFieldsSchema = Arrays
				.asList(Optional.ofNullable(aggFields)
						.orElse(ConfigUtil.getTableSchemaRegistry().getTableSchema(shard.getTableName()).getColumnSet()
								.toArray(new String[0])))
				.stream().map(z -> new String[] { z, columnTypeSchema.get(z) }).collect(Collectors.toList());

		aggFieldsSchema
				.addAll(IntStream.range(0, aggvalues.length)
						.mapToObj(i -> new String[] { String.join(" ", aggvalues[i], aggOps[i]),
								NetezzaQuery.getColumnSchemaMap(aggOps[i], aggvalues[i], columnTypeSchema) })
				.collect(Collectors.toList()));

		ConnectableObservable<Message<Buffer>> parentObsv = RxHelper.toObservable(tracker.getConsumer()).publish();
		parentObsv.filter(BufferUtil::checkIfError).subscribe(x -> stopOnError(x, tracker, response));

		parentObsv.filter(BufferUtil::checkIfMessage).map(x -> x.body()).first()
				.map((message -> new BufferUtil(message, aggFieldsSchema).deserialize())).subscribe(x -> {
					log.info("--------------SENDING HEAD ");
					sendToHttp(formatter.formatDeserializedHeaderBuffer(x, aggFieldsSchema, -1L), response);
				});

		parentObsv.filter(BufferUtil::checkIfMessage).skip(1)
				.map(message -> new BufferUtil(message.body(), aggFieldsSchema).deserialize()).forEach(resultBlock -> {
					log.info("--------------SENDING BODY ");
					sendToHttp(formatter.formatDeserializedContentBuffer(",", resultBlock), response);
				});

		parentObsv.filter(BufferUtil::checkIfEOF).subscribe(x -> {
			log.info("-------------SENDING EOF ");
			sendToHttp(formatter.formatDeserializedEndBuffer(
					Long.parseLong(x.headers().get(QueryExecutor.EXECUTION_TIME_HEADER))), response);
			stopOnEOF(tracker, response, queryParam);
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
