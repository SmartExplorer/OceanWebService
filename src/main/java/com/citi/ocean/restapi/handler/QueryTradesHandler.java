package com.citi.ocean.restapi.handler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import org.apache.http.annotation.Obsolete;
import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.base.Cluster;
import com.citi.ocean.restapi.datasource.base.KDBQuery;
import com.citi.ocean.restapi.datasource.base.NetezzaQuery;
import com.citi.ocean.restapi.datasource.base.Query;
import com.citi.ocean.restapi.datasource.base.Shard;
import com.citi.ocean.restapi.datasource.providers.QueryExecutor;
import com.citi.ocean.restapi.formatter.BaseFormatter;
import com.citi.ocean.restapi.formatter.JsonFormatter;
import com.citi.ocean.restapi.tuple.QueryTradesParam;
import com.citi.ocean.restapi.tuple.FilterParam;
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
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.ConnectableObservable;

/* The handler for query trades end point*/
public class QueryTradesHandler {
	/**
	 * @author hw72786
	 * 
	 *         FilterParam constructor map
	 */
//	private final static String RESULT_BUS = "result";
//	private final static String QUERY_BUS = "query";
	private static final Logger log = Logger.getLogger(QueryTradesHandler.class);
	public static List<Cluster> clusters = ConfigUtil.getCluster(QueryTradesHandler.class);
	private static int uniqueId = 0;

	@SuppressWarnings("serial")
	public static Map<String, BiPredicate<Cluster, QueryTradesParam>> clusterFuncMap = new HashMap<String, BiPredicate<Cluster, QueryTradesParam>>() {
		{
			// TODO: Should not hardcode cluster instances
			put("KDBCLUSTER1", QueryTradesHandler::canKDBClusterServiceThisReq);
			put("NETEZZA", QueryTradesHandler::canNetezzaClusterServiceThisReq);
		}
	};

	@SuppressWarnings("serial")
	public static Map<String, BiFunction<Shard, QueryTradesParam, Query>> queryBuilderFuncMap = new HashMap<String, BiFunction<Shard, QueryTradesParam, Query>>() {
		{
			put(Cluster.CLUSTER_TYPE_KDB, QueryTradesHandler::buildKDBQuery);
			put(Cluster.CLUSTER_TYPE_NETEZZA, QueryTradesHandler::buildNetezzaQuery);
		}
	};

	/* The entry point for user request */
	public static void handleRequest(RoutingContext routingContext) {
		QueryTradesParam queryParam = null;
		try {
			queryParam = parseHttpRequest(routingContext);
		} catch (IllegalArgumentException e) {
			Validator.sendErrorMsgOnValidationException(routingContext, e);
			return;
		}

		Cluster cluster = selectCluster(queryParam);

		List<Query> queries = buildQuery(cluster, queryParam);

		queryShards(queries, queryParam);

	}

	/********************************************
	 * Parsing and validating
	 ************************************/
	/*
	 * Parse http request, do basic data type validation while parsing and
	 * create query parameter object
	 */
	public static QueryTradesParam parseHttpRequest(RoutingContext routingContext) {
		/**
		 * httpRequest Parsing and validating
		 */
		MultiMap params = routingContext.request().params();
		String userId = Optional.ofNullable(params.get(Validator.HTTP_PARAM_NAME_USERID))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_MISSING_USERID));
		int limit = Optional.ofNullable(params.get(Validator.HTTP_PARAM_MAX_ROWS))
				.map(ExceptionUtil.rethrowIllegalArgExc(Integer::parseInt,
						Validator.HTTP_PARAM_MAX_ROWS + ExceptionUtil.EXCEPTION_MSG_ILLEGAL_DATATYPE))
				.orElse(Validator.DEFAULT_MAX_ROWS);
		String category = Optional
				.ofNullable(
						Validator.CATEGORY_SCHEMA.get(Optional.ofNullable(params.get(Validator.HTTP_PARAM_CATEGORY))
								.orElseThrow(() -> new IllegalArgumentException(
										ExceptionUtil.EXCEPTION_MSG_MISSING_CATEGORY))))
				.orElseThrow(() -> new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_ILLEGAL_CATEGORY));
		// v1: validate by checking if the column name is in column list/schema
		Optional<String[]> columns = Optional
				.ofNullable(
						params.get(Validator.HTTP_PARAM_COLUMNS))
				.map(x -> x
						.split(","))
				.filter(x -> Arrays.asList(x).stream()
						.filter(col -> !col.equals("OCEAN_PROCESS_DATE"))
						.allMatch(z -> ExceptionUtil.ifFalseThrowIllegalArgExc(
								ConfigUtil.SCHEMA_LIST.stream().map(ConfigUtil.getTableSchemaRegistry()::getTableSchema)
										.map(t -> t.getColumnSet()).anyMatch(t -> t.contains(z)),
								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_COLUMNS)));
		// v2: validate by getting the column type, include view only columns in TableSchema bean
//		Optional<String[]> columns = Optional
//				.ofNullable(
//						params.get(Validator.HTTP_PARAM_COLUMNS))
//				.map(x -> x
//						.split(","))
//				.filter(x -> Arrays.asList(x).stream()
//						.allMatch(z -> ExceptionUtil.ifFalseThrowIllegalArgExc(
//								ConfigUtil.SCHEMA_LIST.stream().map(ConfigUtil.getTableSchemaRegistry()::getTableSchema)
//										.map(t -> t.getColumnType(z)).anyMatch(t -> t != null),
//								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_COLUMNS)));
		Optional<String[]> orderByAsc = Optional
				.ofNullable(
						params.get(Validator.HTTP_PARAM_ORDER_BY_ASC))
				.map(x -> x
						.split(","))
				.filter(x -> Arrays.asList(x).stream()
						.allMatch(z -> ExceptionUtil.ifFalseThrowIllegalArgExc(
								ConfigUtil.SCHEMA_LIST.stream().map(ConfigUtil.getTableSchemaRegistry()::getTableSchema)
										.map(t -> t.getColumnSet()).anyMatch(t -> t.contains(z)),
								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_COLUMNS)));
		Optional<String[]> orderByDesc = Optional
				.ofNullable(
						params.get(Validator.HTTP_PARAM_ORDER_BY_DESC))
				.map(x -> x
						.split(","))
				.filter(x -> Arrays.asList(x).stream()
						.allMatch(z -> ExceptionUtil.ifFalseThrowIllegalArgExc(
								ConfigUtil.SCHEMA_LIST.stream().map(ConfigUtil.getTableSchemaRegistry()::getTableSchema)
										.map(t -> t.getColumnSet()).anyMatch(t -> t.contains(z)),
								ExceptionUtil.EXCEPTION_MSG_ILLEGAL_COLUMNS)));

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
		QueryTradesParam queryTradesParam = new QueryTradesParam(routingContext, userId, category, columns.orElse(null),
				limit, filters, orderByAsc, orderByDesc);
		validate(queryTradesParam);
		return queryTradesParam;
	}

	/* perform inter-field validations and throw appropriate exception */
	public static void validate(QueryTradesParam queryParam) {
		/**
		 * date range validation
		 */
		Validator.dateFilterMerger(queryParam.getFilters());

		/**
		 * category validation
		 */
		if (queryParam.getFilters().size() > 1 && !queryParam.getFilters().keySet().stream()
				.filter(x -> !x.equals(Validator.HTTP_PARAM_TRADE_DATE_RANGE))
				.filter(x -> !x.equals("OCEAN_PROCESS_DATE"))
				.allMatch(ConfigUtil.getTableSchemaRegistry()
						.getTableSchema(queryParam.getCategory()).getColumnSet()::contains)) {

			throw new IllegalArgumentException(ExceptionUtil.EXCEPTION_MSG_INCOMPATIBLE_CROSS_TABLE_FILTERS);
		}
		log.info("Validation Pass");
	}

	/********************************************
	 * Selecting cluster
	 ****************************************/

	/* Identify the cluster that can service the request */
	public static Cluster selectCluster(QueryTradesParam queryParam) {
		return clusters.stream().filter(cluster -> clusterFuncMap.get(cluster.getType()).test(cluster, queryParam))
				.findFirst().get();
		// return null;
	}

	/* Checks if KDB cluster can service this request */
	public static boolean canKDBClusterServiceThisReq(Cluster cluster, QueryTradesParam queryParam) {
		return false;
	}

	/* Checks if Netezza cluster can service this request */
	public static boolean canNetezzaClusterServiceThisReq(Cluster cluster, QueryTradesParam queryParam) {
		return true;
	}

	/********************************************
	 * Building Query
	 ****************************************/

	/* Build the query for each shard */
	public static List<Query> buildQuery(Cluster cluster, QueryTradesParam queryParam) {
		return cluster.getShards().stream().filter(x -> x.getTableName().equals(queryParam.getCategory()))
				.map(shard -> queryBuilderFuncMap.get(cluster.getType()).apply(shard, queryParam))
				.collect(Collectors.toList());
	}
	

	/* Builds KDB query */
	public static KDBQuery buildKDBQuery(Shard shard, QueryTradesParam queryParam) {
		return null;
	}

	/* Builds Netezza query */
	public static NetezzaQuery buildNetezzaQuery(Shard shard, QueryTradesParam queryParam) {
		NetezzaQuery query = new NetezzaQuery(shard);
		query.build(queryParam);
		query.buildCountQuery(queryParam);
		return query;
	}

	/********************************************
	 * Querying Cluster
	 ****************************************/

	/* Query the cluster and get the observable */
	public static QueryRespTracker<Buffer> queryShards(List<Query> queries, QueryTradesParam queryParam) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();

		String replyTopicGenerated = generateReplyTopic();
		MessageConsumer<Buffer> resultConsumer = eb.consumer(replyTopicGenerated);
		QueryRespTracker<Buffer> tracker = new QueryRespTracker<Buffer>(queries.size(), resultConsumer);
		subscribeObserver3(tracker, queryParam, queries.get(0).getShard());
		queries.stream().forEach(query -> queryShard(query, queryParam, replyTopicGenerated));
		return new QueryRespTracker<Buffer>(queries.size(), resultConsumer);
	}

	public static String generateReplyTopic() {
		if (uniqueId > 100000) {
			uniqueId = 0;
		}
		uniqueId++;
		return "QueryTradesHandler" + uniqueId;
	}

	public static void queryShard(Query query, QueryTradesParam queryParam,
			String replyTopicGenerated) {
		EventBus eb = queryParam.getRoutingContext().vertx().eventBus();
		String shardTopic = query.getShard().getTopic();
		// use send since only 1 worker needs to take the query
		eb.send(shardTopic, query.getQuery(), 
				new DeliveryOptions().addHeader(ConfigUtil.REPLY_TOPIC, replyTopicGenerated));
		eb.send(shardTopic, query.getCountQuery(),
				new DeliveryOptions().addHeader(QueryExecutor.COUNT_HEADER, query.getQuery())
						.addHeader(ConfigUtil.REPLY_TOPIC, replyTopicGenerated));	
	}

	/********************************************
	 * Subscribe to Observer
	 ******************************************/
	/**
	 * Buffer&Reactive Approach, buffer items until count messages arrives, then
	 * send asyncly
	 * 
	 * @param tracker
	 * @param queryParam
	 * @param shard
	 */
	@Obsolete
	public static void subscribeObserver(QueryRespTracker<Buffer> tracker, QueryTradesParam queryParam, Shard shard) {
		BaseFormatter formatter = getFormatter(queryParam);
		final HttpServerResponse response = queryParam.getRoutingContext().response();
		ConnectableObservable<Message<Buffer>> observable = RxHelper.toObservable(tracker.getConsumer()).publish();
		response.putHeader("content-type", "application/json; charset=utf-8").setChunked(true);
		Map<String, String> columnTypeSchema = BufferUtil.getColumnTypeSchema(shard.getTableName());
		List<String[]> combinedSchema = Arrays
				.asList(Optional.ofNullable(queryParam.getColumns())
						.orElse(ConfigUtil.getTableSchemaRegistry().getTableSchema(shard.getTableName()).getColumnSet()
								.toArray(new String[0])))
				.stream().map(z -> new String[] { z, columnTypeSchema.get(z) }).collect(Collectors.toList());
		log.info("-------------INITIAL-------" + observable.toString());

		Action1<Throwable> onError = new Action1<Throwable>() {

			@Override
			public void call(Throwable t1) {
				// TODO Auto-generated method stub
				log.info("Stopping observable after receving an error" + t1.getMessage());
				tracker.incrementCurrCount();
				tracker.getConsumer().unregister();
				// TODO: Improve error handling
				JsonObject error = new JsonObject();
				error.put("code", 500);
				error.put("message", t1.getMessage());
				response.end(error.encode());
			}

		};

		observable.filter(BufferUtil::checkIfError).subscribe(x -> stopOnError(x, tracker, response));
		Observable<List<Message<Buffer>>> ob = observable.takeUntil(new Func1<Message<Buffer>, Boolean>() {

			@Override
			public Boolean call(Message<Buffer> t1) {
				return BufferUtil.checkIfCountMessage(t1);
			}

		}).buffer(1000);

		ob.first().doOnError(onError::call).subscribe(msgList -> {
			Optional<Message<Buffer>> count = msgList.stream().filter(BufferUtil::checkIfCountMessage).findAny();
			List<Message<Buffer>> messages = msgList.stream().filter(BufferUtil::checkIfNotCountMessage)
					.filter(BufferUtil::checkIfMessage).collect(Collectors.toList());
			Optional<Message<Buffer>> EOF = msgList.stream().filter(BufferUtil::checkIfEOF).findAny();
			count.ifPresent(x -> {
				log.info("-------SENDING HEAD--------");
				sendToHttp(formatter.formatDeserializedHeaderBuffer(combinedSchema, x.body().getLong(0)), response);

			});
			String encodedMsg = messages.stream().map(x -> {
				return new BufferUtil(x.body(), combinedSchema).deserialize();
			}).map(formatter::formatDeserializedContentBuffer).reduce(new BinaryOperator<String>() {
				public String apply(String arg0, String arg1) {
					return arg0 + "," + arg1;
				}
			}).get();

			EOF.ifPresent(x -> {
				log.info("-------SENDING BODY--------");
				sendToHttp(encodedMsg, response);
				log.info("-------------SENDING EOF-------- ");
				clearOnEOF(tracker, response);
				sendToHttp(formatter.formatDeserializedEndBuffer(
						Long.parseLong(x.headers().get(QueryExecutor.EXECUTION_TIME_HEADER))), response);
				stopOnEOF(tracker, response, queryParam);
			});
			if (!EOF.isPresent()) {
				log.info("-------SENDING BODY--------");
				sendToHttp(encodedMsg + ",", response);
			}
		});

		Observable<Message<Buffer>> body = observable.skipUntil(ob).takeUntil(new Func1<Message<Buffer>, Boolean>() {

			@Override
			public Boolean call(Message<Buffer> t1) {
				if (BufferUtil.checkIfEOF(t1)) {
					log.info("-------------SENDING EOF-------- ");
					clearOnEOF(tracker, response);
					sendToHttp(formatter.formatDeserializedEndBuffer(
							Long.parseLong(t1.headers().get(QueryExecutor.EXECUTION_TIME_HEADER))), response);
					stopOnEOF(tracker, response, queryParam);
					return true;
				} else
					return false;
			}

		}).filter(BufferUtil::checkIfMessage).filter(BufferUtil::checkIfNotCountMessage);

		body.first().doOnError(onError::call)
				.map(message -> new BufferUtil(message.body(), combinedSchema).deserialize()).subscribe(x -> {
					log.info("-------SENDING BODY--------");
					sendToHttp(formatter.formatDeserializedContentBuffer(x), response);
				});

		body.skip(1).map(message -> new BufferUtil(message.body(), combinedSchema).deserialize())
				.doOnError(onError::call).forEach(resultBlock -> {
					log.info("-------SENDING BODY--------");
					sendToHttp(formatter.formatDeserializedContentBuffer(",", resultBlock), response);
				});

		observable.connect();
	}

	/**
	 * @param tracker
	 * @param queryParam
	 * @param shard
	 */
	public static void subscribeObserver3(QueryRespTracker<Buffer> tracker, QueryTradesParam queryParam, Shard shard) {
		BaseFormatter formatter = getFormatter(queryParam);
		final HttpServerResponse response = queryParam.getRoutingContext().response();
		ConnectableObservable<Message<Buffer>> observable = RxHelper.toObservable(tracker.getConsumer()).publish();
		response.putHeader("content-type", "application/json; charset=utf-8").setChunked(true);

		Map<String, String> columnTypeSchema = BufferUtil.getColumnTypeSchema(shard.getTableName());
		List<String[]> combinedSchema = Arrays
				.asList(Optional.ofNullable(queryParam.getColumns())
						.orElse(ConfigUtil.getTableSchemaRegistry().getTableSchema(shard.getTableName()).getColumnSet()
								.toArray(new String[0])))
				.stream()
				.map(z -> NetezzaQuery.getColumnSchemaMap(z, columnTypeSchema))
				.collect(Collectors.toList());
		log.info("-------------INITIAL-------");

		observable.filter(BufferUtil::checkIfError).subscribe(x -> stopOnError(x, tracker, response));

		observable.filter(BufferUtil::checkIfNotError).first().subscribe(x -> {
			log.info("----------SENDING HEAD------------ ");
			sendToHttp(formatter.formatDeserializedHeaderBuffer(combinedSchema, -1L), response);
		});
		observable.filter(BufferUtil::checkIfMessage).filter(BufferUtil::checkIfNotCountMessage).first()
				.map(message -> new BufferUtil(message.body(), combinedSchema).deserialize()).subscribe(resultBlock -> {
					log.info("--------------SENDING FIRSTBODY------------");
					sendToHttp(formatter.formatDeserializedContentBuffer(resultBlock), response);
				});

		observable.filter(BufferUtil::checkIfMessage).filter(BufferUtil::checkIfNotCountMessage).skip(1)
				.map(message -> new BufferUtil(message.body(), combinedSchema).deserialize()).subscribe(resultBlock -> {
					log.info("--------------SENDING BODY-------------- ");
					sendToHttp(formatter.formatDeserializedContentBuffer(formatter.getDelimiterBetweenHdrAndContent(), resultBlock), response);
				});

		/**
		 * Either COUNT msg returned first or EOF msg returned first Always run
		 * only 1 EOF
		 */

		CountObject countObj = new CountObject();

		Subscription countSub = observable.filter(BufferUtil::checkIfCountMessage).subscribe(countMsg -> {
			log.info("------SET COUNT--------");
			countObj.setCount(countMsg.body().getLong(0));
		});

		observable.filter(BufferUtil::checkIfEOF).subscribe(x -> {
			if (countObj.getCount() < 0) {
				countSub.unsubscribe();
				observable.filter(BufferUtil::checkIfCountMessage).subscribe(countMsg -> {
					log.info("-------------SENDING EOF1---------");
					String message = formatter.formatDeserializedEndBuffer(
							Long.parseLong(x.headers().get(QueryExecutor.EXECUTION_TIME_HEADER)), 
							countMsg.body().getLong(0));
					sendEOR(message, tracker, response, queryParam);
				});
			} else {
				log.info("-------------SENDING EOF2-----------");
				String message = formatter.formatDeserializedEndBuffer(Long.parseLong(x.headers().get(QueryExecutor.EXECUTION_TIME_HEADER)),
						countObj.getCount());
				sendEOR(message, tracker, response, queryParam);
			}
		});
		observable.connect();
	}

	static class CountObject {
		private long count = -1;

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}
	}

	private static void sendEOR(String message, QueryRespTracker<Buffer> tracker,
			HttpServerResponse response, QueryTradesParam queryParam) {
		sendToHttp(message, response);
		clearOnEOF(tracker, response);
		stopOnEOF(tracker, response, queryParam);
	}

	/* Create the formatter from the factory */
	public static BaseFormatter getFormatter(QueryTradesParam queryParam) {
		//TODO select the formatter
		BaseFormatter formatter = new JsonFormatter();
		return formatter;
	}

	/* Sends data to http socket */
	public static void sendToHttp(String message, HttpServerResponse response) {
		response.write(message);
	}

	public static void stopOnEOF(QueryRespTracker<Buffer> tracker, HttpServerResponse response, QueryTradesParam queryParam) {
		tracker.getConsumer().unregister();
		if (tracker.getCurentCnt() >= tracker.getTotalCnt()) {
			response.end();
			log.info("Stopping response after receiving all messages");
			long duration = System.currentTimeMillis() - queryParam.getRequestTime();
			MonitorUtil.sendEvent(queryParam.getRoutingContext().vertx(), MonitorUtil.MonitorType.MONITOR_PERFORMANCE_ENDPOINT, Long.toString(duration));
		}
	}

	public static void clearOnEOF(QueryRespTracker<Buffer> tracker, HttpServerResponse response) {
		tracker.incrementCurrCount();
		if (tracker.getCurentCnt() >= tracker.getTotalCnt()) {
			tracker.getConsumer().unregister();
			log.info("Clear observable after receiving all messages");
		}
	}

	public static void stopOnError(Message<Buffer> msg, QueryRespTracker<Buffer> tracker, HttpServerResponse response) {
		log.info("Stopping observable after receving an error" + msg.body());
		tracker.incrementCurrCount();
		tracker.getConsumer().unregister();
		// TODO: Improve error handling
		JsonObject error = new JsonObject();
		error.put("code", 500);
		error.put("message", "Encountered" + msg.body() + " while processing request.");
		response.end(error.encode());
	}

}
