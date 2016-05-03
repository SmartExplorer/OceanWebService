package com.citi.ocean.restapi.service;

import java.util.List;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.datasource.NetezzaEndpoint;
import com.citi.ocean.restapi.datasource.SolrEndpoint;
import com.citi.ocean.restapi.handler.AggTradesHandler;
import com.citi.ocean.restapi.handler.FilterValuesHandler;
import com.citi.ocean.restapi.handler.QueryHintHandler;
import com.citi.ocean.restapi.handler.QueryTradesHandler;
import com.citi.ocean.restapi.tuple.ShardEndpointDetails;
import com.citi.ocean.restapi.util.ConfigUtil;
import com.citi.ocean.restapi.worker.MonitorWorker;
import com.citi.ocean.restapi.worker.NetezzaWorker;
import com.citi.ocean.restapi.worker.SolrWorker;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class VertxRunner {

	private static final Logger log = Logger.getLogger(VertxRunner.class);

	public static void main(String[] args) {
		VertxRunner runner = new VertxRunner();
		runner.start();
	}

	public void start() {
		Vertx vertx = Vertx.vertx();

		deployNetezzaVerticle(vertx);
		deploySolrVerticle(vertx);
		vertx.deployVerticle(MonitorWorker.class.getCanonicalName(), new DeploymentOptions().setWorker(true));
		createHttpServer(vertx);
		log.info("Started..");
	}

	private void deploySolrVerticle(Vertx vertx) {
		List<ShardEndpointDetails<SolrEndpoint>> endpoints = ConfigUtil.getSolrConfig();
		endpoints.stream().forEach(epd -> {
			JsonObject option = new JsonObject();
			option.put(ConfigUtil.URL, epd.getEndpoint().getUrl());
			option.put(ConfigUtil.SOLR_WORKER_TOPIC, epd.getShard().getTopic());
			vertx.deployVerticle(SolrWorker.class.getCanonicalName(), new DeploymentOptions().setWorker(true)
					.setInstances(epd.getEndpoint().getConnectionsCount()).setConfig(option));
		});
	}
	
	private void deployNetezzaVerticle(Vertx vertx) {
		List<ShardEndpointDetails<NetezzaEndpoint>> endpoints = ConfigUtil.getNetezzaConfig();
		endpoints.stream().forEach(epd -> {
			JsonObject option = new JsonObject();
			option.put(ConfigUtil.URL, epd.getEndpoint().getUrl());
			option.put(ConfigUtil.USER, epd.getEndpoint().getUser());
			option.put(ConfigUtil.PASSWORD, epd.getEndpoint().getPassword());
			option.put(ConfigUtil.NETEZZA_WORKER_TOPIC, epd.getShard().getTopic());
			vertx.deployVerticle(NetezzaWorker.class.getCanonicalName(), new DeploymentOptions().setWorker(true)
					.setInstances(epd.getEndpoint().getConnectionsCount()).setConfig(option));
		});
	}

	private void createHttpServer(Vertx vertx) {

		String port = System.getProperty("port", "9999");
		Router router = Router.router(vertx);
		// router.route().handler(BodyHandler.create());
		router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST)
				.allowedMethod(HttpMethod.OPTIONS).allowedHeader("X-PINGARUNER").allowedHeader("Content-Type"));
		router.get("/data/api/ocean/queryTrades").handler(QueryTradesHandler::handleRequest);
		router.get("/data/api/ocean/aggregateTrades").handler(AggTradesHandler::handleRequest);
		router.get("/data/api/ocean/queryHint").handler(QueryHintHandler::handleRequest);
		router.get("/data/api/ocean/filterValues").handler(FilterValuesHandler::handleRequest);
		router.route("/docs/*").handler(StaticHandler.create("swagger"));
		vertx.createHttpServer().requestHandler(router::accept).listen(Integer.parseInt(port));
	}

}
