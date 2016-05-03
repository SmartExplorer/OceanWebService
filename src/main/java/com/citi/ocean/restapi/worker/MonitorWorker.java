package com.citi.ocean.restapi.worker;

import org.apache.log4j.Logger;

import com.citi.ocean.restapi.util.MonitorUtil;

import io.vertx.core.AbstractVerticle;

public class MonitorWorker extends AbstractVerticle {
	private static final Logger log = Logger.getLogger(MonitorUtil.class);

	@Override
	public void start() throws Exception {
		log.info("Start(): Initialize Monitor Consumer.");
		MonitorUtil.logEvent(vertx);
	}
}
