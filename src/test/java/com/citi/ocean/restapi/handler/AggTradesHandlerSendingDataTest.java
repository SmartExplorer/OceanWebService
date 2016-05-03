package com.citi.ocean.restapi.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class AggTradesHandlerSendingDataTest {
	private static final Logger log = Logger.getLogger(AggTradesHandlerSendingDataTest.class);
	
	@Test
	public void testMaxRowsResultMatch100Pass() throws IOException {
		int rounds = 30;
		String url = "http://localhost:9999/data/api/ocean/aggregateTrades?";
		String tradeRange = "TRADE_DATE_RANGE=";
		String userId = "userId=das&aggFields=TRADE_DATE&aggValues=NOTIONAL1_AMT,NOTIONAL1_AMT&aggOperations=count,sum&category=OTC";
		List<Integer> result = new LinkedList<>();
		for(int i = 0; i < rounds; ++i) {
			log.info("---------------round----" + i);
			int range = Math.toIntExact(Math.round(Math.random() * 100)) + 20;
			String u = url + tradeRange + range + "&" + userId;
			log.info(u);
			HttpURLConnection con = (HttpURLConnection) new URL(u).openConnection();

			// optional default is GET
			con.setRequestMethod("GET");
			int responseCode = con.getResponseCode();
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			JsonObject jObject = new JsonObject(response.toString());
			result.add(jObject.getJsonObject("Datarows").getJsonArray("rows").size());
			assertEquals(200, responseCode);
			assertNotNull(jObject.getJsonObject("Datarows").getJsonArray("rows"));
			assertNotNull(jObject.getInteger("ExecutionTime"));
			log.info("result=" + jObject.getJsonObject("Datarows").getJsonArray("rows").size());
		}
	}

}
