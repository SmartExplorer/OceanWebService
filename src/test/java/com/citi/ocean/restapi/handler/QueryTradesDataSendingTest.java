package com.citi.ocean.restapi.handler;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.Test;
import io.vertx.core.json.JsonObject;

public class QueryTradesDataSendingTest {
	private static final Logger log = Logger.getLogger(QueryTradesDataSendingTest.class);
	@Test
	public void testMaxRowsResultMatch100Pass() throws IOException {
		int rounds = 30;
		String url = "http://localhost:9999/data/api/ocean/queryTrades?&category=OTC&";
		String tradeRange = "TRADE_DATE_RANGE=";
		String userId = "userId=das";
		String maxRows = "maxRows=";
		List<Integer> result = new LinkedList<>();
		List<Integer> expected = new LinkedList<>();
		for(int i = 0; i < rounds; ++i) {
			log.info("---------------round----" + i);
			int range = Math.toIntExact(Math.round(Math.random() * 100)) + 20;
			int rows = Math.toIntExact(Math.round(Math.random() * 100));
			String u = url + tradeRange + range + "&" + userId + "&" + maxRows + rows;
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
			expected.add(rows);
			log.info("result=" + jObject.getJsonObject("Datarows").getJsonArray("rows").size() + "  " + "expected=" + (rows) );
		}
		assertArrayEquals(expected.toArray(), result.toArray());
	}

}
