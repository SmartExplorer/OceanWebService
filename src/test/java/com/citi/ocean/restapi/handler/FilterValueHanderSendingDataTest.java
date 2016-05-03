package com.citi.ocean.restapi.handler;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.junit.Test;

import io.vertx.core.json.JsonArray;

public class FilterValueHanderSendingDataTest {
	private static final Logger log = Logger.getLogger(FilterValueHanderSendingDataTest.class);
	@Test
	public void test() throws MalformedURLException, IOException {
		int rounds = 200;
		String url = "http://localhost:9999/data/api/ocean/filterValues?TRADE_DATE_FROM=20151001&TRADE_DATE_TO=20151002&userId=et&filterField=SRC_SYS&category=OTC";
		for(int i = 0; i < rounds; ++i) {
			log.info("---------------round----" + i);
			HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
			// optional default is GET
			con.setRequestMethod("GET");
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			JsonArray ja = new JsonArray(response.toString());
			log.debug(ja.toString());
			assertEquals(3, ja.size());
		}
	}

}
