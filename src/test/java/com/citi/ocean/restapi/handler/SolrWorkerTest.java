package com.citi.ocean.restapi.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;

import com.citi.ocean.restapi.worker.SolrWorker;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SolrWorkerTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void jsonParseTest() {
		
		HttpSolrClient httpSolrClient = new HttpSolrClient("http://fxomdlnx3u.nam.nsroot.net:8984/solr/datamart/");
		
		SolrQuery solrQuery = new SolrQuery();
		solrQuery.set("q", "option");
		solrQuery.set("wt", "json");
		solrQuery.set("group", "true");
		solrQuery.set("group.field", "category");
		solrQuery.set("group.limit", 5);
		solrQuery.set("group.ngroups", "true");
		solrQuery.set("omitHeader", "true");
		
		NoOpResponseParser jsonParser = new NoOpResponseParser();
		jsonParser.setWriterType("json");
		httpSolrClient.setParser(jsonParser);
		QueryRequest req = new QueryRequest(solrQuery);
		String result = "";
		try {
			NamedList<Object> response = httpSolrClient.request(req);
			result = response.get("response").toString();
		} catch (SolrServerException | IOException e) {
		}
		
		JsonObject raw = new JsonObject(result);

		List<JsonObject> jsons = new ArrayList<JsonObject>();
		raw.getJsonObject("grouped").getJsonObject("category")
		.getJsonArray("groups").stream()
		.filter(m -> m instanceof Map)
		.map(m -> ((Map)m).get("doclist"))
		.map(m -> ((Map)m).get("docs"))
		.flatMap(l -> ((List)l).stream())
		.filter(m -> m instanceof Map)
		// Compatibility issue with mvn build
		.forEach(x -> jsons.add(SolrWorker.buildOutput((Map)x)));
		String jsonOutput = (new JsonArray(jsons)).encode();
		Assert.assertEquals("[{\"category\":\"OTC\",\"subCategory\":\"BASE_PRODUCT_TYPE\",\"value\":\"FxOption\"},{\"category\":\"OTC\",\"subCategory\":\"PRODUCT_ID\",\"value\":\"Equity:Option:PriceReturnBasicPerformance:SingleName\"},{\"category\":\"OTC\",\"subCategory\":\"PRODUCT_ID\",\"value\":\"Equity:Option:PriceReturnBasicPerformance:SingleIndex\"},{\"category\":\"OTC\",\"subCategory\":\"PRODUCT_ID\",\"value\":\"InterestRate:Option:Swaption\"}]", jsonOutput);

	}
	
	@Test
	public void buildOutputTest() {
		Map<String, String> m = new HashMap<String, String>();
		m.put("category", "OTC");
		m.put("subCategory", "BASE_PRODUCT_TYPE");
		m.put("BASE_PRODUCT_TYPE_od_tg", "FxOption");
		JsonObject o = SolrWorker.buildOutput(m);
		
		Assert.assertEquals(m.get("category"), o.getString("category"));
		Assert.assertEquals(m.get("subCategory"), o.getString("subCategory"));
		Assert.assertEquals(m.get("BASE_PRODUCT_TYPE_od_tg"), o.getString("value"));
		
	}
	
	
	
}
