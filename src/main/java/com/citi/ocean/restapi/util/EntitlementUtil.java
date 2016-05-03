package com.citi.ocean.restapi.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EntitlementUtil {

	@SuppressWarnings("serial")
	public static Set<String> getEntitledColumnsForQueryTrades(String userId){
		return new HashSet<String>() {{
		    add("UITID");
		}}; 
	}
	
	public static Map<String,String[]> getEntitledPredicatesForQueryTrades(String userId){
		return new HashMap<String,String[]>();
	}
}
