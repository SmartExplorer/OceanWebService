package com.citi.ocean.restapi.handler;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import io.vertx.core.MultiMap;

public class StaticTestMap implements MultiMap{

	private Map<String, String> map = new HashMap<String, String>();
	
	public StaticTestMap (Map<String, String> map) {
		for (Map.Entry<String, String> entry : map.entrySet()) {
			this.map.put(entry.getKey().toUpperCase(), entry.getValue());
		}
//		this.map = map;
	}
	
	
	@Override
	public Iterator<Entry<String, String>> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String get(CharSequence name) {
		// TODO Auto-generated method stub
		return map.get(name);
	}

	@Override
	public String get(String name) {
		// TODO Auto-generated method stub
		return map.get(name.toUpperCase());
	}

	@Override
	public List<String> getAll(String name) {
		// TODO Auto-generated method stub
		return Arrays.asList(map.get(name));
	}

	@Override
	public List<String> getAll(CharSequence name) {
		// TODO Auto-generated method stub
		return Arrays.asList(map.get(name));
	}

	@Override
	public List<Entry<String, String>> entries() {
		// TODO Auto-generated method stub
		return new ArrayList<Entry<String, String>>(map.entrySet());
	}

	@Override
	public boolean contains(String name) {
		// TODO Auto-generated method stub
		return map.containsValue(name);
	}

	@Override
	public boolean contains(CharSequence name) {
		// TODO Auto-generated method stub
		return map.containsValue(name);
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return map.isEmpty();
	}

	@Override
	public Set<String> names() {
		// TODO Auto-generated method stub
		return map.keySet();
	}

	@Override
	public MultiMap add(String name, String value) {
		// TODO Auto-generated method stub
		map.put(name, value);
		return this;
	}

	@Override
	public MultiMap add(CharSequence name, CharSequence value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap add(String name, Iterable<String> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap add(CharSequence name, Iterable<CharSequence> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap addAll(MultiMap map) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap addAll(Map<String, String> headers) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap set(String name, String value) {
		map.put(name, value);
		return this;
	}

	@Override
	public MultiMap set(CharSequence name, CharSequence value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap set(String name, Iterable<String> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap set(CharSequence name, Iterable<CharSequence> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap setAll(MultiMap map) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap setAll(Map<String, String> headers) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiMap remove(String name) {
		// TODO Auto-generated method stub
		map.remove(name);
		return this;
	}

	@Override
	public MultiMap remove(CharSequence name) {
		// TODO Auto-generated method stub
		map.remove(name);
		return this;
	}

	@Override
	public MultiMap clear() {
		// TODO Auto-generated method stub
		map.clear();
		return this;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return map.size();
	}
	
	public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue());
    }

    public static <K, U> Collector<Map.Entry<K, U>, ?, ConcurrentMap<K, U>> entriesToConcurrentMap() {
        return Collectors.toConcurrentMap((e) -> e.getKey(), (e) -> e.getValue());
    }
	
}
