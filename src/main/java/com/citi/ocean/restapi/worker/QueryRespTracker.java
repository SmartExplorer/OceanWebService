package com.citi.ocean.restapi.worker;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.eventbus.MessageConsumer;
import rx.Subscription;

public class QueryRespTracker<T> {
	private int totalCnt;
	private int curentCnt;
	private MessageConsumer<T> consumer;
	private List<Subscription> subscriptions = new ArrayList<Subscription>();

	public QueryRespTracker(int totalCnt, MessageConsumer<T> consumer) {
		this.totalCnt = totalCnt;
		this.consumer = consumer;
	}

	public void incrementCurrCount() {
		curentCnt++;
	}

	public int getTotalCnt() {
		return totalCnt;
	}

	public int getCurentCnt() {
		return curentCnt;
	}

	public MessageConsumer<T> getConsumer() {
		return consumer;
	}

	public List<Subscription> getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(List<Subscription> subscriptions) {
		this.subscriptions = subscriptions;
	}
	
	public void addSubscription(Subscription subscription) {
		subscriptions.add(subscription);
	}
	
	public void shutdownSubscriptions() {
		subscriptions.stream().forEach(sub -> sub.unsubscribe());
	}
}
