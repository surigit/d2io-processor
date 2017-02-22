/**
 * 
 */
package com.suren.mbr.d2io.config;

import java.util.Map;

/**
 * @author Surendra Myneni
 *
 */
public class D2IOConfig {

	private Map<String, String> engine;
	private Producer producer;
	private Consumer consumer;

	public Map<String, String> getEngine() {
		return engine;
	}

	public void setEngine(Map<String, String> engine) {
		this.engine = engine;
	}

	
	
	public Producer getProducer() {
		return producer;
	}

	public void setProducer(Producer producer) {
		this.producer = producer;
	}

	public Consumer getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}

	@Override
	public String toString() {
		return "D2IOConfig [engine=" + engine + ", producer=" + producer + ", consumer=" + consumer + "]";
	}


	
}
