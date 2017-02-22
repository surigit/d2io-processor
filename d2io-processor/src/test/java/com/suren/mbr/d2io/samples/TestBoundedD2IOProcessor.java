package com.suren.mbr.d2io.samples;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import com.suren.mbr.d2io.config.Consumer;
import com.suren.mbr.d2io.config.D2IOConfig;
import com.suren.mbr.d2io.config.Producer;
import com.suren.mbr.d2io.intf.ConsumerCallback;
import com.suren.mbr.d2io.intf.D2IOProcessor;
import com.suren.mbr.d2io.intf.FileInputDatasource;
import com.suren.mbr.d2io.intf.ProducerDatasource;
import com.suren.mbr.d2io.intf.ResultSummary;
import com.suren.mbr.d2io.intf.impl.BoundedD2IOProcessor;
import com.suren.mbr.d2io.intf.impl.D2IOContext;

public class TestBoundedD2IOProcessor {
	private static Log log = LogFactory.getLog(TestBoundedD2IOProcessor.class.getName());

	@Before
	public void setUp() throws Exception {
	}

	
	@Test
	public void testCase1() {

		/**
		 * 1. WITHOUT POLLING LISTENER INTERVAL 
		 */
		
		//1. Engine Configuration
		Map<String, String> eCfg = new HashMap();
		eCfg.put(D2IOProcessor.EXTERNAL_THREAD_COUNT, "10");
		eCfg.put(D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD, "5");
		eCfg.put(D2IOProcessor.LISTENER, "com.suren.mbr.d2io.samples.SampleD2IOListener");
		//eCfg.put(D2IOProcessor.LISTENER_POLL_INTERVAL, "1");

		
		//2. Producer
		ProducerDatasource ds = new LoadSampleDatasource();

		//3. Producer Config Map 
		Producer p = new Producer();
		Map<String, String> inputMap = new HashMap();
		inputMap.put("rows", "100000");
		inputMap.put("fname", "TC1");
		inputMap.put("fcksum", "11");
		inputMap.put("fkey", "11-9999");
		p.setCfgMap(inputMap);
		
		//4. Consumer
		ConsumerCallback xCb = new ExternalCallBackImpl();

		//5. Consumer Config Map
		Consumer c = new Consumer();
		Map<String, String> xCbCfg = new HashMap();
		xCbCfg.put("delayInMills", "10");
		c.setCfgMap(xCbCfg);


		//6. D2IO Context 
		D2IOConfig d2ioCfg = new D2IOConfig();
		d2ioCfg.setConsumer(c);
		d2ioCfg.setProducer(p);
		d2ioCfg.setEngine(eCfg);
		
		D2IOContext ctx = new D2IOContext(ds, xCb, d2ioCfg);

		D2IOProcessor prcs = new BoundedD2IOProcessor();
		ResultSummary result;
		try {

			result = prcs.runProcess(ctx);
			log.info(result);
		
		} catch (Exception e) {
			log.error("Error",e);
			assertTrue(false);
		}

	}

	@Test
	public void testCase2() {

		/**
		 * 1. WITH POLLING LISTENER INTERVAL 
		 */
		
		//1. Engine Configuration
		Map<String, String> eCfg = new HashMap();
		eCfg.put(D2IOProcessor.EXTERNAL_THREAD_COUNT, "4");
		eCfg.put(D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD, "5");

		
		//2. Producer
		ProducerDatasource ds = new LoadSampleDatasource();

		//3. Producer Config Map 
		Producer p = new Producer();
		Map<String, String> inputMap = new HashMap();
		inputMap.put("rows", "1000");
		inputMap.put("fname", "TC2");
		inputMap.put("fcksum", "21");
		inputMap.put("fkey", "21-9999");
		p.setCfgMap(inputMap);
		
		//4. Consumer
		ConsumerCallback xCb = new ExternalCallBackImpl();

		//5. Consumer Config Map
		Consumer c = new Consumer();
		Map<String, String> xCbCfg = new HashMap();
		c.setCfgMap(xCbCfg);

		//6. D2IO Context 
		D2IOConfig d2ioCfg = new D2IOConfig();
		d2ioCfg.setConsumer(c);
		d2ioCfg.setProducer(p);
		d2ioCfg.setEngine(eCfg);
		
		D2IOContext ctx = new D2IOContext(ds, xCb, d2ioCfg);

		D2IOProcessor prcs = new BoundedD2IOProcessor();
		ResultSummary result;
		try {

			result = prcs.runProcess(ctx);
			log.info(result);
		
		} catch (Exception e) {
			log.error("Error",e);
			assertTrue(false);
		}

	}

	@Test
	public void testCase3() {

		/**
		 * 1. WITH POLLING LISTENER INTERVAL 
		 */
	
		//1. Engine Configuration
		Map<String, String> eCfg = new HashMap();
		eCfg.put(D2IOProcessor.EXTERNAL_THREAD_COUNT, "10");
		eCfg.put(D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD, "5");
		eCfg.put(D2IOProcessor.LISTENER, "com.suren.mbr.d2io.samples.SampleD2IOListener");
		eCfg.put(D2IOProcessor.LISTENER_POLL_INTERVAL, "5");

		
		//2. Producer
		ProducerDatasource ds = new LoadSampleDatasource();

		//3. Producer Config Map 
		Producer p = new Producer();
		Map<String, String> inputMap = new HashMap();
		inputMap.put("rows", "100");
		inputMap.put("fname", "TC3");
		inputMap.put("fcksum", "31");
		inputMap.put("fkey", "31-9999");
		p.setCfgMap(inputMap);
		
		//4. Consumer
		ConsumerCallback xCb = new ExternalCallBackImpl();

		//5. Consumer Config Map
		Consumer c = new Consumer();
		Map<String, String> xCbCfg = new HashMap();
		xCbCfg.put("delayInMills", "100");
		c.setCfgMap(xCbCfg);


		//6. D2IO Context 
		D2IOConfig d2ioCfg = new D2IOConfig();
		d2ioCfg.setConsumer(c);
		d2ioCfg.setProducer(p);
		d2ioCfg.setEngine(eCfg);
		
		D2IOContext ctx = new D2IOContext(ds, xCb, d2ioCfg);
		
		D2IOProcessor prcs = new BoundedD2IOProcessor();
		ResultSummary result;
		try {

			result = prcs.runProcess(ctx);
			log.info(result);
		
		} catch (Exception e) {
			log.error("Error",e);
			assertTrue(false);
		}

	}

	@Test
	public void testCase4() {

		/**
		 * 1. WITH POLLING LISTENER INTERVAL 
		 */

		//1. Engine Configuration
		Map<String, String> eCfg = new HashMap();
		
		//2. Producer
		ProducerDatasource ds = new LoadSampleDatasource();

		//3. Producer Config Map 
		Producer p = new Producer();
		Map<String, String> inputMap = new HashMap();
		inputMap.put("rows", "100000");
		inputMap.put("fname", "TC4");
		inputMap.put("fcksum", "41");
		inputMap.put("fkey", "41-9999");
		p.setCfgMap(inputMap);
		
		//4. Consumer
		ConsumerCallback xCb = new ExternalCallBackImpl();

		//5. Consumer Config Map
		Consumer c = new Consumer();
		Map<String, String> xCbCfg = new HashMap();
		c.setCfgMap(xCbCfg);


		//6. D2IO Context 
		D2IOConfig d2ioCfg = new D2IOConfig();
		d2ioCfg.setConsumer(c);
		d2ioCfg.setProducer(p);
		d2ioCfg.setEngine(eCfg);
		
		D2IOContext ctx = new D2IOContext(ds, xCb, d2ioCfg);		

		D2IOProcessor prcs = new BoundedD2IOProcessor();
		ResultSummary result;
		try {

			result = prcs.runProcess(ctx);
			log.info(result);
		
		} catch (Exception e) {
			log.error("Error",e);
			assertTrue(false);
		}

	}
	
	
	@Test
	public void testCase5() {

		/**
		 * WITHOUT A FILE DATASOURCE
		 * Generates a Key of its own 
		 */

		
		//1. Engine Configuration
		Map<String, String> eCfg = new HashMap();
		
		//2. Producer
		ProducerDatasource ds = new SampleNotAFileDatasource();

		//3. Producer Config Map 
		Producer p = new Producer();
		Map<String, String> inputMap = new HashMap();
		inputMap.put("rows", "100000");
		p.setCfgMap(inputMap);
		
		//4. Consumer
		ConsumerCallback xCb = new ExternalCallBackImpl();

		//5. Consumer Config Map
		Consumer c = new Consumer();
		Map<String, String> xCbCfg = new HashMap();
		c.setCfgMap(xCbCfg);


		//6. D2IO Context 
		D2IOConfig d2ioCfg = new D2IOConfig();
		d2ioCfg.setConsumer(c);
		d2ioCfg.setProducer(p);
		d2ioCfg.setEngine(eCfg);
		
		D2IOContext ctx = new D2IOContext(ds, xCb, d2ioCfg);
		
		D2IOProcessor prcs = new BoundedD2IOProcessor();
		ResultSummary result;
		try {

			result = prcs.runProcess(ctx);
			log.info(result);
		
		} catch (Exception e) {
			log.error("Error",e);
			assertTrue(false);
		}

	}
}
