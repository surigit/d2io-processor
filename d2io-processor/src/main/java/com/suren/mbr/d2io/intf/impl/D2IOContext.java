/**
 * 
 */
package com.suren.mbr.d2io.intf.impl;

import java.util.Map;
import java.util.Properties;

import com.suren.mbr.d2io.config.D2IOConfig;
import com.suren.mbr.d2io.intf.ConsumerCallback;
import com.suren.mbr.d2io.intf.ProducerDatasource;

/**
 * @author Surendra Myneni
 *
 */
public class D2IOContext {

	private ProducerDatasource inputDS;
	private ConsumerCallback outputCB;
	private D2IOConfig d2ioCfg;

	
	public D2IOContext(ProducerDatasource inputDS, ConsumerCallback outputCB,D2IOConfig d2ioCfg) {
		super();
		this.inputDS = inputDS;
		this.outputCB = outputCB;
		this.d2ioCfg = d2ioCfg;
	}
	public ProducerDatasource getInputDS() {
		return inputDS;
	}
	public ConsumerCallback getOutputCB() {
		return outputCB;
	}
	public D2IOConfig getD2ioCfg() {
		return d2ioCfg;
	}
	public void setD2ioCfg(D2IOConfig d2ioCfg) {
		this.d2ioCfg = d2ioCfg;
	}
	
}
