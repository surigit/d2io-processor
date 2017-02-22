package com.suren.mbr.d2io.intf.impl;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.suren.mbr.d2io.intf.ResultSummary;

public class ResultsSummaryImpl implements ResultSummary {

	private AtomicLong inputCount = new AtomicLong(0);
	private AtomicLong submitCount = new AtomicLong(0);;
	private AtomicLong responseCount = new AtomicLong(0);;
	private AtomicLong expCount = new AtomicLong(0);;
	
	public String getJobName() {
		return null;
	}
	public Long getInputCount() {
		// TODO Auto-generated method stub
		return inputCount.longValue();
	}

	public void addInputCount() {
		inputCount.incrementAndGet();
	}

	public Long getSubmitCount() {
		// TODO Auto-generated method stub
		return submitCount.longValue();
	}

	public void addSubmitCount() {
		submitCount.incrementAndGet();
	}

	public Long getResponseCount() {
		// TODO Auto-generated method stub
		return responseCount.longValue();
	}

	public void addResponseCount() {
		responseCount.incrementAndGet();
	}

	@Override
	public Long getExpCount() {
		return expCount.longValue();
	}
	@Override
	public void addExpCount() {

		expCount.incrementAndGet();
	}
	public int getStatus() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public String toString() {
		return "ResultsSummaryImpl [inputCount=" + inputCount + ", submitCount=" + submitCount + ", responseCount="
				+ responseCount + ", expCount=" + expCount + "]";
	}

}
