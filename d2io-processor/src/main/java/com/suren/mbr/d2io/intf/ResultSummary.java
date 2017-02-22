package com.suren.mbr.d2io.intf;

import java.util.concurrent.atomic.AtomicLong;

public interface ResultSummary {

	public String getJobName();
	public Long getInputCount(); 
	public void addInputCount(); 
	
	public Long getSubmitCount(); 
	public void addSubmitCount(); 

	public Long getResponseCount(); 
	public void addResponseCount(); 
	
	public Long getExpCount(); 
	public void addExpCount(); 

	public int getStatus(); 
	
}
