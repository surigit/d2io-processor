/**
 * 
 */
package com.suren.mbr.d2io.intf.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.suren.mbr.d2io.intf.CallbackStatus;

/**
 * @author Surendra Myneni
 *
 */
public class CallbackStatusImpl implements CallbackStatus {

	private static Log log = LogFactory.getLog(CallbackStatusImpl.class.getName());

	private Map<String, Object> callResults;
	
	public CallbackStatusImpl(Map<String, Object> callResults) {
		super();
		this.callResults = callResults;
	}

	public Map<String, Object> getCallResults() {
		
		log.debug("getCallResults()|1|Begin..");
		log.debug("getCallResults()|9|End..");
		
		return this.callResults;
	}

}
