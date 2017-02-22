package com.suren.mbr.d2io.intf;

import com.suren.mbr.d2io.intf.impl.D2IOContext;

public interface D2IOProcessor {

	public static String EXTERNAL_THREAD_COUNT="threads"; 
	public static String MAX_QUE_STG_COUNT_TIMES_THREAD="bufferLimit"; 
	public static String LISTENER="listener"; 
	public static String LISTENER_POLL_INTERVAL="listenerPollInterval"; 
	
	public ResultSummary runProcess(D2IOContext d2ioCtx) throws Exception;
	
}
