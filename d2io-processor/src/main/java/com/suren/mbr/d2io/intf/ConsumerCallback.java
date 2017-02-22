package com.suren.mbr.d2io.intf;

import java.util.Map;

public interface ConsumerCallback {

	public CallbackStatus makeExternalCall(RecordData sourceData, Map<String, String> configMap) throws Exception;
	
}
