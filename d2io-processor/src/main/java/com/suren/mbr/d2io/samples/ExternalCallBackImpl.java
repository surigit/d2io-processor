/**
 * 
 */
package com.suren.mbr.d2io.samples;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.suren.mbr.d2io.intf.CallbackStatus;
import com.suren.mbr.d2io.intf.ConsumerCallback;
import com.suren.mbr.d2io.intf.RecordData;

/**
 * @author Surendra Myneni
 *
 */
public class ExternalCallBackImpl implements ConsumerCallback {
	private static Log log = LogFactory.getLog(ExternalCallBackImpl.class.getName());

	public CallbackStatus makeExternalCall(RecordData sourceData, Map<String, String> configMap) throws Exception {
		//log.debug("makeExternalCall()|1|Begin...");
		
		long delayInMillis = 0;
		try {
			delayInMillis = new Long(configMap.get("delayInMillis"));
		} catch (Exception e) {
			delayInMillis = 0;
		}
		
		int key = new Integer(sourceData.getRecordKey()).intValue();
//		if(key > 20 && key < 29){
//			throw new Exception ("Intentionally Thrown");
//		}
		
		if(delayInMillis > 0){
			Thread.sleep(delayInMillis);
		}
		
		//log.info(sourceData);
		//log.debug("makeExternalCall()|9|End...");
		return null;
	}

}
