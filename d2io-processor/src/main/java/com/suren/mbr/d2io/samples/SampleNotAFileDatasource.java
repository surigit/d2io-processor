/**
 * 
 */
package com.suren.mbr.d2io.samples;

import java.util.Map;

import com.suren.mbr.d2io.intf.ProducerDatasource;
import com.suren.mbr.d2io.intf.RecordData;
import com.suren.mbr.d2io.intf.impl.InputRecordData;

/**
 * @author Surendra Myneni
 *
 */
public class SampleNotAFileDatasource implements ProducerDatasource {

	
	private int limit = 5000;
	private int record = 0;

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#init(java.util.Map)
	 */
	@Override
	public void init(Map<String, String> configMap) throws Exception {
		if(configMap.get("rows")!= null){
			limit = new Integer(configMap.get("rows")).intValue();
		}
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#hasNext()
	 */
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(record < limit) return true;
		return false;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#next()
	 */
	@Override
	public RecordData next() {
		// TODO Auto-generated method stub
		if (record < limit){
			record++;
			return new InputRecordData(new Integer(record), new Integer(record).toString()); 
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#close()
	 */
	@Override
	public void close() {

	}

}
