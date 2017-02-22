/**
 * 
 */
package com.suren.mbr.d2io.samples;

import java.util.Map;

import com.suren.mbr.d2io.intf.FileInputDatasource;
import com.suren.mbr.d2io.intf.RecordData;
import com.suren.mbr.d2io.intf.impl.InputRecordData;

/**
 * @author Surendra Myneni
 *
 */
public class LoadSampleDatasource implements FileInputDatasource {

	private int record = 0;
	private int limit = 5000;
	private String fname = null;
	private String fcksum = null;
	private String fkey = null;
	
	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.InputDatasource#init(java.util.Map)
	 */
	public void init(Map<String, String> configMap) throws Exception {
		if(configMap.get("rows")!= null){
			limit = new Integer(configMap.get("rows")).intValue();
		}
	
		this.fname = configMap.get("fname");
		this.fcksum = configMap.get("fcksum");
		this.fkey = configMap.get("fkey");
		
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.InputDatasource#hasNext()
	 */
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(record < limit) return true;
		return false;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.InputDatasource#next()
	 */
	public RecordData next() {
		// TODO Auto-generated method stub
		if (record < limit){
			record++;
			return new InputRecordData(new Integer(record), new Integer(record).toString()); 
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.InputDatasource#close()
	 */
	public void close() {

	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.FileInputDatasource#getFileName()
	 */
	public String getFileName() {
		return this.fname;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.FileInputDatasource#getFileCheckSum()
	 */
	public String getFileCheckSum() {
		// TODO Auto-generated method stub
		return this.fcksum;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.FileInputDatasource#getUniqueFileKey()
	 */
	public String getUniqueFileKey() {
		// TODO Auto-generated method stub
		return this.fkey;
	}

}
