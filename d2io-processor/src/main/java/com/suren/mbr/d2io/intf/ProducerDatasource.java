package com.suren.mbr.d2io.intf;

import java.util.Map;

public interface ProducerDatasource {

	/**
	 * The Init is called by the framework- if end implementation wants to initialize and prepare prior to read
	 * @throws Exception
	 */
	public void init(Map<String, String> configMap) throws Exception;
	
	public boolean hasNext() ;

	public RecordData next() ;

	public void close() ;

}
