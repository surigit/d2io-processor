/**
 * 
 */
package com.suren.mbr.d2io.intf;

/**
 * @author Surendra Myneni
 *
 */
public interface FileInputDatasource extends ProducerDatasource {

	public String getFileName();

	public String getFileCheckSum();

	public String getUniqueFileKey();
	
}
