/**
 * 
 */
package com.suren.mbr.d2io.intf;

/**
 * @author Surendra Myneni
 *
 */
public interface D2IOListener {

	public static enum D2IO_EVENT{
		START,STOP,ERROR,WARN,STATUS;
	}
	
	public void onStart(String mesg);
	public void onStop(String mesg);
	public void onError(String mesg);
	public void onWarn(String mesg);
	public void onIntervalStatus(String mesg);
	
}
