/**
 * 
 */
package com.suren.mbr.d2io.events;

import com.suren.mbr.d2io.intf.D2IOListener.D2IO_EVENT;

/**
 * @author Surendra Myneni
 *
 */
public class D2IOEvent {

	private D2IO_EVENT type;
	private String mesg;
	public D2IO_EVENT getType() {
		return type;
	}
	public String getMesg() {
		return mesg;
	}
	public D2IOEvent(D2IO_EVENT type, String mesg) {
		super();
		this.type = type;
		this.mesg = mesg;
	}
	@Override
	public String toString() {
		return "D2IOEvent [type=" + type + ", mesg=" + mesg + "]";
	}
}
