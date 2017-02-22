package com.suren.mbr.d2io.intf.impl;

import com.suren.mbr.d2io.intf.RecordData;

public class InputRecordData implements RecordData {

	private Object rData = null;
	private String rKey = null;
	
	public InputRecordData(Object rData, String rKey) {
		super();
		this.rData = rData;
		this.rKey = rKey;
	}

	public String getRecordKey() {
		return this.rKey;
	}

	public Object getRecord() {
		return this.rData;
	}

	@Override
	public String toString() {
		return "InputRecordData [rData=" + rData + ", rKey=" + rKey + "]";
	}

}
