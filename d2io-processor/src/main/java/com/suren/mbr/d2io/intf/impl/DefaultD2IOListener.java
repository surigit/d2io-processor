/**
 * 
 */
package com.suren.mbr.d2io.intf.impl;

import com.suren.mbr.d2io.intf.D2IOListener;

/**
 * @author Surendra Myneni
 *
 */
public class DefaultD2IOListener implements D2IOListener {

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onStart(java.lang.String)
	 */
	@Override
	public void onStart(String mesg) {
		System.out.println(mesg);
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onStop(java.lang.String)
	 */
	@Override
	public void onStop(String mesg) {
		System.out.println(mesg);
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onError(java.lang.String)
	 */
	@Override
	public void onError(String mesg) {
		System.err.println(mesg);
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onWarn(java.lang.String)
	 */
	@Override
	public void onWarn(String mesg) {
		System.out.println(mesg);
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onIntervalStatus(java.lang.String)
	 */
	@Override
	public void onIntervalStatus(String mesg) {
		System.out.println(mesg);
	}

}
