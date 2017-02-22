/**
 * 
 */
package com.suren.mbr.d2io.samples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.suren.mbr.d2io.intf.D2IOListener;
import com.suren.mbr.d2io.intf.impl.BoundedD2IOProcessor;

/**
 * @author Surendra Myneni
 *
 */
public class SampleD2IOListener implements D2IOListener {
	private static Log log = LogFactory.getLog(SampleD2IOListener.class.getName());

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
		// TODO Auto-generated method stub
		System.out.println(mesg);

	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onError(java.lang.String)
	 */
	@Override
	public void onError(String mesg) {
		// TODO Auto-generated method stub
		System.out.println(mesg);

	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOListener#onIntervalStatus(java.lang.String)
	 */
	@Override
	public void onIntervalStatus(String mesg) {
		// TODO Auto-generated method stub
		System.out.println(mesg);

	}

	@Override
	public void onWarn(String mesg) {
		System.out.println(mesg);
	}

}
