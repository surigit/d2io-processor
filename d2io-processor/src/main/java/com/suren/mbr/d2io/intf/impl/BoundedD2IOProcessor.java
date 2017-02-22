/**
 * 
 */
package com.suren.mbr.d2io.intf.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.suren.mbr.d2io.events.D2IOEvent;
import com.suren.mbr.d2io.intf.CallbackStatus;
import com.suren.mbr.d2io.intf.D2IOListener;
import com.suren.mbr.d2io.intf.D2IOListener.D2IO_EVENT;
import com.suren.mbr.d2io.intf.D2IOProcessor;
import com.suren.mbr.d2io.intf.FileInputDatasource;
import com.suren.mbr.d2io.intf.ProducerDatasource;
import com.suren.mbr.d2io.intf.RecordData;
import com.suren.mbr.d2io.intf.ResultSummary;
import com.suren.mbr.d2io.utils.D2IOUtils;

/**
 * @author Surendra Myneni
 *
 */
public class BoundedD2IOProcessor implements D2IOProcessor {

	private static Log log = LogFactory.getLog(BoundedD2IOProcessor.class.getName());

	private Map<String,String> engineMap;
	private ArrayBlockingQueue<RecordData> prcsQue = null;
	private ArrayBlockingQueue<D2IOEvent> listQue = null;
	private int LEAD_TIME_ONDONE=1000;
	private ExecutorService service = null;
	private CountDownLatch doneLatch = null;
	private ResultSummary summary = null;
	private List<Callable<String>> allCalls = null;
	private List<Thread> outThreads = null;
	private Lock lock = null;
	private String eventKey="";
	private Thread listenerThread = null;
	private boolean hasListener = false;
	private Timer timer = null;
	private boolean producerStarted = false;
	private int listenerInterval = 0;
	private D2IOListener listener=null;
	private ExecutorService listServ = null;
	
	// Producer Signal 
	private volatile int PRODUCER_DONE =0;

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.D2IOProcessor#runProcess(com.suren.mbr.d2io.intf.impl.D2IOContext)
	 */
	@Override
	public ResultSummary runProcess(D2IOContext d2ioCtx) throws Exception {
		long sT = System.currentTimeMillis();
		log.debug("runProcess()|1|Begin");
		
		//1. Initialize
		try {
			init(d2ioCtx);
		} catch (Exception e) {
			log.error("runProcess()|2|Failed in Initialization",e);
			throw new Exception("runProcess()|2|Failed in Initialization",e);
		}

		
		//2. Start the Listener here 
		if(hasListener && listener!= null){
			listServ = Executors.newFixedThreadPool(1);
			Future<String> listen = listServ.submit(new ListenerCallable(listener));
			listServ.shutdown();
		}
		
		//2. Call 
		service.invokeAll(allCalls);
		service.shutdown();
		
		//3. Await Until all Threads are complete
		doneLatch.await();

		String tmpKey = eventKey;

		//4. clean up 
		cleanUp();

		log.info("runProcess()|3|"+summary);
		log.info("runProcess()|4|eventKey="+tmpKey+"|Latency ["+((System.currentTimeMillis()-sT)/1000)+"] seconds|End");

		return summary;
	}

	/**
	 * CLEAN UP 
	 */
	private void cleanUp() throws Exception{
		
		if(hasListener){
			if(timer != null){
				log.info("cleanUp|1|Listener Timer Canceled");
				timer.cancel();
				timer = null;
			}
			if(listenerInterval > 0){
				try {
					listQue.put(new D2IOEvent(D2IO_EVENT.STATUS,eventKey+"|"+summary));
				} catch (InterruptedException e) {
				}
			}
			listQue.put(new D2IOEvent(D2IO_EVENT.STOP,eventKey+"|"+D2IO_EVENT.STOP));
			Thread.sleep(LEAD_TIME_ONDONE);
			listenerThread.interrupt();
			listServ.shutdownNow();
			log.info("cleanUp|2|Listener Thread Interrupt complete");
		}
		
		service.shutdownNow();
		
		engineMap = null;
		prcsQue = null;
		listQue = null;
		service = null;
		doneLatch = null;
		allCalls = null;
		outThreads = null;
		lock = null;
		eventKey=null;
		listenerThread = null;
		hasListener = false;
		timer = null;
		producerStarted = false;
		listenerInterval = 0;
		listener = null;
		PRODUCER_DONE=0;
		listServ = null;
	}
	
	
	/**
	 * Prepare for Bounded Process
	 */
	private void init(D2IOContext ctx) throws Exception{
		
		log.debug("init()|1| start...");
		int cores = Runtime.getRuntime().availableProcessors();
		log.info("init()|2| Available Cores ["+cores+"]");;
		int tCount = cores;
		engineMap = ctx.getD2ioCfg().getEngine();

		//1. Figure out the Num of Threads
		if(engineMap.get(D2IOProcessor.EXTERNAL_THREAD_COUNT)!=null){
			try {
				tCount = new Integer(engineMap.get(D2IOProcessor.EXTERNAL_THREAD_COUNT)).intValue();
				if(tCount > cores ){
					log.warn("init()|3| Available Cores ["+cores+"]. Thread Count Requested ["+tCount+"]. Having Threads more than Avail Cores may eat up CPU and may have no value and also slow down.");
				}
				if(tCount < 0){
					tCount = cores;
					log.error("init()|4| Invalid Thread Count ["+tCount+"]. Defaulted to available cores");
				}
			} catch (NumberFormatException e) {
				log.error("init()|5|D2IOProcessor.EXTERNAL_THREAD_COUNT was ["+engineMap.get(D2IOProcessor.EXTERNAL_THREAD_COUNT)+"]. Unable to convert to int",e);
				log.warn("init()|6|Thread Count default to available Cores ["+cores+"]");
			}
		}else{
			log.info("init()|7| Thread Count defaulted to Available Cores ["+cores+"]");;
		}

		//2. Bounded Buffer Limit 
		int stag_limit =  tCount * 5; // default buffer
		
		if(engineMap.get(D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD)!=null){
			try {
				stag_limit = new Integer(engineMap.get(D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD)).intValue()*tCount;
				if(stag_limit < 0){
					log.warn("init()|8|Applying Default Bounded Buffer Limit ["+stag_limit+"]");
					stag_limit = tCount * 5;
				}
			
			} catch (NumberFormatException e) {
				log.error("init()|9|D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD was ["+engineMap.get(D2IOProcessor.MAX_QUE_STG_COUNT_TIMES_THREAD)+"]. Unable to convert to int",e);
				log.warn("init()|10|Applying Default Bounded Buffer Limit ["+stag_limit+"]");
				stag_limit = tCount * 5;
			}
		}else{
			log.info("init()|11| default Bounded Buffer Limit  ["+stag_limit+"]");;
		}

		log.info("init()|12| Final Config|Thread Count ["+tCount+"]; BB Limit ["+stag_limit+"]");

		try {
			listener = D2IOUtils.getListener(engineMap.get(D2IOProcessor.LISTENER));
		} catch (Exception e) {
			log.error("init()|13|Bad Listener Supplied. Listener is ["+engineMap.get(D2IOProcessor.LISTENER)+"]. Listener MUST be instance of D2IOListener Interface",e);
			log.error("init()|14|Leave the listener property blank for Default Listener");
			throw new Exception("init()|11|Bad Listener Supplied. Listener is ["+engineMap.get(D2IOProcessor.LISTENER)+"]. Listener MUST be instance of D2IOListener Interface",e);
		}

		
		//3. Set the Listener here 
		if(listener != null){
			listQue = new ArrayBlockingQueue<D2IOEvent>(20,true);
			hasListener = true;
			
			// get the refresh interval of listener 
			try {
				listenerInterval = new Integer(engineMap.get(D2IOProcessor.LISTENER_POLL_INTERVAL)).intValue();
			} catch (Exception e) {
				log.error("init()|15| Listener Interval Failed to parse value ["+engineMap.get(D2IOProcessor.LISTENER_POLL_INTERVAL)+"]. No Intermediate Status shall be reported via Listerner.");
				listenerInterval = 0;
			}
		}
		
		//4.Set the Bounded Buffer limit
		prcsQue = new ArrayBlockingQueue<RecordData>(stag_limit, true);
		log.debug("init()|16| Bounded Buffer Set");

		//5. Prep the pool count

		service = Executors.newFixedThreadPool(tCount+1);
		log.debug("init()|17| Thread Pool Set : POOL Count ["+(tCount+1)+"]");

		//6. Prep the latch 
		doneLatch = new CountDownLatch(tCount+1); 
		log.debug("init()|18| Latches set : LATCH COUNT ["+(tCount+1)+"]");
		summary = new ResultsSummaryImpl();


		allCalls = new ArrayList<Callable<String>>(tCount);
		allCalls.add(new InputCallable(doneLatch, ctx,summary));
		
		for (int i=1;i<=tCount;i++){
			allCalls.add(new ExternalCBCallable(doneLatch, ctx,summary));
		}
		log.debug("init()|19| Callables Prepared. Calls SIZE ["+allCalls.size()+"]");
		
		//7. prep callable threads 
		outThreads = new ArrayList<Thread>(tCount);
		log.debug("init()|20| Callable Thread for Interrupt prepped");
		
		//8. renentant lock 
		lock = new ReentrantLock(true);
		
		//9. Set the Timer 
		if(hasListener){
			timer = new Timer();
			log.debug("init()|21| Listener TIMER Created");
		}
		
		log.debug("init()|22| End...");
		
	}
	
	/**
	 * INPUT DATASOURCE CALLABLE
	 * @author Surendra Myneni
	 *
	 */
	private class InputCallable implements Callable<String>{

		private CountDownLatch doneLatch;
		private D2IOContext ctx;
		private ResultSummary summary;
		private long tid =0;
		
		public InputCallable(CountDownLatch doneLatch, D2IOContext ctx, ResultSummary summary) {
			super();
			this.doneLatch = doneLatch;
			this.ctx = ctx;
			this.summary = summary;
		}

		public String call() throws Exception {

			log.debug("InputCallable.call|1|Begin");
			tid = Thread.currentThread().getId();
			
			if(hasListener && listenerInterval > 0){
				log.debug("InputCallable.call|2|Timser Set for Secs ["+listenerInterval+"]");
				timer.schedule(new StatusTimer(), 0, listenerInterval * 1000);
			}
			
			//1. Read and Pump into the Bounded Buffer 
			ProducerDatasource iDS = ctx.getInputDS();
			
			try {
				iDS.init(ctx.getD2ioCfg().getProducer().getCfgMap());
				log.debug("InputCallable.call|3|InputDS init invoked");

				eventKey = D2IOUtils.getPrimaryKey();
				if(iDS instanceof FileInputDatasource){
					FileInputDatasource fDS = (FileInputDatasource) iDS;
					eventKey = D2IOUtils.getFileKey(fDS.getFileName(), fDS.getFileCheckSum(), fDS.getUniqueFileKey());
				}
				
				producerStarted = true;
				log.debug("InputCallable.call|4|eventKey="+eventKey);
				if(hasListener)listQue.put(new D2IOEvent(D2IO_EVENT.START,eventKey+"|"+D2IO_EVENT.START));
				
				while(iDS.hasNext()){
					RecordData rData = iDS.next();
					if(rData == null){
						log.error("InputCallable.call|5|InputDS hasNext has returned NULL. Throwing Excepton. Terminating");
						throw new Exception("InputCallable.call|5|InputDS hasNext has returned NULL.");
					}
					
					// ADD timeout functionality here - if warranted - so as not to block indefinitely.
					
					prcsQue.put(rData);;
					summary.addInputCount();
				}

			} catch (Exception e) {
				log.error("InputCallable.call|7|Encountered Exception."+e.getLocalizedMessage(),e);
			}finally{
				
				//2. Always Call Close on the InputDatasource 
				try {
					log.debug("InputCallable.call|8|Invoking inputDS close");
					iDS.close();
					log.debug("InputCallable.call|9|inputDS closed");
					
				} catch (Exception e) {
					log.error("InputCallable.call|10|inputDS close() encountered exception",e);
				}
			}

			log.debug("InputCallable.call|11|Sleeping for Lead Time");
			Thread.sleep(LEAD_TIME_ONDONE);
			log.debug("InputCallable.call|12|Awoke from Lead Time");

			PRODUCER_DONE = 1;
			log.debug("InputCallable.call|13|PRODUCER DONE SIGNAL ISSUED");
			
			//3. decrement the latch 
			doneLatch.countDown();
			log.debug("InputCallable.call|18|Latch Decremented|End");
			
			return "0";
		}

	}
	
	/**
	 * EXTERNAL CALLBACK CALLABLE
	 * @author Surendra Myneni
	 *
	 */
	private class ExternalCBCallable implements Callable<String>{

		private CountDownLatch doneLatch;
		private D2IOContext ctx;
		private ResultSummary summary;
		private long tid =0;
		
		public ExternalCBCallable(CountDownLatch doneLatch, D2IOContext ctx, ResultSummary summary) {
			super();
			this.doneLatch = doneLatch;
			this.ctx = ctx;
			this.summary = summary;
		}


		public String call() throws Exception {

			tid = Thread.currentThread().getId();
			log.debug("ExternalCBCallable.call|1|Begin| TID ["+tid+"]");
			
			// set the Thread into the call Threads list

			//1. Poll the Bounded Buffer 
			while(1==1){
				
				RecordData rData=null;

				rData = prcsQue.poll();
				
				//2. Make that External Call here 

				if(null!= rData){
					try {
						summary.addSubmitCount();
						log.debug("ExternalCBCallable.call|6|TID ["+tid+"]| Invoking Xternal Call");
						CallbackStatus callStatus = ctx.getOutputCB().makeExternalCall(rData, ctx.getD2ioCfg().getConsumer().getCfgMap());
						log.debug("ExternalCBCallable.call|7|TID ["+tid+"]| Finished Xternal Call Status ["+callStatus+"]");
						summary.addResponseCount();
					} catch (Exception e) {
						listQue.put(new D2IOEvent(D2IO_EVENT.ERROR,eventKey+"|"+rData.getRecordKey()+"|"+e.getLocalizedMessage()));
						summary.addExpCount();
						log.error("ExternalCBCallable.call|8|TID ["+tid+"]|Exception in xternal Call. Moving ON",e);
					}				

				}
				
				//3. CHECK PRODUCER SIGNAL 
				if(PRODUCER_DONE > 0 && prcsQue.isEmpty()){
					doneLatch.countDown();
					log.info("ExternalCBCallable.call|5|TID ["+tid+"] Latch Decremented Curr Count="+doneLatch.getCount());
					rData = null;
					return "0";
				}
				
				//4. NULL the rData here - giving GC a hint to claim it 
				rData = null;
			}		
			
		}
		
	}	
	
	/**
	 * 
	 * @author Surendra Myneni
	 *
	 */
	private class ListenerCallable implements Callable<String>{

		private D2IOListener listener;
		private long tid =0;
		
		public ListenerCallable(D2IOListener listener) {
			super();
			this.listener = listener;
		}

		public String call() throws Exception {

			tid = Thread.currentThread().getId();
			// set the Thread into the call Threads list
			listenerThread = Thread.currentThread();
			//1. Poll the Bounded Buffer 
			while(1==1){

				D2IOEvent event=null;
				try {
					event = listQue.take();
					//System.out.println(event);
				} catch (InterruptedException e1) {
					log.info("ListenerCallable.call|1|TID ["+tid+"]| Thread Interrupted");
					return "0";
				}
				
				if(D2IOListener.D2IO_EVENT.START.equals(event.getType())){
					listener.onStart(event.getMesg());
				}
				if(D2IOListener.D2IO_EVENT.STOP.equals(event.getType())){
					listener.onStop(event.getMesg());
				}
				if(D2IOListener.D2IO_EVENT.ERROR.equals(event.getType())){
					listener.onError(event.getMesg());
				}
				if(D2IOListener.D2IO_EVENT.WARN.equals(event.getType())){
					listener.onWarn(event.getMesg());
				}
				if(D2IOListener.D2IO_EVENT.STATUS.equals(event.getType())){
					listener.onIntervalStatus(event.getMesg());
				}

			}		
			
		}
		
	}
	
	/**
	 * 
	 * @author Surendra Myneni
	 *
	 */
	private class StatusTimer extends TimerTask{

		@Override
		public void run() {

			if(hasListener && producerStarted){
				try {
					listQue.put(new D2IOEvent(D2IO_EVENT.STATUS, eventKey+"|"+summary));
				} catch (InterruptedException e) {
				}
			}
		}

		@Override
		public boolean cancel() {
			// TODO Auto-generated method stub
			return super.cancel();
		}

		@Override
		public long scheduledExecutionTime() {
			// TODO Auto-generated method stub
			return super.scheduledExecutionTime();
		}
		
	}
}
