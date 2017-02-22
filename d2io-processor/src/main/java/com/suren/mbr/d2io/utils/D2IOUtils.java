package com.suren.mbr.d2io.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

import com.suren.mbr.d2io.config.D2IOConfig;
import com.suren.mbr.d2io.intf.ConsumerCallback;
import com.suren.mbr.d2io.intf.D2IOListener;
import com.suren.mbr.d2io.intf.ProducerDatasource;
import com.suren.mbr.d2io.intf.impl.DefaultD2IOListener;
import com.suren.mbr.d2io.samples.SampleD2IOListener;

public class D2IOUtils {

	protected static Log log = LogFactory.getLog(D2IOUtils.class.getName());
	private static AtomicInteger uuid = new AtomicInteger(0);
	private static AtomicInteger primkey = new AtomicInteger(0);
	private static ReentrantLock lock = new ReentrantLock(true);

	
	public static String logBaseMesg(String uri){
		Long tid = Thread.currentThread().getId();
		return MessageFormat.format("TID [{0}], Uri [{1}]", new String[]{tid.toString(),uri});
	}

	public static String logBaseMesg(String uri, long sTime){
		Long tid = Thread.currentThread().getId();
		Long fT = System.currentTimeMillis() - sTime;
		return MessageFormat.format("TID [{0}], Uri [{1}], Latency ms [{2}]", new String[]{tid.toString(),uri,fT.toString()});
	}

	public static String getCurrentTime(){
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/Chicago"));
		return df.format(cal.getTime());
		
	}

	public static String getCurrentTime(String pattern){
		DateFormat df = new SimpleDateFormat(pattern);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/Chicago"));
		return df.format(cal.getTime());
		
	}

	
	public static String getUUID(){
		
		String fTime = null;
		try {
			lock.lock();
			String currTime = D2IOUtils.getCurrentTime("yyyyMMddHHmmssSSS");
			fTime = currTime+"_"+uuid.incrementAndGet();
		} finally{
			lock.unlock();
		}
		
		return fTime;
	}

	public static String getPrimaryKey(){
		
		String fTime = null;
		try {
			lock.lock();
			String currTime = D2IOUtils.getCurrentTime("yyyyMMddHHmmssSSS");
			Thread.sleep(1);
			fTime = currTime+primkey.incrementAndGet();
		} catch(Exception e){
			
		}finally{
			lock.unlock();
		}
		
		return fTime;
	}
	
	public static String getFileKey(String fileName, String checkSum, String uniqFileKey){
		
		if(fileName == null && null== checkSum && null==uniqFileKey){
			return getPrimaryKey();
		}
		
		if("".equals(fileName) || "".equals(checkSum) && "".equals(uniqFileKey)){
			return getPrimaryKey();
		}
		return "fileName="+fileName+";checkSum="+checkSum+";fileKey="+uniqFileKey;
	}
	
	
	/**
	 * 
	 * @param listClass
	 * @return
	 * @throws Exception
	 */
	public static D2IOListener getListener(String listClass) throws Exception{
		
		if(listClass == null){
			log.warn("getListener()|1|No Listerner Supplied. Attaching Default Listener");
			return  new DefaultD2IOListener();
		}
		try {
			Class<?> clazz = Class.forName(listClass);
			return  (D2IOListener)clazz.newInstance();
		} catch (ClassNotFoundException e) {
			log.error("ClassNotFoundException:Class["+listClass+"]. Cannot Continue.",e);
			throw new RuntimeException("ClassNotFoundException:Class["+listClass+"]. Cannot Continue.",e);
		} catch (InstantiationException e) {
			log.error("InstantiationException["+listClass+"]. Cannot Continue.",e);
			throw new RuntimeException("InstantiationException:Class["+listClass+"]. Cannot Continue.",e);
		} catch (IllegalAccessException e) {
			log.error("IllegalAccessException["+listClass+"]. Cannot Continue.",e);
			throw new RuntimeException("IllegalAccessException:Class["+listClass+"]. Cannot Continue.",e);
		} catch (ClassCastException e) {
			log.error("ClassCastException["+listClass+"]. Cannot Continue.",e);
			throw new RuntimeException("ClassCastException:Class["+listClass+"]. Cannot Continue.",e);
		} catch (Exception e) {
			log.error("Exception["+listClass+"]. Cannot Continue.",e);
			throw new RuntimeException("Exception:Class["+listClass+"]. Cannot Continue.",e);
		}
	}
	
	/**
	 * 
	 * @param cfgFile
	 * @return
	 * @throws Exception
	 */
	public static D2IOConfig loadConfiguration(String cfgFile) throws Exception{
		
		D2IOConfig d2ioCfg = null;
        Yaml yaml = new Yaml();  
		InputStream in=null;
		File file = new File(cfgFile);
		try {
			try {
				
				if(!file.exists()){
					log.error("loadConfiguration()|1| File Does not Exist. ["+cfgFile+"]");
					throw new Exception ("loadConfiguration()|1| File Does not Exist. ["+cfgFile+"]");
				}
				
				if(!file.canRead()){
					log.error("loadConfiguration()|2| File Cannot Be Read. Check Permissions. ["+cfgFile+"]");
					throw new Exception ("loadConfiguration()|2| File Cannot Be Read. Check Permissions. ["+cfgFile+"]");
				}
				in = new FileInputStream(file);
			} catch (Exception e) {
				log.error("loadConfiguration()|3|Unable to read the Config File ["+cfgFile+"]. Cannot continue.",e);
				throw new Exception ("loadConfiguration()|3|Unable to read the Config File ["+cfgFile+"]. Cannot continue.",e);
			}
			log.debug("loadConfiguration()|4| Parsing config file");
			d2ioCfg = yaml.loadAs(in, D2IOConfig.class );
			log.info("loadConfiguration()|5| Parsed Config="+d2ioCfg.toString());
		} catch (Exception e) {
			log.error("loadConfiguration()|6| Unable to Parse the YML config File ["+cfgFile+"]. Cannot Continue."+e.getLocalizedMessage(),e);
			throw new Exception ("loadConfiguration()|6| Unable to Parse the YML config File ["+cfgFile+"]. Cannot Continue."+e.getLocalizedMessage(),e);
		}finally{
			// close the input stream 
			if (null!= in){
				in.close();
				in = null;
			}
			file= null;
			log.debug("loadConfiguration()|7| Closed File InputStream");
		}
		
		return d2ioCfg;
	}
	
	
	/**
	 * 
	 * @param d2ioCfg
	 * @return
	 */
	public static void runValidation(D2IOConfig d2ioCfg) throws Exception{
		List<String> errors = new ArrayList();
		
		if(null==d2ioCfg.getProducer()){
			errors.add("runValidation()|8| Producer Config cannot be NULL.");
		}

		if(null==d2ioCfg.getConsumer()){
			errors.add("runValidation()|8| Consumer Config cannot be NULL.");
		}
		
		if(!errors.isEmpty()){
			Iterator<String> itr = errors.iterator();
			while (itr.hasNext()){
				log.error("runValidation()|9| "+itr.next());
			}
			log.error("runValidation()|10| Please fix all the above listed errors. Cannot continue");
			throw new Exception ("runValidation()|10| Please fix all the listed errors. Cannot continue");
		}
		
	}
	
	/**
	 * 
	 * @param clazzName
	 * @return
	 * @throws Exception
	 */
	public static ProducerDatasource getProducerDatasource(String clazzName) throws Exception{
		
		return (ProducerDatasource) createInstance(clazzName);
	}
	
	public static ConsumerCallback getConsumerCallback(String clazzName) throws Exception{
		
		return (ConsumerCallback) createInstance(clazzName);
	}
	
	/**
	 * 
	 * @param clazzName
	 * @return
	 * @throws Exception
	 */
	private static Object createInstance(String clazzName) throws Exception{

		try {
			Class<?> clazz = Class.forName(clazzName);
			return clazz.newInstance();
		} catch (ClassNotFoundException e) {
			log.error("ClassNotFoundException:Class["+clazzName+"]. Cannot Continue.",e);
			throw new RuntimeException("ClassNotFoundException:Class["+clazzName+"]. Cannot Continue.",e);
		} catch (InstantiationException e) {
			log.error("InstantiationException["+clazzName+"]. Cannot Continue.",e);
			throw new RuntimeException("InstantiationException:Class["+clazzName+"]. Cannot Continue.",e);
		} catch (IllegalAccessException e) {
			log.error("IllegalAccessException["+clazzName+"]. Cannot Continue.",e);
			throw new RuntimeException("IllegalAccessException:Class["+clazzName+"]. Cannot Continue.",e);
		} catch (ClassCastException e) {
			log.error("ClassCastException["+clazzName+"]. Cannot Continue.",e);
			throw new RuntimeException("ClassCastException:Class["+clazzName+"]. Cannot Continue.",e);
		} catch (Exception e) {
			log.error("Exception["+clazzName+"]. Cannot Continue.",e);
			throw new RuntimeException("Exception:Class["+clazzName+"]. Cannot Continue.",e);
		}
	}
	
}
