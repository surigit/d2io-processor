/**
 * 
 */
package com.suren.mbr.d2io.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.suren.mbr.d2io.config.D2IOConfig;
import com.suren.mbr.d2io.intf.ConsumerCallback;
import com.suren.mbr.d2io.intf.D2IOProcessor;
import com.suren.mbr.d2io.intf.ProducerDatasource;
import com.suren.mbr.d2io.intf.ResultSummary;
import com.suren.mbr.d2io.intf.impl.BoundedD2IOProcessor;
import com.suren.mbr.d2io.intf.impl.D2IOContext;
import com.suren.mbr.d2io.utils.D2IOUtils;

/**
 * @author Surendra Myneni
 *
 */
public class D2IOProcessorMain {
	private static Log log = LogFactory.getLog(D2IOProcessorMain.class.getName());
	private static String FILE="FILE";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("       D 2 I O      P R O C E S S O R           ");
		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

		//look for System Parameter the file 
		String file = System.getProperty(FILE);
		
		if(null==file){
			log.error("main()|1| JVM Arg -DFILE=<file path>/<file name> is missing. Cannot continue");
			System.exit(-1);
		}

		log.debug("main()|2| Received File ["+file+"]");
		
		D2IOProcessorMain main = new D2IOProcessorMain();
		try {
			main.run(file);
			log.info("main()|3| END");
		} catch (Exception e) {
			log.error("main()|3| Run encountered error. Terminating "+e.getLocalizedMessage(),e);
			System.exit(-1);
		}
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void run(String cfgFile) throws Exception{
		
		//1. Load the config File 
		D2IOConfig d2ioCfg = d2ioCfg = D2IOUtils.loadConfiguration(cfgFile);
		log.info("run()|1| D2IOConfig Created");
		
		//2. Validate Configuration  
		D2IOUtils.runValidation(d2ioCfg);
		log.info("run()|2| Validation complete");

		//3. Create the Producer DataSource
		ProducerDatasource producer = D2IOUtils.getProducerDatasource(d2ioCfg.getProducer().getClazz());
		log.info("run()|3| Producer Datasource Created");

		//4. Producer Configuration 
		if(d2ioCfg.getProducer().getCfgMap()==null || d2ioCfg.getProducer().getCfgMap().isEmpty()){
			log.warn("run()|4| Producer Configuratiion Map is empty or NULL. May be required for Producer to operate properly.");
		}
		
		//5. Create the Consumer call back
		ConsumerCallback consumer = D2IOUtils.getConsumerCallback(d2ioCfg.getConsumer().getClazz());;
		log.info("run()|5| Consumer Callback Created");

		//6. Consumer Configuration 
		if(d2ioCfg.getConsumer().getCfgMap()==null || d2ioCfg.getConsumer().getCfgMap().isEmpty()){
			log.warn("run()|6| Consumer Configuratiion Map is empty or NULL. May be required for Consumer to operate properly.");
		}

		D2IOContext ctx = new D2IOContext(producer, consumer, d2ioCfg);
		log.info("run()|7| D2IOContext Created");

		executeProcessor(ctx);

	}

	/**
	 * 
	 * @param ctx
	 * @throws Exception
	 */
	private void executeProcessor(D2IOContext ctx) throws Exception{
		
		D2IOProcessor prcs = new BoundedD2IOProcessor();
		try {
			log.info("executeProcessor()|1| Begin");
			ResultSummary summary = prcs.runProcess(ctx);
			log.info("executeProcessor()|2| "+summary);
			log.info("executeProcessor()|3| End");
			
		} catch (Exception e) {
			log.error("executeProcessor()|4| Error in Processor."+e.getLocalizedMessage(),e);
			throw new Exception ("executeProcessor()|4| Error in Processor."+e.getLocalizedMessage(),e);
		}

	}

}
