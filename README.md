# d2io-processor
Data In Data Out Processor using a Bounded Buffer

# Synopsis
The library is a classic producer consumer use case using a bounder buffer to transfer entries from the producer to consumer. Hence the name Data In Data Out Processor. The Producer and Consumer are interfaces to be implemented by the end user.

# Code Example 

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

The prcsQue.put is adding entry to the bounded buffer and will be blocked when the buffer is at full capacity.

			//1. Poll the Bounded Buffer 
			rData = prcsQue.poll();
      
The Consumer thread polls the bounded buffer and on poll, removes the entry from the buffer on the head. The producer which is blocked due to full capacity, now adds a new entry at the tail of the buffer. 

By doing the above, the adding and removing to buffer keeps a steady flow of data and avoids staging a lot of data in the JVM, expecially when concurrent consumer workers has a delay in processing the inbound entries. 

# Motivation
Multiple Use Cases: 
a) A  2 GB file arrives on the hour and needs to be processed without reading the entire file into memory.
b) A database needs to be read 60K rows and each row needs to be processed without loading the entire result set into memory
c) Ability to concurrently process the Inbound entries.
d) The Consumer call back needs to send the inbound record to an external resource like a database, JMS Q, REST/SOAP service etc

# Configuration
 The d2io.yml file as available in the src/main/resources is self-explanatory. 

# Installation 
a) Download and Import into your eclipse workspace.
b) Apache Maven 3.2.1 and higher must be installed
c) Run maven build
d) In the target folder a jar named as "d2io-processor-1.0-jar-with-dependencies.jar" should be created
e) From the source(src/main/resources) copy the following files to an external folder location of your choice. 
  1) d2io.yml
  2) log4j.xml 
f) run the following from your command line 
   java -DFILE=<path>\d2io.yml -Dlog4j.configuration=file:<path>\log4j.xml -Xms256m -Xmx256m -XX:MaxMetaspaceSize=128m -XX:SurvivorRatio=8 -jar target/d2io-processor-1.0-jar-with-dependencies.jar

  e.g. java -DFILE=/tmp/d2io.yml -Dlog4j.configuration=file:/tmp/log4j.xml -Xms256m -Xmx256m -XX:MaxMetaspaceSize=128m -XX:SurvivorRatio=8 -jar target/d2io-processor-1.0-jar-with-dependencies.jar

# Tests
1) Multiple File Sizes were tested.
2) Missing Configuration and Error Scenarios were also tested

# Contributors
a) Suggestions are welcome. Testbranch.

# License
Apache Open License
