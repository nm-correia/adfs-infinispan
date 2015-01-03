package org.infinispan.interceptors.ADFS.computation;

import java.io.IOException;
import java.net.URISyntaxException;

/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;*/


import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import adfs.core.ADFSActiveFileMeta;
import adfs.core.ADFSFile;
import adfs.core.ADFSFileMeta;

public class ADFSDistProcessing implements Runnable {
	
	//spark-submit --class org.apache.spark.examples.SrkPi --master yarn-cluster --conf spark.yarn.jar=hdfs://192.168.1.201/computations/spark/lib/spark-assembly-1.1.0-hadoop2.4.0.jar hdfs://192.168.1.201/computations/spark/scala/spark-examples.jar 4
	
	private static final String PROP_TMPDIR = "projects_dir";
	
	private static final String SPARK_S = "spark";
	private static final String HADOOP_S = "hadoop";
	
	private ADFSDistFSI fs;
	private Cache<byte[], byte[]> adfsCache;
	private Marshaller m;
	private Map<String, Boolean> compProcs;
	private byte[] nullVal;
	private int checkerTimestep;
	private String tmpDir;
	
	private ADFSSpark sparkProc;
	//private ADFSSpark hadoopProc;

	private static final Log LOG = LogFactory.getLog(ADFSDistProcessing.class);
	
	
	public ADFSDistProcessing(Properties p, ADFSDistFSI fs,
			Cache<byte[], byte[]> c, Marshaller m, int checkerTimestep) {
		
		this.fs = fs;
		this.adfsCache = c;
		this.m = m;
		this.checkerTimestep = checkerTimestep;
		this.compProcs = new HashMap<String, Boolean>();
	    
		//BLABLE
	    try { this.nullVal = m.objectToByteBuffer(null); }
	    catch (Exception e) { e.printStackTrace(); }
		
		// Other properties
		this.tmpDir = p.getProperty(PROP_TMPDIR);
		
		//blabla Spark
		this.sparkProc = new ADFSSpark(p, fs, compProcs);
		this.sparkProc.initFramework();
		
		//blabla
		(new Thread(this)).start();
	}
	
	
	// TODO check if src file is an active file that is computing
	// content == null?????
	private void prepareSrcFiles(ADFSActiveFileMeta afMeta)
			throws IOException, InterruptedException, ClassNotFoundException {

		for(String srcFile: afMeta.getSrcFiles()) {			
			String srcMetaKey = ADFSFile.ATTR_PREFIX + srcFile;
            byte[] srcMetaKeyVal = m.objectToByteBuffer(srcMetaKey);
            byte[] srcMetaVal = adfsCache.get(srcMetaKeyVal);
            ADFSFileMeta srcMeta = srcMetaVal == null ?
                    null : (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            
            if(srcMeta == null) {
                // TODO check if exist in the fs
                adfsCache.put(srcMetaKeyVal, nullVal);
                srcMetaVal = adfsCache.get(srcMetaKeyVal);
                srcMeta = (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            }
            
            /*if(scrMeta == null) {
             * LOG.errorf(SRC FILE: " + srcFile + " DOES NOT EXISTS");
                return invokeNextInterceptor(ctx, cmd);
            }*/
            
            //blabla
            srcMeta.setAvailability(false); // BLOCK ACCESS TO SOURCE FILES
            
            srcMetaVal = m.objectToByteBuffer(srcMeta);
            adfsCache.put(srcMetaKeyVal, srcMetaVal);
			
            // Fazer get a todos os srcs ativos para confirmar q nao estao stale
            // in a graph at least 1 active file does not use other active files
            // deadlocks may occur p1 needs p2 and p2 needs p1
            //LOG.warnf("GET: src file - " + srcFs);
            adfsCache.get(m.objectToByteBuffer(srcFile));           
        }
		
	}
	
	
	private void releaseSrcFiles(ADFSActiveFileMeta afMeta)
			throws IOException, InterruptedException, ClassNotFoundException {
		
		for(String srcFile: afMeta.getSrcFiles()) {			
			String srcMetaKey = ADFSFile.ATTR_PREFIX + srcFile;
            byte[] srcMetaKeyVal = m.objectToByteBuffer(srcMetaKey);
            byte[] srcMetaVal = adfsCache.get(srcMetaKeyVal);
            ADFSFileMeta srcMeta = srcMetaVal == null ?
                    null : (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            
            if(srcMeta == null) {
                // TODO check if exist in the fs
                adfsCache.put(srcMetaKeyVal, nullVal);
                srcMetaVal = adfsCache.get(srcMetaKeyVal);
                srcMeta = (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            }
            
            /*if(scrMeta == null) {
             * LOG.errorf(SRC FILE: " + srcFile + " DOES NOT EXISTS");
                return invokeNextInterceptor(ctx, cmd);
            }*/

            //blabla
            srcMeta.setAvailability(true);
            
            srcMetaVal = m.objectToByteBuffer(srcMeta);
            adfsCache.put(srcMetaKeyVal, srcMetaVal);
        }
		
	}
	
	
	private void execDistComputation(ADFSActiveFileMeta af)
			throws InterruptedException, IOException, URISyntaxException {
		
		String framework = af.getFramework();
		
		// Spark
		if(framework.compareToIgnoreCase(SPARK_S) == 0) {
			// mark the file
			// TODO synchronized?
			compProcs.put(af.getName(), true);
			this.sparkProc.execDistProcessing(af);
		}
		else
			throw new IOException("Framework not supported");
	}
	
	
	// BLEBLE
	// Active files associated were already mark as stale
	public void processActiveFile(ADFSActiveFileMeta afMeta) {
		
		try {
			//blabla
			prepareSrcFiles(afMeta);
			
			// Launch the computation in background
			execDistComputation(afMeta);

		} catch (Exception e) { e.printStackTrace(); }

	}

	
	private void checkCompletedProcesses()
			throws IOException, InterruptedException, ClassNotFoundException {
		
		synchronized(compProcs) {
			
			for(Entry<String, Boolean> entry: compProcs.entrySet()) { // iterator??

				if(entry.getValue()) continue;
				
				LOG.warnf("COMPUTATION COMPLETED: " + entry.getKey());
				
				String afMetaKey = ADFSFile.ATTR_PREFIX + entry.getKey();
				byte[] afMetaKeyVal = m.objectToByteBuffer(afMetaKey);
				byte[] afMetaVal = adfsCache.get(afMetaKeyVal);
				ADFSActiveFileMeta afMeta = afMetaVal == null ?
				        null : (ADFSActiveFileMeta)m.objectFromByteBuffer(afMetaVal);
				
				// MUST EXIST
				//if(afMetaVal != null) {
				
				String filename = Paths.get(afMeta.getName()).getFileName().toString();
				String hiddenOutput = Paths.get(afMeta.getName()).getParent() + "/." + filename;
					
					// Check if final result is a directory
					if(fs.isDir(hiddenOutput)) {
						fs.mergeDir(hiddenOutput, hiddenOutput + "_done");
						//fs.rmDir(hiddenOutput);
						
						fs.rm(afMeta.getName()); // the previous file
						fs.mv(hiddenOutput + "_done", afMeta.getName());
					}
					else {
						fs.rm(afMeta.getName()); // the previous file
						fs.mv(hiddenOutput, afMeta.getName());
					}
					
					afMeta.setAvailability(true);
					afMeta.setStale(false);
					
					afMetaVal = m.objectToByteBuffer(afMeta);
					adfsCache.put(afMetaKeyVal, afMetaVal);
					
					// New cache content
					byte[] afContentKeyVal = m.objectToByteBuffer(entry.getKey());
					adfsCache.remove(afContentKeyVal);
					adfsCache.put(afContentKeyVal, nullVal);
				//}
				
				// blabl
				releaseSrcFiles(afMeta);
				
				// blabla
				compProcs.remove(entry.getKey());
			}
		}
	}

	// BLABLA
	@Override
	public void run() {
        // run every 30 seconds...
        //final long timeToSleep = 30000;

        try {
        	
            while(true) {
                // wait for a bit, before checking for complete processes
                Thread.sleep(checkerTimestep);
                checkCompletedProcesses();
            }
            
        } catch(Exception e) { e.printStackTrace(); }	
	}

}
