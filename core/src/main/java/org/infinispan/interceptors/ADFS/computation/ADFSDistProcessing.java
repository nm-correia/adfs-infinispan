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
	
	private static final String SPARK_S = "spark";
	//private static final String HADOOP_S = "hadoop";
	
	private ADFSDistFSI fs;
	private Cache<byte[], byte[]> adfsCache;
	private Marshaller m;
	private Map<String, Boolean> compProcs;
	private byte[] nullVal;
	private int checkerTimestep;
	
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
		
		//blabla Spark
		this.sparkProc = new ADFSSpark(p, fs, this);
		this.sparkProc.initFramework();
		
		//TODO blabla Hadoop
		
		// No need now with spark shell
		//(new Thread(this)).start();
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
		switch(framework) {
		
			// Spark
			case SPARK_S:
				// mark the file
				// TODO synchronized? -> NOT NEEDED NOW WITH SHELL
				//compProcs.put(af.getName(), true);
				if(!this.sparkProc.execDistProcessing(af))
					throw new IOException("Spark exec error. All shells busy?");
			break;
			
			// TODO Hadoop
			
			// Not supported
			default:
				throw new IOException("Framework not supported");
			//break;
		}
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
	
	
	// blabla
	public void completeComputation(String afName)
			throws IOException, InterruptedException, ClassNotFoundException {
		
		String afMetaKey = ADFSFile.ATTR_PREFIX + afName;
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
		byte[] afContentKeyVal = m.objectToByteBuffer(afName);
		adfsCache.remove(afContentKeyVal);
		adfsCache.put(afContentKeyVal, nullVal);
		
		// blabl
		releaseSrcFiles(afMeta);
	}

	
	private void checkCompletedProcesses()
			throws IOException, InterruptedException, ClassNotFoundException {
		
		synchronized(compProcs) {
			for(Entry<String, Boolean> entry: compProcs.entrySet()) {
				if(entry.getValue()) continue;
				
				LOG.warnf("COMPUTATION COMPLETED: " + entry.getKey());
				completeComputation(entry.getKey());
				compProcs.remove(entry.getKey());
			}
		}
	}

	// BLABLA
	@Override
	public void run() {
        try {
            while(true) {
                // wait for a bit, before checking for completed computations
                Thread.sleep(checkerTimestep);
                checkCompletedProcesses();
            }
        } catch(Exception e) { e.printStackTrace(); }	
	}

}
