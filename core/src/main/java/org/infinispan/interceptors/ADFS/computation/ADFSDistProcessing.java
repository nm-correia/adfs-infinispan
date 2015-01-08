package org.infinispan.interceptors.ADFS.computation;

import java.io.IOException;
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
	
    private static final String PROP_CHECK_TIMESTEP = "checker_timestep";
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
			Cache<byte[], byte[]> c, Marshaller m) {
		
		this.fs = fs;
		this.adfsCache = c;
		this.m = m;
		this.checkerTimestep = Integer.parseInt(p.getProperty(PROP_CHECK_TIMESTEP));
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
	
	
	// TODO check for in progress active files (new meta field: inProgress)
	// TODO multiple prepares, the first release releases the files, must implement a counter
	// semaphore
	private void prepareSrcFiles(ADFSActiveFileMeta afMeta)
			throws IOException, InterruptedException, ClassNotFoundException {

		for(String srcFile: afMeta.getSrcFiles()) {			
			String srcMetaKey = ADFSFile.ATTR_PREFIX + srcFile;
            byte[] srcMetaKeyVal = m.objectToByteBuffer(srcMetaKey);
            byte[] srcMetaVal = adfsCache.get(srcMetaKeyVal);
            ADFSFileMeta srcMeta = srcMetaVal == null ?
                    null : (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            
            if(srcMeta == null) {
            	if(!fs.isFile(srcFile)) {
            		LOG.errorf("File does not exist: " + srcFile);
            		continue;
            	}
            	else {
            		adfsCache.put(srcMetaKeyVal, nullVal);
                    srcMetaVal = adfsCache.get(srcMetaKeyVal);
                    srcMeta = (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            	}
            }
            
            // TODO counter inc
            // BLOCK ACCESS TO SOURCE FILES
            srcMeta.setAvailability(false);
            
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
            	if(!fs.isFile(srcFile)) {
            		LOG.errorf("File does not exist: " + srcFile);
            		continue;
            	}
            	else {
            		adfsCache.put(srcMetaKeyVal, nullVal);
                    srcMetaVal = adfsCache.get(srcMetaKeyVal);
                    srcMeta = (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            	}
            }

            //blabla
            srcMeta.setAvailability(true);
            
            srcMetaVal = m.objectToByteBuffer(srcMeta);
            adfsCache.put(srcMetaKeyVal, srcMetaVal);
        }
		
	}
	
	
	private boolean execDistComputation(ADFSActiveFileMeta af) {
		
		boolean ret = false;
		String framework = af.getFramework();
		switch(framework) {
		
			// Spark
			case SPARK_S:
				// mark the file
				//synchronized(compProcs) {compProcs.put(af.getName(), true);}
				try {
					if(!this.sparkProc.execDistProcessing(af)) {
						releaseSrcFiles(af);
						LOG.errorf("Spark exec error. All shells busy?");
					} else ret = true;
				} catch(Exception e) { e.printStackTrace(); }
			break;
			
			// TODO case HADOOP_S:
			
			// Not supported
			default:
				LOG.errorf("Framework not supported");
			break;
		}
		return ret;
	}
	
	
	// BLEBLE
	// Active files associated were already mark as stale
	public boolean processActiveFile(ADFSActiveFileMeta afMeta) {
		
		try {
			//blabla
			prepareSrcFiles(afMeta);
			
			// Launch the computation in background
			return execDistComputation(afMeta);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
	
	
	// blabla
	public void completeComputation(String afName)
			throws IOException, InterruptedException, ClassNotFoundException {
		
		String afMetaKey = ADFSFile.ATTR_PREFIX + afName;
		byte[] afMetaKeyVal = m.objectToByteBuffer(afMetaKey);
		byte[] afMetaVal = adfsCache.get(afMetaKeyVal);
		ADFSActiveFileMeta afMeta = afMetaVal == null ?
		        null : (ADFSActiveFileMeta)m.objectFromByteBuffer(afMetaVal);
		
		// FILE MUST EXIST, no need to check
		
		String filename = Paths.get(afMeta.getName()).getFileName().toString();
		String hiddenOutput = Paths.get(afMeta.getName()).getParent() + "/." + filename;
			
		// Check if final result is a directory
		if(fs.isDir(hiddenOutput)) {
			fs.mergeDir(hiddenOutput, hiddenOutput + "_done");
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
		
		// TODO update all the active files that are using this active file
		// how? i don't know :)
		
		// New cache content
		byte[] afContentKeyVal = m.objectToByteBuffer(afName);
		adfsCache.remove(afContentKeyVal);
		adfsCache.put(afContentKeyVal, nullVal);
		
		// blabl
		releaseSrcFiles(afMeta);
	}
	
	
	public boolean checkActiveSrcFiles(ADFSActiveFileMeta afMeta)
			throws IOException, InterruptedException, ClassNotFoundException {
		
		boolean ret = true;
		
		for(String srcFile: afMeta.getSrcFiles()) {			
			String srcMetaKey = ADFSFile.ATTR_PREFIX + srcFile;
            byte[] srcMetaKeyVal = m.objectToByteBuffer(srcMetaKey);
            byte[] srcMetaVal = adfsCache.get(srcMetaKeyVal);
            ADFSFileMeta srcMeta = srcMetaVal == null ?
                    null : (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            
            if(srcMeta == null) {
            	if(!fs.isFile(srcFile)) {
            		LOG.errorf("File does not exist: " + srcFile);
            		continue;
            	}
            	else {
            		adfsCache.put(srcMetaKeyVal, nullVal);
                    srcMetaVal = adfsCache.get(srcMetaKeyVal);
                    srcMeta = (ADFSFileMeta)m.objectFromByteBuffer(srcMetaVal);
            	}
            }
            
            if(srcMeta.isActive()) {
            	ADFSActiveFileMeta aSrcMeta = (ADFSActiveFileMeta) srcMeta;
            	if(aSrcMeta.isStale() && aSrcMeta.isAvailable()) {
            		// Launch the execution
            		adfsCache.get(m.objectToByteBuffer(srcFile));
            		// TODO must be null
            		ret = false;
            	}
            }
		}
		
		return ret;
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
