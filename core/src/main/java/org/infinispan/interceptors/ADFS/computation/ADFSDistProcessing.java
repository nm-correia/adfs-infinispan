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
	private static final String PROP_SPARK_RUN = "spark_run";
	private static final String PROP_SPARK_MASTER = "spark_master";
	private static final String PROP_SPARK_JAR = "spark_jar";
	private static final String PROP_HADOOP_RUN = "hadoop_run";
	
	private static final String SPARK_S = "spark";
	private static final String HADOOP_S = "hadoop";
	
	private ADFSDistFSI fs;
	private Cache<byte[], byte[]> adfsCache;
	private Marshaller m;
	private Map<String, Process> compProcs;
	private byte[] nullVal;
	private int checkerTimestep;

	
	private static final Log LOG = LogFactory.getLog(ADFSDistProcessing.class);
	
	// TODO Different classes in the future
	private String sparkRun, hadoopRun;
	private String sparkMaster;
	private String sparkJar;
	
	private String tmpDir;
	
	public ADFSDistProcessing(Properties p, ADFSDistFSI f,
			Cache<byte[], byte[]> c, Marshaller m, int checkerTimestep) {
		this.fs = f;
		this.adfsCache = c;
		this.m = m;
		this.checkerTimestep = checkerTimestep;
		this.compProcs = new HashMap<String, Process>();
	    
		//BLABLE
	    try { this.nullVal = m.objectToByteBuffer(null); }
	    catch (Exception e) { e.printStackTrace(); }
		
		// Other properties
		this.tmpDir = p.getProperty(PROP_TMPDIR);
		this.sparkRun = p.getProperty(PROP_SPARK_RUN);
		this.sparkMaster = p.getProperty(PROP_SPARK_MASTER);
		this.hadoopRun = p.getProperty(PROP_HADOOP_RUN);
		this.sparkJar = p.getProperty(PROP_SPARK_JAR);
		
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
		String project = af.getProject();
		String projectArgs = af.getProjectArgs();
		String computationArgs = af.getComputationArgs();
		
		String filename = Paths.get(af.getName()).getFileName().toString();
		String hiddenOutput = Paths.get(af.getName()).getParent() + "/." + filename;
		
		String fs_url = fs.getUrl();
		String projectName = Paths.get(project).getFileName().toString();
		
		String srcFiles = "";
		for(String srcF: af.getSrcFiles())
			srcFiles += fs_url + srcF + ",";
		
		if(srcFiles.compareTo("") != 0)
			srcFiles = srcFiles.substring(0, srcFiles.length()-1);
		
		String commandLine;
		
		// Spark
		if(framework.compareToIgnoreCase(SPARK_S) == 0)			
			commandLine = sparkRun + " " + computationArgs +
					" " + "--master " + sparkMaster +
					" " + "--conf spark.yarn.jar=" + fs_url + sparkJar +
					" " + fs_url + project + " " + projectArgs +
					" " + srcFiles + " " + fs_url + hiddenOutput;
		// Hadoop
		else if(framework.compareToIgnoreCase(HADOOP_S) == 0)
			commandLine = hadoopRun + " " + computationArgs +
					" " + tmpDir + projectName + " " + projectArgs + " " + srcFiles +
					" " + fs_url + hiddenOutput;
		else
			throw new IOException("Framework not supported");
		
		// remove the previous result if any
		// TODO rmFile
		//fs.rmDir(af.getName());
		
		//fs.copyToLocal(project, tmpDir);
		//(new File(tmpDir + projectName)).delete();
		
		LOG.warnf("CMDLINE: " + commandLine);
		Process proc = Runtime.getRuntime().exec(commandLine);
		compProcs.put(af.getName(), proc); // delay the process
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
			
			for(Entry<String, Process> entry: compProcs.entrySet()) { // iterator??

				try { entry.getValue().exitValue(); } // Check exit value?
				catch(IllegalThreadStateException e) { continue; } // skip active file
				
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
