package org.infinispan.interceptors.ADFS.computation;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ADFSSpark {

	private static final String ADFS_COMPS_DIR = "computations";
	private static final String ADFS_ABS_COMPS_DIR = "/adfs/computations";
	
	private static final String PROP_SPARK_SHELL = "spark_shell";
	private static final String PROP_SPARK_MASTER = "spark_master";
	private static final String PROP_SPARK_JAR = "spark_jar";
	private static final String PROP_SPARK_CORES = "spark_exec_cores";
	private static final String PROP_TMPDIR = "projects_dir";
	
	private static final Log LOG = LogFactory.getLog(ADFSSpark.class);
	
	private String sparkShell, sparkMaster, sparkJar;
	private int execCores;
	private ADFSSparkThread[] shellPool;
	private ADFSDistFSI fs;
	private String tmpDir;
	
	
	public ADFSSpark(Properties p, ADFSDistFSI fs) {
		this.sparkShell = p.getProperty(PROP_SPARK_SHELL);
		this.sparkMaster = p.getProperty(PROP_SPARK_MASTER);
		//this.sparkJar = p.getProperty(PROP_SPARK_JAR);
		this.execCores = Integer.parseInt(p.getProperty(PROP_SPARK_CORES));
		this.shellPool = new ADFSSparkThread[1]; // TODO arg to prop filename
		
		this.tmpDir = p.getProperty(PROP_TMPDIR);
		this.fs = fs;
	}
	
	
	public void initFramework() {
		String compJars = "";
		
		File compsDir = new File(tmpDir + ADFS_COMPS_DIR);
		
		if(compsDir.exists()) {
			try { FileUtils.deleteDirectory(compsDir); }
			catch (IOException e1) { e1.printStackTrace(); }
		}
		
		fs.copyToLocal(ADFS_ABS_COMPS_DIR, tmpDir);
		
		for(File comp: compsDir.listFiles())
			compJars += comp.getAbsolutePath() + ",";
		
		try {
			String initSparkShellCmdl = sparkShell + " --master " + sparkMaster +
								" " + "--jars " + compJars +
								" " + "--total-executor-cores " + execCores;
			LOG.warnf("INIT SPARK SHELL COMMAND: " + initSparkShellCmdl);
			
			for(int i = 0; i < 1; i++) { // TODO
				Process sparkShell;
				sparkShell = Runtime.getRuntime().exec(initSparkShellCmdl);
				
				this.shellPool[i] = new ADFSSparkThread(sparkShell);
				this.shellPool[i].start();
			}
		
		} catch (Exception e) { e.printStackTrace(); }
	}
	
	
	public void execDistProcessing(String cmd) {}
	
}
