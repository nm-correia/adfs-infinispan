package org.infinispan.interceptors.ADFS.computation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.infinispan.interceptors.ADFS.storage.ADFSFSI;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import adfs.core.ADFSActiveFileMeta;

public class ADFSSpark {

	private static final String PROP_REMOTE_COMPS_DIR = "remote_projects_dir";
	private static final String PROP_TMPDIR = "local_projects_dir";
	
	private static final String PROP_SPARK_MASTER = "spark_master";
	private static final String PROP_SPARK_CORES = "spark_exec_total_cores";
	private static final String PROP_SPARK_MEM = "spark_exec_node_mem";
	
	private static final String PROP_SPARK_SCALA_SHELL = "spark_scala_shell";
	private static final String PROP_SPARK_SCALA_POOLSIZE =
			"spark_scala_shell_poolsize";

	
	private static final Log LOG = LogFactory.getLog(ADFSSpark.class);
	
	
	private String sparkScalaShell, sparkMaster;
	private int execTotalCores;
	private String execNodeMem;
	private int scalaShellSize;
	private ADFSSparkScalaShellRunnable[] scalaShellPool;
	private ADFSFSI fs;
	private String tmpDir;
	private ADFSDistProcessing dp;
	private String adfsProjRemoteDir;
	
	
	public ADFSSpark(Properties p, ADFSFSI fs, ADFSDistProcessing dp) {
		this.sparkMaster = p.getProperty(PROP_SPARK_MASTER);
		this.execTotalCores = Integer.parseInt(p.getProperty(PROP_SPARK_CORES));
		this.execNodeMem = p.getProperty(PROP_SPARK_MEM);
		
		this.sparkScalaShell = p.getProperty(PROP_SPARK_SCALA_SHELL);
		this.scalaShellSize = Integer.parseInt(p.getProperty(PROP_SPARK_SCALA_POOLSIZE));
		this.scalaShellPool = new ADFSSparkScalaShellRunnable[scalaShellSize];
		
		this.tmpDir = p.getProperty(PROP_TMPDIR);
		this.adfsProjRemoteDir = p.getProperty(PROP_REMOTE_COMPS_DIR);
		
		this.fs = fs;
		this.dp = dp;
	}
	
	
	// BLABLA
	public void initFramework() {
		// Initialize the scala and java shell
		initScalaShell();
		
		//TODO init python shell
	}
	
	
	// BLABLA
	private void initScalaShell() {
		String projDir = Paths.get(adfsProjRemoteDir).getFileName().toString();
		File compsDir = new File(tmpDir + projDir);
		if(compsDir.exists()) {
			try { FileUtils.deleteDirectory(compsDir); }
			catch (IOException e1) { e1.printStackTrace(); }
		}
		
		String compJars = "";
		fs.copyToLocal(adfsProjRemoteDir, tmpDir);
		for(File comp: compsDir.listFiles())
			compJars += comp.getAbsolutePath() + ",";
		
		try {
			String initSparkShellCmdl = sparkScalaShell + " --master " + sparkMaster +
								" --jars " + compJars +
								" --total-executor-cores " + execTotalCores +
								" --executor-memory " + execNodeMem;
			LOG.warnf("INIT SPARK SHELL COMMAND: " + initSparkShellCmdl);
			
			for(int i = 0; i < scalaShellSize; i++) {
				Process sparkShell;
				sparkShell = Runtime.getRuntime().exec(initSparkShellCmdl);
				this.scalaShellPool[i] =
						new ADFSSparkScalaShellRunnable(sparkShell, fs.getUrl(), dp);
			}
		
		} catch (Exception e) { e.printStackTrace(); }
	}
	
	
	// BLABLA
	public boolean execDistProcessing(ADFSActiveFileMeta af) {
		String projExtension = FilenameUtils.getExtension(af.getProject());
		
		boolean ret = false;
		switch(projExtension) {
		
			// Scala and Java projects
			case "jar":
				for(int i = 0; i < scalaShellSize; i++)
					if(!this.scalaShellPool[i].isBusy()) {
						this.scalaShellPool[i].procActiveFile(af);
						ret = true;
						break;
					}
			break;
			
			// Python projects
			case "py":
				// TODO
				ret = true;
			break;
		}
		
		return ret;
	}

}
