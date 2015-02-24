package org.infinispan.interceptors.ADFS.computation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import adfs.core.ADFSActiveFileMeta;

public class ADFSSparkScalaShellRunnable implements Runnable {

	private static final String SCALA_PROMPT = "\nscala> ";
	private static final String SCALA_IMPORTS = "import adfs.projects._\n";
	private static final int 	READ_BUFFER_SIZE = 512;
	
	private static final Log LOG =
			LogFactory.getLog(ADFSSparkScalaShellRunnable.class);
	
	public boolean started, busy;
	private String fsURL;
	private ADFSActiveFileMeta af;
	
	// Channels to write and read from the spark shell process
	private Process sparkShell;
	
	private InputStream is;
	private InputStream es;
	private OutputStream os;
	
	private BufferedReader br;
	private BufferedReader bre;
	private BufferedWriter bw;
	
	// ADFSDistProcess object to complete computation
	private ADFSDistProcessing dp;
	
	
	public ADFSSparkScalaShellRunnable(Process sparkShell, String fsURL,
								ADFSDistProcessing dp) {
		super();
		this.started = false;
		this.busy = false;
		this.fsURL = fsURL;
		this.dp = dp;
		this.af = null;
		
		this.sparkShell = sparkShell;
		
		this.is = sparkShell.getInputStream();
		this.br = new BufferedReader(new InputStreamReader(is));
		
		this.os = sparkShell.getOutputStream();
		this.bw = new BufferedWriter(new OutputStreamWriter(os));
		
		this.es = sparkShell.getErrorStream();
		this.bre = new BufferedReader(new InputStreamReader(es));
		
		// Launch shell
		new Thread(this).start();
	}
	
	
	@Override
	public void run() {
		// lock shell
		this.busy = true;
		
		char[] charBuffer = new char[READ_BUFFER_SIZE];
		char[] charBufferErr = new char[READ_BUFFER_SIZE];
		int n, nErr;
		
		// Launch shell (in a different thread??)
		if(!this.started) {
			try {
				// Read shell init output
				String initShell = "";
				do {
					n = br.read(charBuffer);
					initShell += new String(charBuffer, 0, n);
				} while(!initShell.endsWith(SCALA_PROMPT));
				
				LOG.warnf("INIT SHELL:\n" + initShell);
				LOG.warnf("spark shell now available!");
				
				// Write import to shell
				LOG.warnf("doing projects import...");
				bw.write(SCALA_IMPORTS);
				bw.flush();
				
				// Read shell import output
				String importRet = "";
				do {
					n = br.read(charBuffer);
					importRet += new String(charBuffer, 0, n);
				} while(!importRet.endsWith(SCALA_PROMPT));
				
				LOG.warnf("IMPORTS:\n" + importRet);
				LOG.warnf("imports completed!");
				
				this.started = true;
				
			} catch (Exception e) { e.printStackTrace(); }
		}
		
		// start the computation and wait for finishing it
		else {
			// 1st we need to build the command line for the special adfs call
			
			// Find class name
			String procArgs = af.getComputationArgs();
			int afClassStart = procArgs.indexOf("--class ") + 8; // TODO ugly
			String afClass = procArgs.substring(afClassStart).split(" ", 2)[0];
			
			// Build proc args
			String afProjArgs = "";
			if(af.getProjectArgs() != null)
				if(!af.getProjectArgs().isEmpty())
					for(String arg: af.getProjectArgs().split(" "))
						afProjArgs += "\"" + arg + "\"" + ",";
			
			// Build src files TODO just one string
			String srcFiles = "\"";
			for(String srcF: af.getSrcFiles())
				srcFiles += fsURL + srcF + ",";
			srcFiles.substring(0, srcFiles.length()-1);
			srcFiles = srcFiles.substring(0, srcFiles.length()-1) + "\",";
			
			// Hidden output
			String filename = Paths.get(af.getName()).getFileName().toString();
			String hiddenOutput = "\"" + fsURL +
					Paths.get(af.getName()).getParent() + "/." + filename + "\"";
			
			// Build final call
			String execProc = afClass + ".adfsRun(sc, Array(" +
					afProjArgs + srcFiles + hiddenOutput + "))\n";
			
			// Logging
			LOG.warnf("EXEC PROC: " + execProc);
			
			// 2nd we run the command line in the spark shell
			try {
				bw.write(execProc);
				bw.flush();

				// 3rd we read the result until we find the scala prompt
				String compRet = "";
				do {
					
					// Otherwise computations are not done, dont know why
					if(es.available() > 0)
						nErr = bre.read(charBufferErr);
					
					if(is.available() > 0) {
						n = br.read(charBuffer);
						compRet += new String(charBuffer, 0, n);
					}
					
				} while(!compRet.endsWith(SCALA_PROMPT));
				
				// Finally, we complete the file computation and kill the thread
				dp.completeComputation(af.getName());
				LOG.warnf("EXEC:\n" + compRet);
				LOG.warnf("PROC done! Spark shell is now available!");

			} catch (Exception e) { e.printStackTrace(); }
		}
		
		// free shell
		this.busy = false;
	}
	
	
	// Check if this object is being used by some thread
	public boolean isBusy() { return busy; }
	
	
	// Run a new computation
	public void procActiveFile(ADFSActiveFileMeta af) {
		this.af = af;
		new Thread(this).start();
	}

}
