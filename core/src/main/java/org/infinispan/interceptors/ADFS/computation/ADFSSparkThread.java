package org.infinispan.interceptors.ADFS.computation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.Map;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import adfs.core.ADFSActiveFileMeta;

public class ADFSSparkThread implements Runnable {

	private static final Log LOG = LogFactory.getLog(ADFSSparkThread.class);
	
	private Process sparkShell;
	public boolean started, busy;
	private ADFSActiveFileMeta af; // blabla
	private String fsURL;
	private Map<String, Boolean> procs;
	
	private BufferedReader br;
	private BufferedWriter bw;
	
	
	public ADFSSparkThread(Process sparkShell, String fsURL, Map<String, Boolean> procs) {
		super();
		this.sparkShell = sparkShell;		
		this.started = false;
		this.busy = false;
		this.af = null;
		this.fsURL = fsURL;
		this.procs = procs;
		
		InputStream is = sparkShell.getInputStream();
		this.br = new BufferedReader(new InputStreamReader(is));
		
		OutputStream os = sparkShell.getOutputStream();
		this.bw = new BufferedWriter(new OutputStreamWriter(os));
	}
	
	
	@Override
	public void run() {
		// lock shell
		this.busy = true;
		
		char[] charBuffer = new char[512]; // TODO
		int n;
		String sparkShellChunk = "";
		
		if(!this.started) {
			// TODO ugly with timeouts :(
			try {
				Thread.sleep(20000); // wait 30 seconds for startup
				n = br.read(charBuffer);
				String initShell = new String(charBuffer, 0, n);
				LOG.warnf("INIT SHELL:\n" + initShell + "\nBYTES READ: " + n);
				
				if(initShell.endsWith("\nscala> ")) {
					LOG.warnf("spark shell now available!");
					
					LOG.warnf("doing projects import...");
					bw.write("import adfs.projects._\n");
					bw.flush();
					
					Thread.sleep(5000); // wait 5 seconds for imports
					n = br.read(charBuffer);
					String importRet = new String(charBuffer, 0, n);
					LOG.warnf("IMPORTS:\n" + importRet + "\nBYTES READ: " + n);
					
					if(!importRet.endsWith("\nscala> "))
						LOG.errorf("waited 5 seconds but imports were not completed..." );
					
					this.started = true;
					
				} else
					LOG.errorf("waited 30 seconds but shell was not done...");
				
			} catch (Exception e) { e.printStackTrace(); }
		}
		
		// start the computation and wait for finishing it
		else {
			// 1st we need to build the command line for the special adfs call
			
			// Find class name
			String procArgs = af.getComputationArgs();
			int afClassStart = procArgs.indexOf("--class ") + 8; // TODO nhe
			String afClass = procArgs.substring(afClassStart).split(" ", 2)[0];
			
			// Build proc args
			String afProjArgs = "";
			if(!af.getProjectArgs().isEmpty())
				for(String arg: af.getProjectArgs().split(" "))
					afProjArgs += "\"" + arg + "\"" + ",";
			
			// Build src files
			String srcFiles = "";
			for(String srcF: af.getSrcFiles())
				srcFiles += "\"" + fsURL + srcF + "\"" + ",";
			
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
			} catch (IOException e1) { e1.printStackTrace(); }
			
			// 3rd we read the result until we find the ????? - forgot the name xD
			while(true) {
				try {

					if(br.ready()) {
						n = br.read(charBuffer);
						sparkShellChunk += new String(charBuffer, 0, n);
						LOG.warnf("CHUNK:\n" + sparkShellChunk + "\nBYTES READ: " + n);
						
						// Finally, we mark the file as non running and kill the thread
						if(sparkShellChunk.endsWith("\nscala> ")) {
							procs.put(af.getName(), false); // TODO synchronized?
							LOG.warnf("Spark shell is now available!");
							break;
						}
						
						// TODO ugly, i think i don't need... it will block
						else if(sparkShellChunk.endsWith("\n"))        sparkShellChunk = "\n";
						else if(sparkShellChunk.endsWith("\ns"))       sparkShellChunk = "\ns";
						else if(sparkShellChunk.endsWith("\nsc"))      sparkShellChunk = "\nsc";
						else if(sparkShellChunk.endsWith("\nsca"))     sparkShellChunk = "\nsca";
						else if(sparkShellChunk.endsWith("\nscal"))    sparkShellChunk = "\nscal";
						else if(sparkShellChunk.endsWith("\nscala"))   sparkShellChunk = "\nscala";
						else if(sparkShellChunk.endsWith("\nscala>"))  sparkShellChunk = "\nscala>";
						else sparkShellChunk = "";
					}
					else
						Thread.sleep(100);
					
				} catch (Exception e) { e.printStackTrace(); }
			}
		}
		//try { is.close(); os.close(); }
		//catch (IOException e) { e.printStackTrace(); }
		
		// free shell
		this.busy = false;
	}
	
	public boolean isBusy() { return busy; }
	public void setActiveFile(ADFSActiveFileMeta af) { this.af = af; }
	
}
