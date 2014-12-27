package org.infinispan.interceptors.ADFS.computation;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ADFSSparkThread extends Thread {

	private static final Log LOG = LogFactory.getLog(ADFSSparkThread.class);
	
	private Process sparkShell;
	private boolean available;
	
	
	public ADFSSparkThread(Process sparkShell) {
		super();
		this.sparkShell = sparkShell;		
		this.available = false;
	}
	
	
	@Override
	public void run() {
		InputStream is = sparkShell.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		char[] charBuffer = new char[4*1024]; // TODO
		
		String oldBuf = "";
		int n;
		String sparkShellChunk;
		
		// TODO ugly
		try {
			Thread.sleep(30000); // wait 30 seconds for startup
			n = isr.read(charBuffer);// first read
			
			if(!isr.ready()) {
				LOG.warnf("spark shell now available!");
				this.available = true;
			} else {
				LOG.errorf("waited 1 minute but shell was not done...");
			}
			
		} catch (Exception e) { e.printStackTrace(); }
			
		
		while(true) {
			try {
				
				if(isr.ready()) {
					n = isr.read(charBuffer); // TODO initialize the array?
					sparkShellChunk = oldBuf.concat(new String(charBuffer));
					
					if(sparkShellChunk.contains("\nscala> ")) { // TODO endswith?
						this.available = true;
						oldBuf = "";
						LOG.warnf("Spark shell is now available!");
					}
					// TODO ugly
					else if(sparkShellChunk.endsWith("\n"))        oldBuf = "\n";
					else if(sparkShellChunk.endsWith("\ns"))       oldBuf = "\ns";
					else if(sparkShellChunk.endsWith("\nsc"))      oldBuf = "\nsc";
					else if(sparkShellChunk.endsWith("\nsca"))     oldBuf = "\nsca";
					else if(sparkShellChunk.endsWith("\nscal"))    oldBuf = "\nscal";
					else if(sparkShellChunk.endsWith("\nscala"))   oldBuf = "\nscala";
					else if(sparkShellChunk.endsWith("\nscala>"))  oldBuf = "\nscala>";
					else oldBuf = "";
				}
				else
					Thread.sleep(2000); // TODO dynamic???
				
			} catch (Exception e) { e.printStackTrace(); }
		}
	}
	
	// blabla
	public boolean available() { return this.available; }
	
}
