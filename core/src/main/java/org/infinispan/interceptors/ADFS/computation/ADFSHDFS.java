package org.infinispan.interceptors.ADFS.computation;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ADFSHDFS implements ADFSDistFSI {
	
	private FileSystem hdfs;
	private String hdfs_url;
	private Configuration conf;

	public ADFSHDFS(String hdfs_url) {
		this.hdfs_url = hdfs_url;
		
		this.conf = new Configuration();
		conf.set("fs.hdfs.impl", 
	            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
	        );
		conf.set("fs.file.impl",
	            org.apache.hadoop.fs.LocalFileSystem.class.getName()
	        );
		
		/*conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));*/
		
		try { this.hdfs = FileSystem.get(new URI(hdfs_url), conf); }
		catch(Exception e) { e.printStackTrace(); }
	}
	
	@Override
	public void copyToLocal(String src, String dst) {
		try { hdfs.copyToLocalFile(false, new Path(src), new Path(dst), true); }
		catch(Exception e) { e.printStackTrace(); }
	}
	
	@Override
	public void mergeDir(String srcDir, String dstFile) {
		try { FileUtil.copyMerge(hdfs, new Path(srcDir), hdfs, new Path(dstFile),
								true, conf, null); }
		catch(Exception e) { e.printStackTrace(); }
	}
	
	@Override
	public boolean isDir(String path) {		
		try { return hdfs.isDirectory(new Path(path)); }
		catch(Exception e) { e.printStackTrace(); }
		return false;
	}
	
	@Override
	public boolean isFile(String path) {
		try { return hdfs.isFile(new Path(path)); }
		catch(Exception e) { e.printStackTrace(); }
		return false;
	}
	
	@Override
	public void rm(String path) {
		try { hdfs.delete(new Path(path), true); }
		catch(Exception e) { e.printStackTrace(); }
	}
	
	@Override
	public void mv(String src, String dst) {
		try { hdfs.rename(new Path(src), new Path(dst)); }
		catch(Exception e) { e.printStackTrace(); }
	}
	
	
	@Override
	public String getUrl() {
		return hdfs_url;
	}
	
	
	@Override
	public byte[] getContent(String path) {
		byte[] readBuf = null;
		
		try {
			FSDataInputStream input = hdfs.open(new Path(path));
			readBuf = new byte[input.available()];
			input.read(readBuf);
			input.close();
			
		} catch (Exception e) { e.printStackTrace(); }
		
		return readBuf;
	}
	
	@Override
	public long getFileSize(String path) {
		try { return hdfs.getFileStatus(new Path(path)).getLen(); }
		catch(Exception e) { e.printStackTrace(); }
		return -1;
	}
	
}
