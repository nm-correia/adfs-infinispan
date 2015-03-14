package org.infinispan.interceptors.ADFS.storage;

public interface ADFSFSI extends ADFSStorageI {
	
	public void rm(String path);
	public void mv(String src, String dst);
	public boolean isFile(String path);
	public boolean isDir(String path);
	public byte[] getContent(String path);
	public long getFileSize(String path);
	public String getUrl();
	public String[] listDirFiles(String path);
	
	public void mergeDir(String srcDir, String dstFile);
	public void copyToLocal(String src, String dst);
	
}
