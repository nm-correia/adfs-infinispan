package org.infinispan.interceptors.ADFS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.ADFS.computation.ADFSDistProcessing;
import org.infinispan.interceptors.ADFS.storage.ADFSFSI;
import org.infinispan.interceptors.ADFS.storage.ADFSHDFS;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import adfs.core.ADFSFile;
import adfs.core.ADFSFileContent;
import adfs.core.ADFSFileMeta;
import adfs.core.ADFSActiveFileMeta;


public class ADFSInterceptor extends BaseCustomInterceptor {
    
    // Some string constants
    private static final String PROPERTIES_FILENAME = "ADFSInterceptor.properties";
    private static final String PROP_CONTENT_MAX_SIZE = "content_max_size";
    private static final String PROP_PROC_MAX_TIME = "proc_max_time";
    private static final String PROP_PROC_MAX_SIZE = "proc_max_size";
    
    private static final String PROP_FILESYSTEM = "store_system";
    private static final String HDFS_S = "hdfs";
    //private static final String CASSANDRA_S = "cassandra";
    private static final String PROP_HDFS_URL = "hdfs_url";
    
    private static final String PROP_FRAMEWORK = "framework";
    private static final String PROP_PROJECT = "project";
    private static final String PROP_PROJECT_ARGS = "project_args";
    private static final String PROP_PROCESSING_ARGS = "processing_args";
    private static final String PROP_INPUTFILES = "input_files";
    
   private static final Log LOG = LogFactory.getLog(ADFSInterceptor.class);
   
   private Address myAddress;
   private Cache<byte[], byte[]> adfsCache;
   private GenericJBossMarshaller m;
   private ADFSDistProcessing distProc;
   // blabla
   private int contentMaxSize;
   
   private long procMaxTime;
   
   private long procSrcMaxSize;
   
   
   // blabla
   private ADFSFSI fs;
   
   private byte[] nullVal;   
   
    
   @Override
   protected Log getLog() {
          return LOG;
   }

   @Override
   protected void start() {
      LOG.warnf("ADFS interceptor starting up for: "
                    + this.cache.getAdvancedCache().getName());
      
      if(this.cache.getAdvancedCache().getName().compareTo("ADFS") == 0) {
          this.myAddress = this.cache.getAdvancedCache().getRpcManager().getAddress();
          this.adfsCache = (Cache<byte[], byte[]>)this.cache;
          this.m = new GenericJBossMarshaller();
          
          // Read properties file
          Properties p = readPropertiesFile(PROPERTIES_FILENAME);
          if(p == null)
                LOG.errorf("property file not found in the classpath");
          
          // Build the filesystem
          this.fs = buildFilesystem(p);
          if(fs == null)
                LOG.errorf("filesystem specified in properties file not supported yet");
          
          // Other properties
          this.contentMaxSize = Integer.parseInt(p.getProperty(PROP_CONTENT_MAX_SIZE));
          this.procMaxTime = Long.parseLong(p.getProperty(PROP_PROC_MAX_TIME));
          this.procSrcMaxSize = Long.parseLong(p.getProperty(PROP_PROC_MAX_SIZE));
          
          // Build the dist computation platform
          this.distProc = new ADFSDistProcessing(p, fs, adfsCache, m);
          
          //BLABLE
          try { this.nullVal = m.objectToByteBuffer(null); }
          catch (Exception e) { e.printStackTrace(); }
      }
      
   }
   
   
   // bla bla
   private Properties readPropertiesFile(String filename) {
        Properties configProp = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename);
        
        if(inputStream == null) return null;
        
        try {
            configProp.load(inputStream);
            return configProp;
        } catch (IOException e) {
            return null;
        }
   }
   
   // bla bla
   private ADFSFSI buildFilesystem(Properties p) {
        String fsString = p.getProperty(PROP_FILESYSTEM);
        ADFSFSI fs;
       
        if(fsString.compareToIgnoreCase(HDFS_S) == 0) {
            String hdfs_url = p.getProperty(PROP_HDFS_URL);
            fs = new ADFSHDFS(hdfs_url);
        }
        //else if(fs.compareToIgnoreCase(CASSANDRA_S) == 0) {
        //  // TODO Cassandra
        //}
        else
            fs = null;
        
        return fs;
   }
   

   @Override
   protected void stop() {
      LOG.warnf("ADFS interceptor stopping");
   }

   
   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand cmd) throws Throwable {     
       if(!(cmd.getKey() instanceof byte[]))
           return invokeNextInterceptor(ctx, cmd);

      Address primaryNode = adfsCache.getAdvancedCache().getDistributionManager()
                .getPrimaryLocation(cmd.getKey());
      
      String key = (String)m.objectFromByteBuffer((byte[])cmd.getKey());
      Object value = (Object)m.objectFromByteBuffer((byte[])cmd.getValue());
      
      LOG.errorf("BEGIN visitPutKeyValueCommand <KEY:" + key +
                                        "> <VALUE:" + value + ">");
      
      if(myAddress.equals(primaryNode)) {
          //LOG.warnf("PUT: I'M (" + myAddress + ") THE PRIMARY FOR: " + key);
          
          Object o = getValue(ctx, cmd.getKey());
          
          if(o == null && value == null) { // new put
              
              // meta put
              if(key.startsWith(ADFSFile.ATTR_PREFIX)) {
                  
                  //LOG.warnf("PUT: META: Checking for config file for: " + key);
                  
                  // Check if there is a config file for this new file
                  // and therefore mark this file as an active file.
                  String keyPath = key.substring(ADFSFile.ATTR_PREFIX.length());
                  byte[] keyPathVal = m.objectToByteBuffer(keyPath);
                  
                  String confPath = keyPath + ADFSFile.CONF_FILE_EXTENSION;
                  byte[] confPathVal = m.objectToByteBuffer(confPath);
                  
                  String confMetaKey = ADFSFile.ATTR_PREFIX + confPath;
                  byte[] confMetaKeyVal = m.objectToByteBuffer(confMetaKey);
                  byte[] confMetaVal = adfsCache.get(confMetaKeyVal);
                  ADFSFileMeta confMeta = confMetaVal == null ?
                          null : (ADFSFileMeta)m.objectFromByteBuffer(confMetaVal);
                  
                  byte[] confContentVal = null;
                  ADFSFileContent confContent = null;
                  
                  if(confMeta != null) {
                	  confContentVal = adfsCache.get(confPathVal);
                	  confContent = confContentVal == null ?
                              null : (ADFSFileContent)m.objectFromByteBuffer(confContentVal);
                  }
                  
                  byte[] confData = null;
                  
                  if(confMeta == null) {
                      // If the file exists in the fs we will read the content
                      if(fs.isFile(confPath)) {
                          confData = fs.getContent(confPath);
                          
                          // register the conf file in infinispan, it exists
                          adfsCache.put(confMetaKeyVal, nullVal);
                          confContentVal = adfsCache.get(confPathVal);
                          confContent = (ADFSFileContent)m.objectFromByteBuffer(confContentVal);
                      }
                  }
                  else {
                      if(confContent.getData() != null)
                          confData = confContent.getData();
                      else
                          confData = fs.getContent(confPath);
                  }
                  
                  // The new meta...
                  ADFSFileMeta afMeta = confData == null ?
                		  new ADFSFileMeta(keyPath) : new ADFSActiveFileMeta(keyPath);
                		  
                  // Hinerith.... cenas -> SO AS DIRETORIAS!!!!!
                // If it is the root file no need for parents
        		  if(keyPath.compareTo("/") != 0) {
                      String dir = keyPath.substring(0, keyPath.lastIndexOf('/'));
                      
                      if(dir.isEmpty())
                          dir = "/";

                      //LOG.warnf("PUT: HINERITH: FATHER DIR " + dir);
                      
                      String fatherMetaKey = ADFSFile.ATTR_PREFIX + dir;
                      byte[] fatherMetaKeyVal = m.objectToByteBuffer(fatherMetaKey);
                      byte[] fatherMetaVal = adfsCache.get(fatherMetaKeyVal);
                      ADFSFileMeta fatherMeta = fatherMetaVal == null ?
                              null : (ADFSFileMeta)m.objectFromByteBuffer(fatherMetaVal);
                      
                      if(fatherMeta == null) {
                          // must exist in the fs
                          adfsCache.put(fatherMetaKeyVal, nullVal);
                          fatherMetaVal = adfsCache.get(fatherMetaKeyVal);
                          fatherMeta = (ADFSFileMeta)m.objectFromByteBuffer(fatherMetaVal);
                      }
                      
                      // herdar os ficheiros ativos da diretoria, so novas DIRS
                      if(!fatherMeta.getAssocActiveFiles().isEmpty()) {
                          for(String activeFile: fatherMeta.getAssocActiveFiles()) {
                        	  
                        	  // Only if new file is a dir associate the active files
                              // from the parent
                              if(fs.isDir(keyPath))
                            	  afMeta.assocActiveFile(activeFile);
                              else {
                            	// mark active files from parent stale
                            	  String activefMetaKey = ADFSFile.ATTR_PREFIX + activeFile;
                                  byte[] activefMetaKeyVal = m.objectToByteBuffer(activefMetaKey);
                                  byte[] activefMetaVal = adfsCache.get(activefMetaKeyVal);
                                  ADFSActiveFileMeta activefMeta = activefMetaVal == null ?
                                          null : (ADFSActiveFileMeta)m.objectFromByteBuffer(activefMetaVal);
                                  
                                  if(activefMeta == null) {
                                      // must exist in the fs TODO NOT
                                      adfsCache.put(activefMetaKeyVal, nullVal);
                                      activefMetaVal = adfsCache.get(activefMetaKeyVal);
                                      activefMeta = (ADFSActiveFileMeta)m.objectFromByteBuffer(activefMetaVal);
                                  }
                                  
                                  // TODO assoc dir if not present to active file too
                                  
                                  if(!activefMeta.setSrcWriteTime(dir, afMeta.getTime()))
                                	  LOG.errorf("SRC DIR: " + dir + " not present in this af!");
                                  
                                  activefMetaVal = m.objectToByteBuffer(activefMeta);
                                  adfsCache.put(activefMetaKeyVal, activefMetaVal);
                              }
                          }
                      }
                  }
                  
        		  // TODO NOOOOO it will build '/'
                  // normal file
                  /*if(confData == null) {
                	  // If it is a directory we must create every files inside
                	  // because..... may be active files inside???
                      if(fs.isDir(keyPath)) {
                    	  for(String dirF: fs.listDirFiles(keyPath)) {
                    		  String dirFMetaKey = ADFSFile.ATTR_PREFIX + dirF;
                    		  byte[] dirFMetaKeyVal = m.objectToByteBuffer(dirFMetaKey);
                    		  byte[] dirFMetaVal = adfsCache.get(dirFMetaKeyVal);
                    		  
                    		  if(dirFMetaVal == null)
                    			  adfsCache.put(dirFMetaKeyVal, nullVal);
                    	  }
                      }
                      //else // Nothing to do
                  }*/
        		  
                  // active file
        		  if(afMeta.isActive()) {
                      //LOG.warnf("PUT: NEW META: FOR ACTIVE FILE: " + key);
                      ADFSActiveFileMeta aafMeta = (ADFSActiveFileMeta)afMeta;
                      
                    // If this is an active file we need to read the config file
                    Properties configProps = new Properties();
                    configProps.load(new ByteArrayInputStream(confData));
                    
                    /*if(!configProps.containsKey(PROP_FRAMEWORK) ||
                            !configProps.containsKey(PROP_PROJECT)) {
                        LOG.errorf("config file: " + confPath +
                                " does not have '" + PROP_FRAMEWORK +
                                "' or/and '" + PROP_PROJECT + "' defined");
                        return invokeNextInterceptor(ctx, cmd);
                    }*/
                    
                    // Build the object to be stored in the infinispan cache.
                    aafMeta.setFramework(configProps.getProperty(PROP_FRAMEWORK));
                    aafMeta.setProject(configProps.getProperty(PROP_PROJECT));
                    aafMeta.setProjectArgs(configProps.getProperty(PROP_PROJECT_ARGS));
                    aafMeta.setComputationArgs(configProps.getProperty(PROP_PROCESSING_ARGS));
                    
                    String[] srcFiles = new String[0];
                    if(configProps.getProperty(PROP_INPUTFILES).compareTo("") != 0)
                    	srcFiles = configProps.getProperty(PROP_INPUTFILES).split(" ");
                               
                    // Associate active file to the source files
            		for(String srcFile: srcFiles) {
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
                        
                        // if an active file is created and uses a dir and
                        // that dir is already created? the subfiles won't be updated :S
                        // TODO if dir, check update all the subfiles, not here, maybe
                        // in put mod.
                        
                        //blabla
                        aafMeta.assocSrcFile(srcFile);
                        srcMeta.assocActiveFile(aafMeta.getName());
                        
                        srcMetaVal = m.objectToByteBuffer(srcMeta);
                        adfsCache.put(srcMetaKeyVal, srcMetaVal);      
                    }
                    
                    // Run the computation if the active file is imediate process
                      if(aafMeta.isWorthItProcess()) {
                          //LOG.warnf("ACTIVE FILE WILL BE COMPUTED, SRC FILES: ");
                          
                        // lancar a computacao em background
                    	  boolean ret = distProc.processActiveFile(aafMeta);
                    	  
                    	  if(ret)
                    		  aafMeta.setAvailability(false);
                    	  else
                    		  LOG.errorf("Computation failed");
                      }
                    
                      // ???
                    //  afMeta = aafMeta;
                      
                    // Update the new value
                      //LOG.warnf("PUT: SETVALUE: " + key + " FINAL VALUE: " + aafMeta);  
                    //cmd.setValue(m.objectToByteBuffer(aafMeta));
                    //LOG.warnf("active file created: " + aafMeta.toString());  
                  }
                                    
                  // Create automatically the new cache content
                  adfsCache.put(keyPathVal, nullVal);
                  
               // Update the new value to put
                  //LOG.warnf("PUT: SETVALUE: " + key + " FINAL VALUE: " + afMeta); 
                  cmd.setValue(m.objectToByteBuffer(afMeta));
              }
              
              // content put
              else {
                  //LOG.warnf("PUT: NEW CONTENT: FOR FILE: " + key);
                  ADFSFileContent aContent = new ADFSFileContent(contentMaxSize);
                  
                  // if the file exists in the fs, we can read to cache
                  if(fs.isFile(key)) {
                      long aContentSize = fs.getFileSize(key);
                      if(0 < aContentSize && aContentSize <= contentMaxSize)
                          aContent.setData(fs.getContent(key));
                  }
                  
                  //LOG.warnf("PUT: SETVALUE: " + key + " FINAL CONTENT SIZE: "
                    //                          + aContent.getData().length);  
                  cmd.setValue(m.objectToByteBuffer(aContent));
              }

          }
          else if(o != null) { // modification

              if(value == null) {
                  LOG.errorf("put with null with entry already created");
                  LOG.errorf("END visitPutKeyValueCommand <KEY:" + key +">");
                  return null;
              }
              
              ADFSFile a = (ADFSFile)m.objectFromByteBuffer((byte[])o);
              
              // meta put
              if(key.startsWith(ADFSFile.ATTR_PREFIX)) {
                  ADFSFileMeta aMeta = (ADFSFileMeta)a;

                  if(aMeta.isActive()) {
                	  ADFSActiveFileMeta oldMeta = (ADFSActiveFileMeta)aMeta;
                	  ADFSActiveFileMeta newMeta = (ADFSActiveFileMeta)value;
                	  
                	  if(newMeta.isStale() ||
                			  (newMeta.isStale() && newMeta.toCompute())) {
                		  
                		  System.out.println(newMeta.getAvgProcTime());
                		  // First we exec the proc if necessary
                		  // TODO total src files size is superior to X no need to compute
                		  if(newMeta.isWorthItProcess() &&
                				  newMeta.getAvgProcTime() < procMaxTime) {
                		  
                              LOG.warnf("IS WORTH PROCESS IT: " + newMeta.getName());
                			  
                			  // even if the active file is stale, we still need to check
                        	  // the states of the source files, may be still stale and
                			  // we have to force the computation
                        	  // distProc.checkActiveSrcFiles does that
                			  
                			  // We need to lauch lazy computations that must be computed before
                			  // this one, if they are lazy
                              // TODO UGLY COMPENSATE ASYNC PUT from sources
                              Thread.sleep(500);
                        	  boolean actSrcsRet = distProc.checkActiveSrcFiles(newMeta);
                			  
                        	  // If all the sources are not stale and available we can launch the computation
                        	  if(actSrcsRet) {
	                            // lancar a computacao em background
	                        	  boolean ret = distProc.processActiveFile(newMeta);
	                        	  
	                        	  if(ret) {
	                        		  newMeta.setAvailability(false);
	                        		  cmd.setValue(m.objectToByteBuffer(newMeta));	                        		  
	                        	  }
	                        	  else
	                        		  LOG.errorf("Computation failed");
                        	  }
                        	  else
                        		  LOG.errorf("There are still computations to be done!");
                          }
                		  
                		  // Second we update the assoc active files
                		  String aMetaName = aMeta.getName();
                		  long newTime = newMeta.lastSrcWriteTime();
                		  
                		  // Put all the assoc active files stale - recursive...
                		  for(String asocActFile: newMeta.getAssocActiveFiles()) {			
                				String assocMetaKey = ADFSFile.ATTR_PREFIX + asocActFile;
                	            byte[] assocMetaKeyVal = m.objectToByteBuffer(assocMetaKey);
                	            byte[] assocMetaVal = adfsCache.get(assocMetaKeyVal);
                	            ADFSActiveFileMeta assocMeta = assocMetaVal == null ?
                	                    null : (ADFSActiveFileMeta)m.objectFromByteBuffer(assocMetaVal);
                	            
                	            if(assocMeta == null) {
                	            	if(!fs.isFile(asocActFile)) {
                	            		LOG.errorf("File does not exist: " + asocActFile);
                	            		continue;
                	            	}
                	            	else {
                	            		adfsCache.put(assocMetaKeyVal, nullVal);
                	            		assocMetaVal = adfsCache.get(assocMetaKeyVal);
                	            		assocMeta = (ADFSActiveFileMeta)m.objectFromByteBuffer(assocMetaVal);
                	            	}
                	            }
                	            
                	            // Mod file entrance in every active file associated
                	            if(!assocMeta.setSrcWriteTime(aMetaName, newTime))
                              	  LOG.errorf("SRC FILE: " + aMetaName + " not present in this af!");
                	            else
                	            	adfsCache.putAsync(assocMetaKeyVal, m.objectToByteBuffer(assocMeta));
                		  }
                	  }
                	  
                	  // After processing...
                	  else if(oldMeta.isStale() && !newMeta.isStale()) {
                		  
                		// Update all the assoc active files
                		  for(String asocActFile: newMeta.getAssocActiveFiles()) {			
                				String assocMetaKey = ADFSFile.ATTR_PREFIX + asocActFile;
                	            byte[] assocMetaKeyVal = m.objectToByteBuffer(assocMetaKey);
                	            byte[] assocMetaVal = adfsCache.get(assocMetaKeyVal);
                	            ADFSActiveFileMeta assocMeta = assocMetaVal == null ?
                	                    null : (ADFSActiveFileMeta)m.objectFromByteBuffer(assocMetaVal);
                	            
                	            if(assocMeta == null) {
                	            	if(!fs.isFile(asocActFile)) {
                	            		LOG.errorf("File does not exist: " + asocActFile);
                	            		continue;
                	            	}
                	            	else {
                	            		adfsCache.put(assocMetaKeyVal, nullVal);
                	            		assocMetaVal = adfsCache.get(assocMetaKeyVal);
                	            		assocMeta = (ADFSActiveFileMeta)m.objectFromByteBuffer(assocMetaVal);
                	            	}
                	            }
                	            
                	            // Mod file entrance in every active file associated
                	            if(!assocMeta.setSrcWriteTime(newMeta.getName(), newMeta.getTime()))
                              	  LOG.errorf("SRC FILE: " + newMeta.getName() + " not present in this af!");
                	            else
                	            	adfsCache.putAsync(assocMetaKeyVal, m.objectToByteBuffer(assocMeta));
                		  }
                	  }
                  }
                  
                  // Only If one of following situations occur over the file metadata
                  // of one active file the associated active files need to be recomputed:
                  //  - The list of src files, only removes!!!
                  //  - The file passed to stale
                  // if the list of source changes the client must change the state
                  // to stale. if the state changes to stale there is nothing to do,
                  // because before the computation of the active file, we must
                  // check if all the sources are not stale, otherwise we compute
                  // the source active file
                  //else {
                      //LOG.warnf("PUT: MOD META: FOR NORMAL FILE: " + key);
                      // If it was the metadata of a normal that changed
                      // there's nothing to do. Only if it's content changes we need
                      // to stale the associated active files.
                  //}
              }
              
              // content put
              else {
                  //LOG.warnf("PUT: MOD CONTENT: FOR FILE: " + key);
                  //ADFSFileContent aContent = (ADFSFileContent)a;
                  
                  // must exist
                  String aMetaKey = ADFSFile.ATTR_PREFIX + key;
                  byte[] aMetaKeyVal = m.objectToByteBuffer(aMetaKey);
                  byte[] aMetaVal = adfsCache.get(aMetaKeyVal);
                  ADFSFileMeta aMeta = (ADFSFileMeta)m.objectFromByteBuffer(aMetaVal);
                  
                  // set with all active files that must be updated
                  Set<String> allAssocActFiles = new HashSet<String>();
                  allAssocActFiles.addAll(aMeta.getAssocActiveFiles());
                  
                  // father dir active files
                  if(key.compareTo("/") != 0) {
                      String dir = key.substring(0, key.lastIndexOf('/'));
                      
                      if(dir.isEmpty())
                          dir = "/";
                      
                      String fatherMetaKey = ADFSFile.ATTR_PREFIX + dir;
                      byte[] fatherMetaKeyVal = m.objectToByteBuffer(fatherMetaKey);
                      byte[] fatherMetaVal = adfsCache.get(fatherMetaKeyVal);
                      ADFSFileMeta fatherMeta = fatherMetaVal == null ?
                              null : (ADFSFileMeta)m.objectFromByteBuffer(fatherMetaVal);
                      
                      if(fatherMeta == null) {
                          // must exist in the fs
                          adfsCache.put(fatherMetaKeyVal, nullVal);
                          fatherMetaVal = adfsCache.get(fatherMetaKeyVal);
                          fatherMeta = (ADFSFileMeta)m.objectFromByteBuffer(fatherMetaVal);
                      }
                      
                      allAssocActFiles.addAll(fatherMeta.getAssocActiveFiles());
                  }
                  
                  // Update meta
                  aMeta.updateTime();
                  String aMetaName = aMeta.getName();
        		  long newTime = aMeta.getTime();
        		  adfsCache.put(aMetaKeyVal, m.objectToByteBuffer(aMeta));
                
                  // Update all active files that are using this file
                  for(String afs: allAssocActFiles) {
                      //LOG.warnf("ACTIVE FILE OUTDATED : " + afs);
                      
                      byte[] afKey = m.objectToByteBuffer(ADFSFile.ATTR_PREFIX + afs);
                      byte[] afValue = adfsCache.get(afKey);
                      ADFSActiveFileMeta af = afValue == null ?
                              null : (ADFSActiveFileMeta)m.objectFromByteBuffer(afValue);
                      
                      if(af == null) {
                          // TODO check if exist in the fs
                          adfsCache.put(afKey, nullVal);
                          afValue = adfsCache.get(afKey);
                          af = (ADFSActiveFileMeta)m.objectFromByteBuffer(afValue);
                      }
                      
                      /*if(scrMeta == null) {
                       * LOG.errorf(SRC FILE: " + srcFile + " DOES NOT EXISTS");
                          return invokeNextInterceptor(ctx, cmd);
                      }*/
                      
                      af.incWrites();
                      
                      if(!af.setSrcWriteTime(aMetaName, newTime))
                      	  LOG.errorf("SRC FILE: " + aMetaName + " not present in this af!");

                      afValue = m.objectToByteBuffer(af);
                      
                      adfsCache.put(afKey, afValue);
                  }

              }
          }
      }
       
      LOG.errorf("END visitPutKeyCommand <KEY:" + m.objectFromByteBuffer((byte[])cmd.getKey()) + "> " +
		  		"<VALUE:" + ((cmd.getValue() == null) ? null : m.objectFromByteBuffer((byte[])cmd.getValue())));
       return invokeNextInterceptor(ctx, cmd);
   }
   
   
   // blabla
   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand cmd) throws Throwable {
       if(!(cmd.getKey() instanceof byte[]))
           return invokeNextInterceptor(ctx, cmd);
      
      Address primaryNode = adfsCache.getAdvancedCache().getDistributionManager()
                .getPrimaryLocation(cmd.getKey());
      
      String key = (String)m.objectFromByteBuffer((byte[])cmd.getKey());
      
      LOG.errorf("BEGIN visitGetKeyValueCommand <KEY:" + key + ">");

      if(myAddress.equals(primaryNode)) {
          //LOG.warnf("GET: I'M (" + myAddress + ") THE PRIMARY FOR: " + key);
          
          Object o = getValue(ctx, cmd.getKey());
          if(o == null) {
              LOG.warnf("GET: key not present in cache: " + key);
              LOG.errorf("END visitGetKeyValueCommand <KEY:" + key + ">");
              return invokeNextInterceptor(ctx, cmd);
          }
          
          // content get
          if(!key.startsWith(ADFSFile.ATTR_PREFIX)) {
              
              // must exist meta for content
              String aMetaKey = ADFSFile.ATTR_PREFIX + key;
              byte[] aMetaKeyVal = m.objectToByteBuffer(aMetaKey);
              byte[] aMetaVal = adfsCache.get(aMetaKeyVal);
              ADFSFileMeta aMeta = (ADFSFileMeta)m.objectFromByteBuffer(aMetaVal);
              
              if(aMeta.isActive()) {
                  //LOG.warnf("GET: CONTENT: FOR ACTIVE FILE: " + key);
                  
                  ADFSActiveFileMeta aaMeta = (ADFSActiveFileMeta)aMeta;
                  aaMeta.incReads();
                  aaMeta.setCompute(true);
                  boolean retSucc = true;
                  
                // se o ficheiro estiver stale e se valer a pena:
                  // CLIENTS CHECK EVERYTIME THEY DO A CONTENT GET
                  // TO SEE IF AN ACTIVE FILE IS AVAILABLE FOR READ
                  // NO NEED FOR THE CHECK BUT WE DO IT ANYWAY
                  if(aaMeta.isStale() && aaMeta.isAvailable()) {
                	  
                	// even if the active file is stale, we still need to check
                	  // the states of the source files, may be still stale and
        			  // we have to force the computation
                	  // distProc.checkActiveSrcFiles does that
                	  boolean actSrcsRet = distProc.checkActiveSrcFiles(aaMeta);
        			  
                	  if(actSrcsRet) {
                        // lancar a computacao em background
                    	  boolean ret = distProc.processActiveFile(aaMeta);
                    	  
                    	  if(ret)
                    		  aaMeta.setAvailability(false);
                    	  else {
                    		  LOG.warnf("Computation failed");
                    		  retSucc = false;
                    	  }
                	  }
                	  else {
                		  LOG.warnf("There are still computations to be done!");
                		  retSucc = false;
                	  }
                      
                  } // senao n e preciso fzr nada
                  
                  aMetaVal = m.objectToByteBuffer(aaMeta);
            	  adfsCache.put(aMetaKeyVal, aMetaVal);
            	  
            	  if(!retSucc)
            		  return nullVal;
                  
              } // senao n for ativo n e preciso fzr nada
              //else
                //  LOG.warnf("GET: CONTENT: FOR NORMAL FILE: " + key);
          }
          //else
            //  LOG.warnf("GET: META: FOR FILE: " + key);
      }
      LOG.errorf("END visitGetKeyValueCommand <KEY:" + key + ">");
      return invokeNextInterceptor(ctx, cmd);     
   }

   
   // blabla
   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand cmd) throws Throwable {  
       if(!(cmd.getKey() instanceof byte[]))
           return invokeNextInterceptor(ctx, cmd);
      
      Address primaryNode = adfsCache.getAdvancedCache().getDistributionManager()
                .getPrimaryLocation(cmd.getKey());
      
      String key = (String)m.objectFromByteBuffer((byte[])cmd.getKey());
      
      LOG.errorf("BEGIN visitRemoveCommand <KEY:" + key + ">");
      
      if(myAddress.equals(primaryNode)) {
          //LOG.warnf("GET: I'M (" + myAddress + ") THE PRIMARY FOR: " + key);
          
          Object o = getValue(ctx, cmd.getKey());
          if(o == null) {
              LOG.warnf("RM: key not present in cache: " + key);
              LOG.errorf("END visitRemoveCommand <KEY:" + key + ">");
              return invokeNextInterceptor(ctx, cmd);
          }
          
          // meta remove
          if(key.startsWith(ADFSFile.ATTR_PREFIX)) {
              //LOG.warnf("RM: META: FOR FILE: " + key);
              
              String keyPath = key.substring(ADFSFile.ATTR_PREFIX.length());
              byte[] keyPathVal = m.objectToByteBuffer(keyPath);
              ADFSFileMeta oldValue = (ADFSFileMeta)m.objectFromByteBuffer((byte[])o);
              
              for(String afs: oldValue.getAssocActiveFiles()) {
                  //LOG.warnf("RM: ACTIVE FILE OUTDATED :" + afs);
                  
                  String afMetaKey = ADFSFile.ATTR_PREFIX + afs;
                  byte[] afMetaKeyVal = m.objectToByteBuffer(afMetaKey);
                  byte[] afMetaVal = adfsCache.get(afMetaKeyVal);
                  ADFSActiveFileMeta afMeta = afMetaVal == null ?
                          null : (ADFSActiveFileMeta)m.objectFromByteBuffer(afMetaVal);
                  
                  if(afMeta == null) {
                      // TODO check if exist in the fs
                      adfsCache.put(afMetaKeyVal, nullVal);
                      afMetaVal = adfsCache.get(afMetaKeyVal);
                      afMeta = (ADFSActiveFileMeta)m.objectFromByteBuffer(afMetaVal);
                  }
                  
                  /*if(scrMeta == null) {
                   * LOG.errorf(SRC FILE: " + srcFile + " DOES NOT EXISTS");
                      return invokeNextInterceptor(ctx, cmd);
                  }*/

                  boolean disasRet = afMeta.disassocSrcFile(key);
                  //LOG.warnf("RM: disassociate: " + afs + " from: " + key + " = " + disasRet);
                  if(disasRet) {
                	  // TODO
                      //afMeta.setStale(true);
                      //afMetaVal = m.objectToByteBuffer(afMeta);
                      
                      //adfsCache.put(afMetaKeyVal, afMetaVal);
                  }           
              }
              
              /*if(oldValue.isActive()) {
                  for(String fs: ((ADFSActiveFile)oldValue).getSrcFiles()) {
                      ADFSFile affs = myCache.get(fs);
                      affs.disassocActiveFile(key);
                      myCache.put(fs, affs);
                  }
              }*/
              
              // NOOOOPE - for rename op is convinient
              // remove the content entry automatically
              //adfsCache.remove(keyPathVal);
          }
          //else
              //LOG.warnf("RM: CONTENT: FOR FILE: " + key);
      }
      
      LOG.errorf("END visitRemoveCommand <KEY:" + m.objectFromByteBuffer((byte[])cmd.getKey()) + "> " +
    		  		"<VALUE:" + ((cmd.getValue() == null) ? null : m.objectFromByteBuffer((byte[])cmd.getValue())));
      return invokeNextInterceptor(ctx, cmd);
   }
   
   
   
   private Object getValue(InvocationContext ctx, Object key) {
       CacheEntry entry = ctx.lookupEntry(key);
       
          if (entry == null || entry.isNull())
              return null;
          if (entry.isRemoved())
              return null;
          
          return entry.getValue();
   }
   
}
