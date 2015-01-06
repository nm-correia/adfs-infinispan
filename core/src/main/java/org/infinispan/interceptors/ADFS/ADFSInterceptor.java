package org.infinispan.interceptors.ADFS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.ADFS.computation.ADFSDistProcessing;
import org.infinispan.interceptors.ADFS.computation.ADFSDistFSI;
import org.infinispan.interceptors.ADFS.computation.ADFSHDFS;
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
    private static final String PROP_CHECK_TIMESTEP = "checker_timestep";
    
    private static final String PROP_FILESYSTEM = "store_system";
    private static final String PROP_TMPDIR = "projects_dir";
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
   // blabla
   private ADFSDistFSI fs;
   
   private int checkerTimestep;
   
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
          this.checkerTimestep = Integer.parseInt(p.getProperty(PROP_CHECK_TIMESTEP));
          
          // Build the dist computation platform
          this.distProc = new ADFSDistProcessing(p, fs, adfsCache, m, checkerTimestep);
          
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
   private ADFSDistFSI buildFilesystem(Properties p) {
        String fsString = p.getProperty(PROP_FILESYSTEM);
        ADFSDistFSI fs;
       
        if(fsString.compareToIgnoreCase(HDFS_S) == 0) {
            String hdfs_url = p.getProperty(PROP_HDFS_URL);
            String tmpDir = p.getProperty(PROP_TMPDIR);
            
            fs = new ADFSHDFS(hdfs_url);
        }
        //else if(fs.compareToIgnoreCase(CASSANDRA_S) == 0) {
        //  // FUTURE
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
                		  
                  // Hinerith.... cenas
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
                      
                      // herdar os ficheiros ativos da diretoria
                      if(!fatherMeta.getAssocActiveFiles().isEmpty()) {
                          for(String activeFile: fatherMeta.getAssocActiveFiles())
                              afMeta.assocActiveFile(activeFile);
                          // TODO mark as stale??
                      }
                  }
                  
                  // normal file
                  /*if(confData == null) {
                      //LOG.warnf("PUT: NEW META: FOR NORMAL FILE: " + key);
                      
                      ADFSFileMeta afMeta = new ADFSFileMeta(keyPath);                      
                      
                      // change the value
                      //LOG.warnf("PUT: SETVALUE: " + key + " FINAL VALUE: " + afMeta);                   
                      cmd.setValue(m.objectToByteBuffer(afMeta));
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
                    
                    if(configProps.getProperty(PROP_INPUTFILES).compareTo("") != 0)
                        for(String f: configProps.getProperty(PROP_INPUTFILES).split(" "))
                            aafMeta.assocSrcFile(f);
                               
                    // Associate active file to the source file
            		for(String srcFile: aafMeta.getSrcFiles()) {			
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
                        
                        // TODO if dir, check all the subfiles
                        
                        //blabla
                        srcMeta.assocActiveFile(aafMeta.getName());
                        
                        srcMetaVal = m.objectToByteBuffer(srcMeta);
                        adfsCache.put(srcMetaKeyVal, srcMetaVal);      
                    }
                    
                    
                    // Run the computation if the active file is imediate process
                      if(aafMeta.isWorthItProcess()) {
                          //LOG.warnf("ACTIVE FILE WILL BE COMPUTED, SRC FILES: ");
                          
                        // lancar a computacao em background
                    	  distProc.processActiveFile(aafMeta);
                    	  aafMeta.setAvailability(false);
                          /*if(distProc.run(aafMeta) != 0)
                              LOG.errorf("Computation failed");*/
                    	  //Thread.sleep(120000);
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
                      
                      // Only If one of following situations occur over the file metadata
                      // of one active file the associated active files need to be recomputed:
                      //  - The list of src files
                      //  - The file passed to stale
                      // TODO for now any change will do
                      // TODO NAO GOSTO NADA DISTO, stale a false avisa mete os outros a stale??
                      // nheeeeee, e o burro sou eu???
                      if(aMeta.isActive()) {
                          //LOG.warnf("PUT MOD: META: FOR ACTIVE FILE: " + key);
                          
                          for(String afs: aMeta.getAssocActiveFiles()) {
                              //LOG.warnf("ACTIVE FILE OUTDATED :" + afs);
                              
                              byte[] afKey = m.objectToByteBuffer(afs);
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
                              
                              af.setStale(true);
                              afValue = m.objectToByteBuffer(af);
                              
                              adfsCache.put(afKey, afValue);
                          }
                      }
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
                      
                      // Update all active files that are using this file
                      for(String afs: aMeta.getAssocActiveFiles()) {
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
                          
                          af.setStale(true);
                          afValue = m.objectToByteBuffer(af);
                          
                          adfsCache.put(afKey, afValue);
                          
                          
                    	  // Only called in release, so mods in the fs are already made??
                          /*if(aaMeta.isWorthItProcess()) {
                              //LOG.warnf("ACTIVE FILE WILL BE COMPUTED, SRC FILES: ");
                          
                        	  //blabl
                        	  aaMeta.setAvailability(false);
                        	  aaMeta.incWrites();  // Increase writes
                        	  
                        	  aMetaVal = m.objectToByteBuffer(aaMeta);
                        	  adfsCache.put(aMetaKeyVal, aMetaVal);
                        	  
                        	// lancar a computacao em background
                        	  distProc.processActiveFile(aaMeta);
                          }*/
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
                  
                // se o ficheiro estiver stale e se valer a pena:
                  // CLIENTS CHECK EVERYTIME THEY DO A CONTENT GET
                  // TO SEE IF AN ACTIVE FILE IS AVAILABLE FOR READ
                  // NO NEED FOR THE CHECK BUT WE DO IT ANYWAY
                  if(aaMeta.isStale() && aaMeta.isAvailable()) {
                	  
                	  // not anymore
                	  aaMeta.setAvailability(false);
                	  aMetaVal = m.objectToByteBuffer(aaMeta);
                	  adfsCache.put(aMetaKeyVal, aMetaVal);
                     
                    // lancar a computacao
                      distProc.processActiveFile(aaMeta);
                      
                      // Instead of sending the old content
                      //return null;
                      
                  } // senao n e preciso fzr nada
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
                      afMeta.setStale(true);
                      afMetaVal = m.objectToByteBuffer(afMeta);
                      
                      adfsCache.put(afMetaKeyVal, afMetaVal);
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
