/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.File;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;


/**
 * @author DGuntha
 *
 */
@Component
@PropertySources({@PropertySource(value="classpath:custom.properties"),@PropertySource(value="classpath:ddp.properties"),@PropertySource(value="classpath:export.properties"),@PropertySource(value="classpath:mail.properties"),@PropertySource(value="classpath:multiaedmail.properties"),
	@PropertySource(value ="file:///E:/DDPConfig/ddp.properties",ignoreResourceNotFound=true),@PropertySource(value="file:///E:/DDPConfig/mail.properties",ignoreResourceNotFound=true),
	@PropertySource(value="file:///E:/DDPConfig/custom.properties",ignoreResourceNotFound=true),@PropertySource(value="file:///E:/DDPConfig/export.properties",ignoreResourceNotFound=true),
	@PropertySource(value="file:///E:/DDPConfig/multiaedmail.properties",ignoreResourceNotFound=true)})
public class ApplicationProperties {
	
	private static final Logger logger = LoggerFactory.getLogger(ApplicationProperties.class);

	private CompositeConfiguration compositeCofiguration;
	
	@Autowired
	private Environment env;
	
	@Resource(name="quartzProperties")
	private  Properties quartzProperties;
	
	private static final String ORG_QUARTZ_SCHEDULER_INSTANCENAME = "org.quartz.scheduler.instanceName"; 

	 
	    @PostConstruct
	    private void init() {
	    	String[] fileNames = {"file:///E:/DDPConfig/custom.properties","file:///E:/DDPConfig/ddp.properties","file:///E:/DDPConfig/export.properties","file:///E:/DDPConfig/mail.properties","file:///E:/DDPConfig/multiaedmail.properties"};
	        try {
	            compositeCofiguration = new CompositeConfiguration();
	           
	            for (String fileName : fileNames) {
	            	PropertiesConfiguration configure = constructPropertiesConfiguration(fileName);
	            	if (configure != null)
	            		compositeCofiguration.addConfiguration(configure);
	            }
	           
	            System.out.println("Loading the properties file: " + fileNames);
	            
	          
	           
	        } catch (Exception ex) {
	        	logger.error("ApplicationProperties.init() - Unable construct the file Name :"+fileNames, ex);
	        }
	    }
	 
	    /**
	     * Method used for getting the values from the composite Configuration.
	     * 
	     * @param key
	     * @return
	     */
	    public String getProperty(String key) {
	    	
	    	String value =  compositeCofiguration.getString(key);
	        return ( value  == null? env.getProperty(key) : value);
	        		
	        
	    }
	    
	    /**
	     * Method used for getting the values from the composite Configuration.
	     * 
	     * @param key
	     * @return
	     */
	    public String getProperty(String key,String defaultValue) {
	    	
	    	String value =  compositeCofiguration.getString(key);
	       return  ( value  == null? env.getProperty(key,defaultValue) : value);
	        		
	        
	    }
	    
	    /**
	     * Method used get the values the resource based on the default value.
	     * 
	     * @param key
	     * @param alertnativeKey
	     * @param defaultValue
	     * @return
	     */
	    public String getProperty(String key,String alertnativeKey, String defaultValue) {
	    	
	    	String value =  compositeCofiguration.getString(key);
	       return  ( value  == null || value.isEmpty() ? env.getProperty(alertnativeKey,defaultValue) : value);
	        		
	        
	    }
	     
	    public void setProperty(String key, Object value) {
	    	compositeCofiguration.setProperty(key, value);
	    }
	     
	    /**
	     * Method used for construct properties configuration object.
	     * 
	     * @param fileName
	     * @return
	     */
	    private PropertiesConfiguration constructPropertiesConfiguration(String fileName) {
	    	
	    	 PropertiesConfiguration configuration = null;
	    	 
	    	try {
	    		boolean isFileExists = true;
	    		
	    		if (fileName.startsWith("file:///")) {
	    			String fileNam = fileName.substring(8);
	    			File file = new File(fileNam);
	    			isFileExists = file.exists();
	    		}
	    		
	    		if (isFileExists) {
			    	 FileChangedReloadingStrategy ddpfileChangedReloadingStrategy = new FileChangedReloadingStrategy();
				     ddpfileChangedReloadingStrategy.setRefreshDelay(1000);
				     configuration = new PropertiesConfiguration(fileName);
				     configuration.setReloadingStrategy(ddpfileChangedReloadingStrategy);
	    		}
	    	} catch (Exception ex) {
	    		logger.error("ApplicationProperties.constructPropertiesConfiguration() - Unable construct the file Name :"+fileName, ex);
	    	}
		     
		     return configuration;
	    }
	  

		/**
		  * Obtain a scheduler instance with the given name. 
		  * When a scheduler with this name exists already, this one is returned. 
		  * When the name is new, a new scheduler instance is created and returned. 
		  *  
		  * @param name 
		  * @return 
		  */ 
		 public  synchronized Scheduler getQuartzScheduler(String name) throws SchedulerException { 
			 
		  Scheduler res = null; 
		  if(quartzProperties!=null) { 
			  quartzProperties.setProperty(ORG_QUARTZ_SCHEDULER_INSTANCENAME,name); 
		  } else { 
			  	throw new SchedulerException("Initialization of scheduler properties failed"); 
		  } 
		  StdSchedulerFactory schedFact = new StdSchedulerFactory(quartzProperties); 
		  res = schedFact.getScheduler(); 
		   
		  return res; 
		 } 
}
