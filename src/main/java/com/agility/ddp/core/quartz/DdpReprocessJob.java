/**
 * 
 */
package com.agility.ddp.core.quartz;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.task.DdpSchedulerJob;

/**
 * @author DGuntha
 *
 */
@PropertySource({"classpath:export.properties"})
public class DdpReprocessJob {

	
	private static final Logger logger = LoggerFactory.getLogger(DdpReprocessJob.class);
		
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	
	@Scheduled(cron="${export.cron.reprocessing}")
	public void reprocessJobs() {
		
		Calendar currentDate = GregorianCalendar.getInstance();
		Calendar startDate = GregorianCalendar.getInstance();
		
		//if any exception occurred default setting as 15 days.  
		int numberDays = 15;
		try {
			
			numberDays = Integer.parseInt(env.getProperty("export.reprocess.numberofdays"));
		} catch (NumberFormatException ex){
			
		}
		
		startDate.add(Calendar.DAY_OF_YEAR, -numberDays);
		
		String appNames = env.getProperty("export.reprocess.appNames");
		logger.info("DdpReprocessJob- reprocessJobs(): Invoked into the method : - "+(new Date()) +" : Application Name : "+appNames);
		
		
		if (null != appNames && appNames.length() > 0) {
			
			String[] applicationNames = appNames.split(",");
			
			for (String appName : applicationNames) {
				
				String strBeanName = env.getProperty("export.rule."+appName+".beanName");
				String strClassName = env.getProperty("export.rule."+appName+".className");
				if (null != strBeanName && strBeanName.length() > 0 &&  null != strClassName && strClassName.length() > 0) {
					try {
						
						DdpSchedulerJob job = (DdpSchedulerJob)applicationContext.getBean(strBeanName, Class.forName(strClassName));
						job.reprocesRuleJob(appName, currentDate,startDate);
						
					} catch (BeansException e) {
						logger.error("DdpReprocessJob - reprocessJobs() : Unable to peform reprocess for application : "+appName, e);
					} catch (ClassNotFoundException e) {
						logger.error("DdpReprocessJob - reprocessJobs() : Unable to peform reprocess for application : "+appName, e);
						e.printStackTrace();
					}
				}
			}
          
		}
	}
}
