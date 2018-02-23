/**
 * 
 */
package com.agility.ddp.core.quartz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * @author dguntha
 *
 */
@PropertySource({"classpath:multiaedmail.properties"})
public class DdpMultiAedReprocessJob {

	private static final Logger logger = LoggerFactory.getLogger(DdpMultiAedReprocessJob.class);
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Scheduled(cron="${multiaed.reprocess}")
	public void reprocessJobs() {
		
		logger.info("Invoked sucesfully in the reprocessJob");
		DdpMultiAedRuleJob mulitAedRuleJob = applicationContext.getBean("multiAedRuleJob", DdpMultiAedRuleJob.class);
		mulitAedRuleJob.reprocessJob();
	}
}
