/**
 * 
 */
package com.agility.ddp.core.quartz;

import java.util.Date;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.agility.ddp.core.task.Task;

/**
 * @author dguntha
 *
 */
public class DdpAedWeeklySLASchedulerJob extends QuartzJobBean {

	private static final Logger logger = LoggerFactory.getLogger(DdpAedWeeklySLASchedulerJob.class);

	@Autowired
	private Task task;
	
	public void setTask(Task task) 
	{
		this.task = task;
	}
	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		
		logger.info("DdpAedWeeklySLASchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpAedWeeklySLASchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		task.execute(strJobName);
		logger.info("DdpAedWeeklySLASchedulerJob.executeInternal() executed successfully.");
		
	}

}
