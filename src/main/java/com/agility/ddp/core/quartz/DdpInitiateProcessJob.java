package com.agility.ddp.core.quartz;

import java.util.Date;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import com.agility.ddp.core.task.Task;

@DisallowConcurrentExecution
public class DdpInitiateProcessJob extends QuartzJobBean  
{
	//@Autowired
	//Environment env;
	
	private static final Logger logger = LoggerFactory.getLogger(DdpInitiateProcessJob.class);
	
	@Autowired
	private Task task;
	
	public void setTask(Task task) 
	{
		this.task = task;
	}
	
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("DdpInitiateProcessJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpInitiateProcessJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		task.execute();
		logger.info("DdpInitiateProcessJob.executeInternal() executed successfully.");
	}

}
