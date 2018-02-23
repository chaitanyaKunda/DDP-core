package com.agility.ddp.core.quartz;

import java.util.Date;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.agility.ddp.core.task.Task;

//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class DdpCreateSchedulerJob extends QuartzJobBean {
	
	//@Autowired
	//Environment env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	private static final Logger logger = LoggerFactory.getLogger(DdpCreateSchedulerJob.class);
	
	@Autowired
	private Task task;
	
	public void setTask(Task task) 
	{
		this.task = task;
	}
	
	
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("DdpCreateSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpCreateSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		task.execute();
		logger.info("DdpCreateSchedulerJob.executeInternal() executed successfully.");
	}

}
