package com.agility.ddp.core.task;

import org.quartz.JobExecutionContext;

public interface Task 
{
	public void execute();
	public void execute(String jobName);
	public void execute(JobExecutionContext context);
}
