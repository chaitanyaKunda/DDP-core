package com.agility.ddp.core.task;

import java.util.ArrayList;
import java.util.List;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.SchedulingException;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.quartz.DdpRuleSchedulerJob;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;

//@PropertySource({"file:///E:/DDPConfig/export.properties","file:///E:/DDPConfig/ddp.properties"})
public class DdpCreateSchedulerTask implements Task
{
	private static final Logger logger = LoggerFactory.getLogger(DdpCreateSchedulerTask.class);
	
	private final String strJobId = DdpCategorizationTask.class.getCanonicalName();
	
	@Autowired
	private TaskUtil taskUtil;
		
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	DdpSchedulerService ddpSchedulerService;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
    private JdbcTemplate controlJdbcTemplate;
	
	@Autowired
	ApplicationContext applicationContext;	
	
	private List<SchedulerFactoryBean> schlist = new ArrayList<SchedulerFactoryBean>();
	
	public String getStrJobId() {
		return strJobId;
	}

	public void execute()
	{
		execute(strJobId);
	}
	
	public void execute(JobExecutionContext context)
	{
		execute(context.getJobDetail().getKey().getGroup()+"."+context.getJobDetail().getKey().getName());
	}
	
	public void execute(String jobName)
	{
		logger.info("DdpCreateSchedulerTask.execute(String jobName) method invoked.");
		
		List<DdpScheduler> ddpSchedulerList = null; 
		
		try 
		{
			ddpSchedulerList = ddpSchedulerService.findAllDdpSchedulers();
			List<Trigger> triggerList = new ArrayList<Trigger>();
			
			//default fire at 12 midnight every month 1st day
			String strCronExp = "0 0 0 1 * ?";
			
			for(DdpScheduler ddpScheduler :  ddpSchedulerList)
        	{
				if(ddpScheduler.getSchType() != null && ddpScheduler.getSchType().trim().equalsIgnoreCase("CRON") && ddpScheduler.getSchStatus() == 0 )
				{
					/** STEP 1 : INSTANTIATE JOB DETAIL CLASS AND SET ITS PROPERTIES **/
		            MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		            
		            Object schedulerJob = null;
		            
		            String strBeanName = env.getProperty("export.rule."+ddpScheduler.getSchRuleCategory()+".beanName");
	            	String strClassName = env.getProperty("export.rule."+ddpScheduler.getSchRuleCategory()+".className");
		            
		            if(ddpScheduler.getSchRuleCategory() != null && strBeanName != null && strClassName != null) 
		            {
		            	schedulerJob = applicationContext.getBean(strBeanName, Class.forName(strClassName));
		            	logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",strBeanName,strClassName,"SCHEDULER : "+ddpScheduler.getSchId());
		            }
		            else
		            {
		            	schedulerJob = applicationContext.getBean("schedulerJob", DdpRuleSchedulerJob.class);
		            	logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Deafult Scheduler Bean [schedulerJob] and Class [DdpRuleSchedulerJob.class] for [{}].","SCHEDULER : "+ddpScheduler.getSchId());
		            }
		            		            
		            		            
		            jdfb.setTargetObject(schedulerJob);
		            jdfb.setTargetMethod("initiateSchedulerJob");
		            jdfb.setArguments(new Object[]{ddpScheduler});
		            logger.info("DdpCreateSchedulerTask.execute(String jobName) - JOB [{}] created.","SCHEDULER : "+ddpScheduler.getSchId());
		            jdfb.setName("SCHEDULER : "+ddpScheduler.getSchId());
		            jdfb.setGroup("GROUP : "+ddpScheduler.getSchId());
		            jdfb.afterPropertiesSet();
		            JobDetail jd = (JobDetail)jdfb.getObject();
		            //To run as stateful job.
		            jd.getJobDataMap().put("type", "FULL");
		            /** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/			
		            CronTriggerFactoryBean ctb = new CronTriggerFactoryBean();
//		            jd.getKey().jobKey("SCHEDULER : "+ddpScheduler.getSchId(), "GROUP : "+ddpScheduler.getSchId());
		           
					ctb.setJobDetail(jd);
					logger.info("DdpCreateSchedulerTask.execute(String jobName) - CRON TRIGGER [{}] created.","SCHEDULER"+ddpScheduler.getSchId());
					ctb.setBeanName("SCHEDULER"+ddpScheduler.getSchId());
					ctb.setGroup("GROUP : " + ddpScheduler.getSchId());
					ctb.setName("SCHEDULER : " + ddpScheduler.getSchId());
					strCronExp = ddpScheduler.getSchCronExpressions();
					logger.info("DdpCreateSchedulerTask.execute(String jobName) - [SCHEDULER{}] EXPRESSION : [{}].",ddpScheduler.getSchId(),strCronExp);
					
//					try 
//					{
						ctb.setCronExpression(strCronExp);
//					}
//					catch (Exception parseEx) 
//					{
//						parseEx.printStackTrace();
//						logger.error("DdpCreateSchedulerTask.execute(String jobName) - Parser Exception in CronExpression [{}].",strCronExp);
//					}
						
					ctb.afterPropertiesSet();
					triggerList.add(ctb.getObject());
					
					if (ddpScheduler.getSchReportFrequency() != null && ddpScheduler.getSchReportFrequency().length() > 0 && !ddpScheduler.getSchReportFrequency().equalsIgnoreCase("none")) {
						CronTriggerFactoryBean ctb1 = createCronExpressionForSchedulerReports(ddpScheduler);
						if (ctb1 != null) 
							triggerList.add(ctb1.getObject());
					}
					//NULL or 0 = not started;  1 = Running; -1 = Failed
					ddpScheduler.setSchIsRunning(1);
				}
				
        	}
            
			/** STEP 3 : INSTANTIATE SCHEDULER FACTORY BEAN AND SET ITS PROPERTIES **/
            SchedulerFactoryBean sfb = new SchedulerFactoryBean();
            //sfb.setJobDetails(new JobDetail[]{(JobDetail)jdfb.getObject()});
            Trigger[] triggerArray = triggerList.toArray(new Trigger[triggerList.size()]);
            sfb.setTriggers(triggerArray);
            sfb.setSchedulerName("Export Rule Job");
            sfb.afterPropertiesSet();
            try 
            {
                sfb.start();
                schlist.add(sfb);
                //Thread waits three minutes to load all jobs
                Thread.sleep(180L * 1000L);
            } 
            catch (SchedulingException e) 
            {
            	//Update the status when job failed
	        	//categorizedDocs.setCatStatus(-1);
				//ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDocs);
	            e.printStackTrace();
            }
            finally
            {
            	//No need to stop the Scheduler
            	//sfb.stop();
            	//logger.info("DdpCreateSchedulerTask.execute(String jobName) - SCHEDULER stopped.");
            }
            
            //Update DdpScheduler table data such as is_running, 
            for(DdpScheduler ddpScheduler :  ddpSchedulerList)
         	{
             	if(ddpScheduler.getSchType() != null && ddpScheduler.getSchType().trim().equalsIgnoreCase("CRON"))
 				{
             		//NULL or 0 = not started;  1 = Running; -1 = Failed
 					ddpScheduler.setSchIsRunning(-1);
             		ddpSchedulerService.updateDdpScheduler(ddpScheduler);
 				}
         	}
        } 
		catch (Exception ex)  
		{
			logger.error("DdpCreateSchedulerTask.execute(String jobName) - Exception while executing Job [{}] [Message:"+ex.getMessage()+"].",jobName);
			ex.printStackTrace();
			if (ddpSchedulerList != null)
            {
				//Update DdpScheduler table data such as is_running, 
	            for(DdpScheduler ddpScheduler :  ddpSchedulerList)
	        	{
	            	if(ddpScheduler.getSchType() != null && ddpScheduler.getSchType().trim().equalsIgnoreCase("CRON"))
					{
	            		ddpSchedulerService.updateDdpScheduler(ddpScheduler);
					}
	        	}
            }
        }

		logger.info("DdpCreateSchedulerTask.execute(String jobName) executed successfully.");
	}
	
	
	/**
	 * Method used for creating the new schedulers based on export rule.
	 * 
	 * @param ddpScheduler
	 */
	public void createNewScheduler(DdpScheduler ddpScheduler) {
		List<Trigger> triggerList = new ArrayList<Trigger>();
		
		if (ddpScheduler.getSchType() != null
				&& ddpScheduler.getSchType().trim().equalsIgnoreCase("CRON")
				&& ddpScheduler.getSchStatus() == 0) {
		
			try {
				/** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
				CronTriggerFactoryBean ctb = createCronExpressionForSchedular(ddpScheduler);
				
				if (ctb == null)
					return;
				
				triggerList.add(ctb.getObject());
				
				if (ddpScheduler.getSchReportFrequency() != null && ddpScheduler.getSchReportFrequency().length() > 0 && !ddpScheduler.getSchReportFrequency().equalsIgnoreCase("none")) {
					
					CronTriggerFactoryBean ctb1 = createCronExpressionForSchedulerReports(ddpScheduler);
					if (ctb1 != null)
						triggerList.add(ctb1.getObject());
				}
				// NULL or 0 = not started; 1 = Running; -1 = Failed
				ddpScheduler.setSchIsRunning(1);

				/**
				 * STEP 3 : INSTANTIATE SCHEDULER FACTORY BEAN AND SET ITS
				 * PROPERTIES
				 **/
				SchedulerFactoryBean sfb = new SchedulerFactoryBean();
				// sfb.setJobDetails(new
				// JobDetail[]{(JobDetail)jdfb.getObject()});
				Trigger[] triggerArray = triggerList
						.toArray(new Trigger[triggerList.size()]);
				sfb.setTriggers(triggerArray);
				sfb.afterPropertiesSet();				
				
				try {
					sfb.start();
					schlist.add(sfb);
					// Thread waits three minutes to load all jobs
				//	Thread.sleep(180L * 1000L);
				} catch (SchedulingException e) {
					// Update the status when job failed
					// categorizedDocs.setCatStatus(-1);
					// ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDocs);
					// e.printStackTrace();
					if (ddpScheduler.getSchType() != null
							&& ddpScheduler.getSchType().trim()
									.equalsIgnoreCase("CRON")) {
						// NULL or 0 = not started; 1 = Running; -1 = Failed
						ddpScheduler.setSchIsRunning(-1);
						ddpSchedulerService.updateDdpScheduler(ddpScheduler);

					}
				} finally {
					// No need to stop the Scheduler
					// sfb.stop();
					// logger.info("DdpCreateSchedulerTask.execute(String jobName) - SCHEDULER stopped.");
				}

				// Update DdpScheduler table data such as is_running,
			}

			catch (Exception ex) {
				// logger.error("DdpCreateSchedulerTask.execute(String jobName) - Exception while executing Job [{}] [Message:"+ex.getMessage()+"].",jobName);
				ex.printStackTrace();
				if (ddpScheduler != null) {
					// Update DdpScheduler table data such as is_running,

					if (ddpScheduler.getSchType() != null
							&& ddpScheduler.getSchType().trim()
									.equalsIgnoreCase("CRON")) {
						// NULL or 0 = not started; 1 = Running; -1 = Failed
						ddpScheduler.setSchIsRunning(-1);
						ddpSchedulerService.updateDdpScheduler(ddpScheduler);

					}

				}
			}
		}

	}
	
	/**
	 * Method used for creating the cron Expression 
	 * @param ddpScheduler
	 * @return
	 */
	public CronTriggerFactoryBean createCronExpressionForSchedular(
			DdpScheduler ddpScheduler) {

		CronTriggerFactoryBean ctb = null;
		try {
			/** STEP 1 : INSTANTIATE JOB DETAIL CLASS AND SET ITS PROPERTIES **/
			MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();

			Object schedulerJob = null;

			String strBeanName = env.getProperty("export.rule."
					+ ddpScheduler.getSchRuleCategory() + ".beanName");
			String strClassName = env.getProperty("export.rule."
					+ ddpScheduler.getSchRuleCategory() + ".className");

			if (ddpScheduler.getSchRuleCategory() != null
					&& strBeanName != null && strClassName != null) {
				schedulerJob = applicationContext.getBean(strBeanName,
						Class.forName(strClassName));
				logger.info(
						"DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",
						strBeanName, strClassName, "SCHEDULER : "
								+ ddpScheduler.getSchId());
			} else {
				schedulerJob = applicationContext.getBean("schedulerJob",
						DdpRuleSchedulerJob.class);
				logger.info(
						"DdpCreateSchedulerTask.execute(String jobName) - Loaded Deafult Scheduler Bean [schedulerJob] and Class [DdpRuleSchedulerJob.class] for [{}].",
						"SCHEDULER : " + ddpScheduler.getSchId());
			}

			jdfb.setTargetObject(schedulerJob);
			jdfb.setTargetMethod("initiateSchedulerJob");
			jdfb.setArguments(new Object[] { ddpScheduler });
			logger.info(
					"DdpCreateSchedulerTask.execute(String jobName) - JOB [{}] created.",
					"SCHEDULER : " + ddpScheduler.getSchId());
			jdfb.setName("SCHEDULER : " + ddpScheduler.getSchId());
			jdfb.setGroup("GROUP : " + ddpScheduler.getSchId());
			jdfb.afterPropertiesSet();
			JobDetail jd = (JobDetail) jdfb.getObject();

			/** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
			ctb = new CronTriggerFactoryBean();
			ctb.setGroup("GROUP : " + ddpScheduler.getSchId());
			ctb.setName("SCHEDULER : " + ddpScheduler.getSchId());
			ctb.setJobDetail(jd);
			
			
			logger.info(
					"DdpCreateSchedulerTask.execute(String jobName) - CRON TRIGGER [{}] created.",
					"SCHEDULER" + ddpScheduler.getSchId());
			ctb.setBeanName("SCHEDULER" + ddpScheduler.getSchId());
			ctb.setGroup("GROUP : " + ddpScheduler.getSchId());
			ctb.setName("SCHEDULER : "+ ddpScheduler.getSchId());
//			ctb.getObject().setJobName(jd.getName());
//			ctb.getObject().setJobGroup("Group Default");
			// ctb.setGroup("Group Default");
			String strCronExp = ddpScheduler.getSchCronExpressions();
			logger.info(
					"DdpCreateSchedulerTask.execute(String jobName) - [SCHEDULER{}] EXPRESSION : [{}].",
					ddpScheduler.getSchId(), strCronExp);

//			try {
				ctb.setCronExpression(strCronExp);
//			} catch (ParseException parseEx) {
//				parseEx.printStackTrace();
//				logger.error(
//						"DdpCreateSchedulerTask.execute(String jobName) - Parser Exception in CronExpression [{}].",
//						strCronExp);
//			}

			ctb.afterPropertiesSet();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return ctb;
	}
	
	/**
	 * Method used for updating the cron expression of existing job.
	 * 
	 * @param ddpScheduler
	 */
	public void updateSchedularJob (DdpScheduler ddpScheduler) {
		
		 logger.debug("updateSchedularJob (DdpScheduler ddpScheduler) is invoked");
		try {
			boolean isSchedulerUpdate = false;
			
			for (SchedulerFactoryBean factoryBean : schlist) {
				Scheduler sch = factoryBean.getScheduler();
		
			    //loop all group
			    for (String groupName : sch.getJobGroupNames()) {
			    	logger.debug("Group name : "+groupName);
			    	
			    	if (groupName.equalsIgnoreCase("GROUP : "+ddpScheduler.getSchId())) {
			    		
			    		TriggerKey triggerKey = new TriggerKey("SCHEDULER : "+ddpScheduler.getSchId(), "GROUP : "+ddpScheduler.getSchId());
			    		Trigger trigger = sch.getTrigger(triggerKey);
					 
					  	if (trigger != null) {
						  
							  CronTriggerFactoryBean newBean = createCronExpressionForSchedular(ddpScheduler);
							  if (newBean != null) {
								  sch.rescheduleJob(triggerKey, newBean.getObject());
								  isSchedulerUpdate = true;
								  logger.info("updateSchedularJob (DdpScheduler ddpScheduler).. ReStarted  Schedular for schedular ID : "+ddpScheduler.getSchId());
							  }
							  	
						  }
					  }
			    	
			    	if (ddpScheduler.getSchReportFrequency() != null && !ddpScheduler.getSchReportFrequency().isEmpty()  && !ddpScheduler.getSchReportFrequency().equalsIgnoreCase("none") && groupName.equalsIgnoreCase("GROUP-REPORT : "+ddpScheduler.getSchId())) {
			    		
			    		TriggerKey triggerKey = new TriggerKey("SCHEDULER-REPORT : "+ddpScheduler.getSchId(), "GROUP-REPORT : "+ddpScheduler.getSchId());
			    		Trigger trigger = sch.getTrigger(triggerKey);
					 
					  	if (trigger != null) {
						  
							  CronTriggerFactoryBean newBean = createCronExpressionForSchedulerReports(ddpScheduler);
							  if (newBean != null) {
								  sch.rescheduleJob(triggerKey, newBean.getObject());
								  isSchedulerUpdate = true;
								  logger.info("updateSchedularJob (DdpScheduler ddpScheduler).. ReStarted  Schedular for schedular ID : "+ddpScheduler.getSchId());
							  }
							  	
						  }
					  }
	 
			    }
			}
			
			if (!isSchedulerUpdate) {
				createNewScheduler(ddpScheduler);
			}
		} catch (Exception ex) {
			 logger.error("updateSchedularJob (DdpScheduler ddpScheduler) - error occurried while changing the cron expression for schedular :  "+ddpScheduler.getSchId(),ex.getMessage());
			//ex.printStackTrace();
		}
		
	}
	
	
	
	/**
	 * Method used for creating the cron Expression 
	 * @param ddpScheduler
	 * @return
	 */
	public CronTriggerFactoryBean createCronExpressionForSchedulerReports(
			DdpScheduler ddpScheduler) {

		CronTriggerFactoryBean ctb = null;
		try {
			/** STEP 1 : INSTANTIATE JOB DETAIL CLASS AND SET ITS PROPERTIES **/
			MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();

			Object schedulerJob = null;

			String strBeanName = env.getProperty("export.rule."
					+ ddpScheduler.getSchRuleCategory() + ".beanName");
			String strClassName = env.getProperty("export.rule."
					+ ddpScheduler.getSchRuleCategory() + ".className");

			if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null && strClassName != null) {
				
				schedulerJob = applicationContext.getBean(strBeanName,Class.forName(strClassName));
				logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",
						strBeanName, strClassName, "SCHEDULER : "+ ddpScheduler.getSchId());
			} else {
				schedulerJob = applicationContext.getBean("schedulerJob",DdpRuleSchedulerJob.class);
				logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Deafult Scheduler Bean [schedulerJob] and Class [DdpRuleSchedulerJob.class] for [{}].",
						"SCHEDULER : " + ddpScheduler.getSchId());
			}

			jdfb.setTargetObject(schedulerJob);
			jdfb.setTargetMethod("initiateSchedulerReport");
			jdfb.setArguments(new Object[] { ddpScheduler });
			logger.info("DdpCreateSchedulerTask.execute(String jobName) - JOB [{}] created.",
					"SCHEDULER-REPORT : " + ddpScheduler.getSchId());
			jdfb.setName("SCHEDULER-REPORT : " + ddpScheduler.getSchId());
			jdfb.setGroup("GROUP-REPORT : " + ddpScheduler.getSchId());
			jdfb.afterPropertiesSet();
			JobDetail jd = (JobDetail) jdfb.getObject();

			/** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
			ctb = new CronTriggerFactoryBean();
			ctb.setGroup("GROUP-REPORT : " + ddpScheduler.getSchId());
			ctb.setName("SCHEDULER-REPORT : " + ddpScheduler.getSchId());
			ctb.setJobDetail(jd);
			
			
			logger.info(
					"DdpCreateSchedulerTask.execute(String jobName) - CRON TRIGGER [{}] created.",
					"SCHEDULER" + ddpScheduler.getSchId());
			ctb.setBeanName("SCHEDULER-REPORT" + ddpScheduler.getSchId());
			ctb.setGroup("GROUP-REPORT : " + ddpScheduler.getSchId());
			ctb.setName("SCHEDULER-REPORT : "+ ddpScheduler.getSchId());
//			ctb.getObject().setJobName(jd.getName());
//			ctb.getObject().setJobGroup("Group Default");
			// ctb.setGroup("Group Default");
			String strCronExp = ddpScheduler.getSchReportFrequency();
			logger.info(
					"DdpCreateSchedulerTask.execute(String jobName) - [SCHEDULER{}] EXPRESSION : [{}].",
					ddpScheduler.getSchId(), strCronExp);

//			try {
				ctb.setCronExpression(strCronExp);
//			} catch (ParseException parseEx) {
//				parseEx.printStackTrace();
//				logger.error(
//						"DdpCreateSchedulerTask.execute(String jobName) - Parser Exception in CronExpression [{}].",
//						strCronExp);
//			}

			ctb.afterPropertiesSet();
		} catch (Exception ex) {
			logger.error("DdpCreateSchedulerTask.createCronExpressionForSchedulerReports() - Unable to create sheduler for report for Scheduler ID : "+ddpScheduler.getSchId(), ex);
		}

		return ctb;
	}
}