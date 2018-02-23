/**
 * 
 */
package com.agility.ddp.core.task;

import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.SchedulingException;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import com.agility.ddp.core.quartz.DdpMultiAedRuleJob;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.data.domain.DdpEmailTriggerSetup;
import com.agility.ddp.data.domain.DdpEmailTriggerSetupService;
import com.agility.ddp.data.domain.DdpRule;


/**
 * @author DGuntha
 *
 */
//@PropertySource({"file:///E:/DDPConfig/export.properties","file:///E:/DDPConfig/ddp.properties"})
public class DdpCreateMultiAedSchedulerTask implements Task {

	private static final Logger logger = LoggerFactory.getLogger(DdpCreateMultiAedSchedulerTask.class);
	private final String strJobId = DdpCategorizationTask.class.getCanonicalName();
	
//	@Autowired
//	private Environment env;
//	
	@Autowired
	private DdpEmailTriggerSetupService ddpEmailTriggerSetupService;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private ApplicationContext applicationContext;	
	
	@Override
	public void execute() {
		execute(strJobId);
	}
	
	private List<SchedulerFactoryBean> schTiggerlist = new ArrayList<SchedulerFactoryBean>();

	/**
	 * Method used for executing the cron job.
	 */
	@Override
	public void execute(String jobName) {
		
		logger.info("DdpCreateMultiAedSchedulerTask.execute(String jobName) method invoked.");
		List<DdpEmailTriggerSetup> ddpEmailTriggerSetups =  getAllEmailTriggerSetupBasedOnScheduler();
		
		if (ddpEmailTriggerSetups == null || ddpEmailTriggerSetups.size() == 0) {
			logger.info("DdpCreateMultiAedSchedulerTask.execute(String jobName) - DdpEmailTriggerSetups is empty list");
			return;
		}
		
		List<Trigger> triggerList = new ArrayList<Trigger>();
		
		for (DdpEmailTriggerSetup ddpEmailTriggerSetup : ddpEmailTriggerSetups) {
			
			CronTriggerFactoryBean cronTriggerBean = createCronExpressionForSchedular(ddpEmailTriggerSetup);
			
			if (cronTriggerBean != null)
				triggerList.add(cronTriggerBean.getObject());
		}
		
		long sleepTime = 120L* 1000L;
		startSchedulerJob(triggerList,sleepTime);
		
		logger.info("DdpCreateMultiAedSchedulerTask.execute(String jobName) executed succesfully.");
		
	}

	@Override
	public void execute(JobExecutionContext context) {
		execute(context.getJobDetail().getKey().getGroup()+"."+context.getJobDetail().getKey().getName());
	}
	
	/**
	 * Method used for creating the new Scheduler
	 * @param ddpEmailTriggerSetup
	 * @return
	 */
	public boolean createNewScheduler(DdpEmailTriggerSetup ddpEmailTriggerSetup) {
		
		boolean isSchedulerStarted = false;
		List<Trigger> tiggerList = new ArrayList<Trigger>();
		CronTriggerFactoryBean cronTriggerBean = createCronExpressionForSchedular(ddpEmailTriggerSetup);
		if (cronTriggerBean != null) {
			tiggerList.add(cronTriggerBean.getObject());
			isSchedulerStarted = startSchedulerJob(tiggerList, 10L);
		}
		
		return isSchedulerStarted;
	}
	
	/**
	 * Method used for updating the scheduler Job.
	 * 
	 * @param ddpEmailTriggerSetup
	 */
	public void updateSchedulerJob(DdpEmailTriggerSetup ddpEmailTriggerSetup) {
		
		try {

			
			for (SchedulerFactoryBean factoryBean : schTiggerlist) {
				Scheduler sch = factoryBean.getScheduler();
				List<String> groupNames = sch.getJobGroupNames();
				for (String groupName : groupNames) {
						if ( groupName.equalsIgnoreCase("TIGGER-GROUP : " + ddpEmailTriggerSetup.getEtrId())) {
						   //get job's trigger
							  Trigger trigger = sch.getTrigger(new TriggerKey( "TIGGER-SCHEDULER : " + ddpEmailTriggerSetup.getEtrId(),"TIGGER-GROUP : " + ddpEmailTriggerSetup.getEtrId()));
									 
							 if (trigger != null) {
										  
								 CronTriggerFactoryBean newBean = createCronExpressionForSchedular(ddpEmailTriggerSetup);
								  if (newBean != null) {
									  sch.rescheduleJob(new TriggerKey( "TIGGER-SCHEDULER : " + ddpEmailTriggerSetup.getEtrId(),"TIGGER-GROUP : " + ddpEmailTriggerSetup.getEtrId()), newBean.getObject());
									  logger.info("updateSchedulerJob (DdpScheduler ddpScheduler).. ReStarted  Scheduler for scheduler ID : "+ddpEmailTriggerSetup.getEtrId());
									  return;
								  }
											  	
								  
							 }
						}
				}
			    
			}
		} catch (Exception ex) {
			 logger.error("updateSchedularJob (DdpScheduler ddpScheduler) - error occurried while changing the cron expression for schedular :  "+ddpEmailTriggerSetup.getEtrId(),ex.getMessage());
			ex.printStackTrace();
		}
		
	}
	
	/**
	 * Method used for getting all Email Triggers.
	 * 
	 * @return {@link List}
	 */
	private List<DdpEmailTriggerSetup> getAllEmailTriggerSetupBasedOnScheduler() {
		
		logger.debug("DdpCreateMultiAedSchedulerTask.getAllEmailTriggerSetupBasedOnScheduler() is invoked");
		List<DdpEmailTriggerSetup> ddpEmailTriggerSetups = null;
		
		try {
			ddpEmailTriggerSetups = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_TIGGER_POINT_SCHEDULAR_ID, new Object[]{Constant.TRIGGER_NAME_SPECIFIC_TIME}, new RowMapper<DdpEmailTriggerSetup>(){

				@Override
				public DdpEmailTriggerSetup mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					
					DdpEmailTriggerSetup ddpEmailTriggerSetup = new DdpEmailTriggerSetup();
					ddpEmailTriggerSetup.setEtrId(rs.getInt("ETR_ID"));
					DdpRule rule = new DdpRule();
					rule.setRulId(rs.getInt("ETR_RULE_ID"));
					ddpEmailTriggerSetup.setEtrRuleId(rule);
					ddpEmailTriggerSetup.setEtrTriggerName(rs.getString("ETR_TRIGGER_NAME"));
					ddpEmailTriggerSetup.setEtrCronExpression(rs.getString("ETR_CRON_EXPRESSION"));
					ddpEmailTriggerSetup.setEtrDocTypes(rs.getString("ETR_DOC_TYPES"));
					ddpEmailTriggerSetup.setEtrDocSelection(rs.getString("ETR_DOC_SELECTION"));
					
					return ddpEmailTriggerSetup;
				}});
			
		} catch (Exception ex) {
			logger.error("DdpCreateMultiAedSchedulerTask.getAllEmailTriggerSetupBasedOnScheduler() - Exception occurried while reteriving details", ex);
			ex.printStackTrace();
		}
		logger.debug("DdpCreateMultiAedSchedulerTask.getAllEmailTriggerSetupBasedOnScheduler() executed successfully");
		return ddpEmailTriggerSetups;
	}
	
	
	/**
	 * Method used for creating the cron Expression 
	 * 
	 * @param 
	 * @return
	 */
	private CronTriggerFactoryBean createCronExpressionForSchedular(DdpEmailTriggerSetup ddpEmailTriggerSetup) {
		
		
		CronTriggerFactoryBean ctb = null;
		try {
			/** STEP 1 : INSTANTIATE JOB DETAIL CLASS AND SET ITS PROPERTIES **/
			MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();

			Object schedulerJob = applicationContext.getBean("multiAedRuleJob", DdpMultiAedRuleJob.class);
			
			jdfb.setTargetObject(schedulerJob);
			jdfb.setTargetMethod("initiateSchedulerJob");
			jdfb.setArguments(new Object[] { ddpEmailTriggerSetup });
			logger.info(
					"DdpCreateMulitAedSchedulerTask.createCronExpressionForSchedular(String jobName) - JOB [{}] created.",
					"Tigger ID : " + ddpEmailTriggerSetup.getEtrId());
			jdfb.setName("TIGGER-SCHEDULER : " + ddpEmailTriggerSetup.getEtrId());
			jdfb.setGroup("TIGGER-GROUP : " + ddpEmailTriggerSetup.getEtrId());
			jdfb.afterPropertiesSet();
			JobDetail jd = (JobDetail) jdfb.getObject();

			/** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
			ctb = new CronTriggerFactoryBean();
			ctb.setJobDetail(jd);
			logger.info(
					"DdpCreateMultiAedSchedulerTask.createCronExpressionForSchedular(String jobName) - CRON TRIGGER [{}] created.",
					"Tigger ID : " + ddpEmailTriggerSetup.getEtrId());
			ctb.setBeanName("TIGGER-SCHEDULER : " + ddpEmailTriggerSetup.getEtrId());
//			ctb.getObject().setJobName(jd.getName());
//			ctb.getObject().setJobGroup("Group Default");
			// ctb.setGroup("Group Default");
			String strCronExp = ddpEmailTriggerSetup.getEtrCronExpression();
			logger.info(
					"DdpCreateMultiAedSchedulerTask.createCronExpressionForSchedular(String jobName) - [SCHEDULER{}] EXPRESSION : [{}].",
					ddpEmailTriggerSetup.getEtrId(), strCronExp);

//			try {
				ctb.setCronExpression(strCronExp);
//			} catch (ParseException parseEx) {
//				parseEx.printStackTrace();
//				logger.error(
//						"DdpCreateMultiAedSchedulerTask.createCronExpressionForSchedular(String jobName) - Parser Exception in CronExpression [{}].",
//						strCronExp);
//			}

			ctb.afterPropertiesSet();
		} catch (Exception ex) {
			logger.error("DdpCreateMultiAedSchedulerTask.createCronExpressionForSchedular() - Exception occurried while creating cron expression for Tigger ID : "+ddpEmailTriggerSetup.getEtrId(), ex);
			ex.printStackTrace();
		}

		return ctb;
	}
	
	/**
	 * Method used for starting the scheduler job.
	 * 
	 * @param triggerList
	 * @param sleepTime
	 * @return
	 */
	private boolean startSchedulerJob (List<Trigger> triggerList, long sleepTime) {
		
		boolean isSchedularStarted = false;
		try {
			
			if (triggerList.size() > 0) {
				
				/** STEP 3 : INSTANTIATE SCHEDULER FACTORY BEAN AND SET ITS PROPERTIES **/
		        SchedulerFactoryBean sfb = new SchedulerFactoryBean();
		        sfb.setSchedulerName("Create Multi AED Scheduler");
		        //sfb.setJobDetails(new JobDetail[]{(JobDetail)jdfb.getObject()});
		        Trigger[] triggerArray = triggerList.toArray(new Trigger[triggerList.size()]);
		        sfb.setTriggers(triggerArray);
		        sfb.afterPropertiesSet();
		        try 
		        {
		            sfb.start();
		            schTiggerlist.add(sfb);
		            //Thread waits three minutes to load all jobs
		            Thread.sleep(sleepTime);
		            isSchedularStarted = true;
		        } 
		        catch (SchedulingException e) 
		        {
		            e.printStackTrace();
		        }
			}
		} catch (Exception ex) {
			logger.error("DdpCreateMultiAedSchedulerTask.execute(String jobName) - Exception occurried while running the SchedulerFactoryBean ", ex);
		}
		return isSchedularStarted;
	}

}
