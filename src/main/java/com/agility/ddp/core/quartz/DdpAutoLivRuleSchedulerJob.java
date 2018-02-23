/**
 * 
 */
package com.agility.ddp.core.quartz;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.components.DdpBackUpDocumentProcess;
import com.agility.ddp.core.components.DdpDFCClientComponent;
import com.agility.ddp.core.task.DdpSchedulerJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpScheduler;
import com.documentum.fc.client.IDfSession;

/**
 * @author DGuntha
 *
 */
@Configuration
public class DdpAutoLivRuleSchedulerJob extends QuartzJobBean implements
		DdpSchedulerJob {
	
	private static final Logger logger = LoggerFactory.getLogger(DdpAutoLivRuleSchedulerJob.class);

	
	@Autowired
	private DdpBackUpDocumentProcess ddpBackUpDocumentProcess;
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private ApplicationProperties applicationProperties;
	
	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		
		logger.info("DdpAutoLivRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpAutoLivRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpAutoLivRuleSchedulerJob.executeInternal() executed successfully.");
	}

	/**
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
		
		logger.info("DdpAutoLivRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		File generatedReportFolder = null;
		if (ddpScheduler.getSchStatus() == 0 && ddpScheduler.getSchReportFrequency() != null && !ddpScheduler.getSchReportFrequency().isEmpty() && !ddpScheduler.getSchReportFrequency().equalsIgnoreCase("none")) {
			try {
				commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-SchedulerReport :"+ddpScheduler.getSchId());
				String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchReportFrequency());
				Calendar startCalendar = Calendar.getInstance();
				Calendar endCalendar = Calendar.getInstance();  
		   		//Call below method to get date range - This needs to be implemented
		   		Calendar startDate = SchedulerJobUtil.getQueryStartDate(strFeq, startCalendar);
		   		Calendar endDate = endCalendar;
		   		generatedReportFolder = commonUtil.generateReports(ddpScheduler, startDate, endDate, 2,Constant.EXECUTION_STATUS_SUCCESS);
			} catch (Exception ex) {
				logger.error("DdpAutoLivRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj)  unable to generate reports for scheduler id :"+ddpScheduler.getSchId(), ex);
			} finally {
				commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-SchedulerReport :"+ddpScheduler.getSchId());
				if (generatedReportFolder != null)
					SchedulerJobUtil.deleteFolder(generatedReportFolder);
			}
		}		
	}
	
	/**
	 * Method used for initiates.
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerJob(Object[] ddpSchedulerObj) 	{
		
		Calendar currentDate = GregorianCalendar.getInstance();
		logger.info("DdpAutoLivRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked.");
		
		
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpAutoLivRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return;
		}
				
		logger.debug("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
		
		// if the type is the properties need to change the code.
		//UI configuration this process will be executed.
		try {
	    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
	    	 Scheduler scheduler = applicationProperties.getQuartzScheduler("DdpAutoLivSchedulerJob-_"+ddpScheduler.getSchId()+"_"+(new Date()));
	    	 jdfb.setTargetObject(this);
		     jdfb.setTargetMethod("generalShedulerJob");
		     jdfb.setArguments(new Object[]{ddpScheduler,currentDate,ddpScheduler.getSchRuleCategory().trim(),currentDate,scheduler});
		     jdfb.setName(ddpScheduler.getSchRuleCategory().trim()+"Process : "+ddpScheduler.getSchId()+"_"+(new Date()));
		     jdfb.afterPropertiesSet();
		     JobDetail jd = (JobDetail)jdfb.getObject();
		     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
		     trigger.setName(ddpScheduler.getSchRuleCategory().trim()+"Process : "+ddpScheduler.getSchId()+"_"+(new Date()));
		     trigger.setGroup(ddpScheduler.getSchRuleCategory().trim()+"Process-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
		     trigger.setStartTime(new Date());
		     trigger.setEndTime(null);
		     trigger.setRepeatCount(0);
		     trigger.setRepeatInterval(5000L);
		        
		     scheduler.scheduleJob(jd, trigger);
		   	 scheduler.start();
     	} catch (Exception ex) {
      		logger.error("DdpAutoLivRuleSchedulerJob.initiateSchedulerJob() Error occurried while running the ondemand service", ex);
      }
	
		logger.info("DdpAutoLivRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
	}
	
	/**
	 * Method used for executing the scheduler job.
	 * 
	 * @param ddpScheduler
	 * @param currentDate
	 * @param appName
	 * @param presentDate
	 * @param scheduler
	 */
	public void generalShedulerJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName,Calendar presentDate,Scheduler scheduler) {
		
		logger.info("DdpAutoLivRuleSchedulerJob.generalShedulerJob(Object[] ddpSchedulerObj) is invoked.");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
			ddpBackUpDocumentProcess.executeJob(ddpScheduler, currentDate, appName,presentDate);
		} catch (Exception ex) {
			logger.error("DdpAutoLivRuleSchedulerJob.generalShedulerJob() - Unable to execute the DdpBackupDocumentProcess.executeJob()	",ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpAutoLivRuleSchedulerJob.generalShedulerJob() - Unable to shutdown the Scheduler thread.",e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
		}
		logger.info("DdpAutoLivRuleSchedulerJob.generalShedulerJob(Object[] ddpSchedulerObj) executed successfully.");
		
	}
	

	
	/**
	 * Method used to run onDemand Service by use call.
	 * 
	 */
	@Override
	public void runOnDemandRuleJob(final DdpScheduler ddpScheduler,
			final Calendar fromDate,final Calendar toDate) {
			logger.info("DdpAutoLivRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) method invoked.");
			
			Calendar currentDate = GregorianCalendar.getInstance(); 
		if (ddpScheduler != null) {
			
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpAutoLivRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
   	      
   	        try {
		    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    		 Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    		 
		    		 jdfb.setTargetObject(this);
			         jdfb.setTargetMethod("runOnDemandService");
			         jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),2,fromDate, toDate,currentDate,null,null,null,scheduler});
			         jdfb.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			         jdfb.afterPropertiesSet();
			         JobDetail jd = (JobDetail)jdfb.getObject();
			         
			         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
				     trigger.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setGroup("OnDemand-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setStartTime(new Date());
				     trigger.setEndTime(null);
				     trigger.setRepeatCount(0);
				     trigger.setRepeatInterval(4000L);
			        
			         scheduler.scheduleJob(jd, trigger);
			     	 scheduler.start();
			     	 
   	        	} catch (Exception ex) {
   	        		logger.error("DdpAutoLivRuleSchedulerJob.runOnDemandRuleJob() Error occurried while running the ondemand service", ex);
   	        	}
			
		}
	}

	/**
	 * Method used for running the onDemandService.
	 * 
	 * @param ddpScheduler
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService
	 * @param fromDate
	 * @param toDate
	 * @param currentDate
	 */
	public void runOnDemandService(DdpScheduler ddpScheduler,DdpExportRule ddpExportRule,String appName,
			int typeOfService,Calendar fromDate,Calendar toDate,Calendar currentDate,String jobNumbers,String consignmentIDs,String docRefs,Scheduler scheduler) {
		
		logger.info("DdpAutoLivRuleSchedulerJob.runOnDemanService() - thread invoked successfully");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
			if (typeOfService == 2)
				ddpBackUpDocumentProcess.processSchedulerJob(ddpScheduler, ddpExportRule,appName, typeOfService, fromDate, toDate,currentDate);
			else
				ddpBackUpDocumentProcess.onDemandProcessSchedulerJob(ddpScheduler, ddpExportRule, appName, typeOfService, fromDate, toDate, currentDate, jobNumbers, consignmentIDs, docRefs);
		} catch (Exception ex) {
			logger.error("DdpAutoLivRuleSchedulerJob.runOnDemanService() - Unable to execute the processSchedulerJob in DdpBackUpDocumentProcess", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpAutoLivRuleSchedulerJob.runOnDemanService() - Unable to shutdown the scheduler thread", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
		}
	}
	
	@Override
	public void reprocesRuleJob(final String appName,final Calendar  endDate,final Calendar startDate) {
		
		logger.info(appName+" : App Name : ========= Invoked into reprocess ========= invoked date : "+endDate.getTime());
		Callable<String> r = new Callable<String>() {
			public String call() {
				
				List<DdpExportMissingDocs> exportDocs = commonUtil.fetchMissingDocsBasedOnAppName(appName,startDate,endDate);
	        	if (exportDocs == null || exportDocs.size() == 0) {
	        		logger.info("DdpAutoLivRuleSchedulerJob.reprocesRuleJob() - DdpExportMissingDocs are empty. For Application Name : "+appName);
	        		return "failed";
	        	}
	        	
	        	DdpExportRule ddpExportRule = commonUtil.getExportRuleById(exportDocs.get(0).getMisExpRuleId());
	        	
	        	if (ddpExportRule == null || ddpExportRule.getExpStatus() != 0) {
	        		logger.info("DdpAutoLivRuleSchedulerJob.reprocesRuleJob() - DdpExportRule is null or status is not equal to zero. For Application Name : "+appName);
	        		return "failed";
	        	}
	        	IDfSession session = null;
	        	boolean isCompleted = false;
	        	try {
	        		commonUtil.addExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
	        		List<DdpRuleDetail> ruleDetails = commonUtil.getMatchedSchdIDAndRuleDetailForExport(ddpExportRule.getExpSchedulerId().getSchId());
	        		
	        		if (ruleDetails == null || ruleDetails.size() == 0) {
	        			logger.info("AppName :  "+appName+". DdpBackUpdDocumentProcess.runGeneralSchedulerJob() - No DdpRuleDetail found for the rule id : "+ddpExportRule.getExpRuleId());
	        			return "failed" ;
	        		}
	        		
	        		session = ddpDFCClientComponent.beginSession();
	        		if (session == null) {
	        			logger.info("DdpAutoLivRuleSchedulerJob.reprocesRuleJob() - IDfSession is not created. AppName : "+appName);
	 	        		return "failed";
	        		}
	        		Calendar currentDate = GregorianCalendar.getInstance();
	        		isCompleted = ddpBackUpDocumentProcess.runGeneralSchedulerJob(ddpExportRule, exportDocs, endDate, 3, appName, session, startDate,currentDate,ruleDetails,null);
	        	} catch (Exception ex) {
	        		logger.error("DdpAutoLivRuleSchedulerJob.reprocesRuleJob() - Unable to reprocess the rule. "+appName, ex);
	        		return "failed";
	        	} finally {
	        		if (session != null)
	        			ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
	        		if (ddpExportRule != null)
 	        			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
	        	}
	        	
	        	return (isCompleted ? "compelted":"failed");
	   	 		
	         }
		};
		

	     	ExecutorService executor = Executors.newCachedThreadPool();
	     	Future<String> future = executor.submit(r);
	        try {
                //print the return value of Future, notice the output delay in console
                // because Future.get() waits for task to get completed
                	logger.info(new Date()+ ": DdpAutoLivRuleSchedulerJob result :"+future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
	     	executor.shutdown();
			
	}


	@Override
	public void runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String jobNumbers) {
		
		Calendar currentDate = GregorianCalendar.getInstance(); 
		if (ddpScheduler != null) {
			
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
   	      
   	        try {
		    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    		 Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    		 
		    		 jdfb.setTargetObject(this);
			         jdfb.setTargetMethod("runOnDemandService");
			         jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),4,fromDate, toDate,currentDate,jobNumbers,null,null,scheduler});
			         jdfb.setName("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			         jdfb.afterPropertiesSet();
			         JobDetail jd = (JobDetail)jdfb.getObject();
			         
			         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
				     trigger.setName("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setGroup("OnDemandJobNumber-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setStartTime(new Date());
				     trigger.setEndTime(null);
				     trigger.setRepeatCount(0);
				     trigger.setRepeatInterval(4000L);
			        
			         scheduler.scheduleJob(jd, trigger);
			     	 scheduler.start();
			     	 
   	        	} catch (Exception ex) {
   	        		logger.error("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedJobNumber() Error occurried while running the ondemand service", ex);
   	        	}
			
		}
		
	}


	@Override
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String consignmentIDs) {
		Calendar currentDate = GregorianCalendar.getInstance(); 
		if (ddpScheduler != null) {
			
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
   	 			return ;
   	 		}
   	      
   	        try {
		    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    		 Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandCosIds : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    		 
		    		 jdfb.setTargetObject(this);
			         jdfb.setTargetMethod("runOnDemandService");
			         jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),5,fromDate, toDate,currentDate,null,consignmentIDs,null,scheduler});
			         jdfb.setName("OnDemandCosIds : "+ddpScheduler.getSchId()+"_"+(new Date()));
			         jdfb.afterPropertiesSet();
			         JobDetail jd = (JobDetail)jdfb.getObject();
			         
			         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
				     trigger.setName("OnDemandCosIds : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setGroup("OnDemandCosIds-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setStartTime(new Date());
				     trigger.setEndTime(null);
				     trigger.setRepeatCount(0);
				     trigger.setRepeatInterval(4000L);
			        
			         scheduler.scheduleJob(jd, trigger);
			     	 scheduler.start();
			     	 
   	        	} catch (Exception ex) {
   	        		logger.error("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() Error occurried while running the ondemand service", ex);
   	        	}
			
		}
	}


	@Override
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String docRefs) {
		
		Calendar currentDate = GregorianCalendar.getInstance(); 
		if (ddpScheduler != null) {
			
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
   	 			return ;
   	 		}
   	      
   	        try {
		    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    		 Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    		 
		    		 jdfb.setTargetObject(this);
			         jdfb.setTargetMethod("runOnDemandService");
			         jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),6,fromDate, toDate,currentDate,null,null,docRefs,scheduler});
			         jdfb.setName("OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
			         jdfb.afterPropertiesSet();
			         JobDetail jd = (JobDetail)jdfb.getObject();
			         
			         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
				     trigger.setName("OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setGroup("OnDemandDocRefs-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
				     trigger.setStartTime(new Date());
				     trigger.setEndTime(null);
				     trigger.setRepeatCount(0);
				     trigger.setRepeatInterval(4000L);
			        
			         scheduler.scheduleJob(jd, trigger);
			     	 scheduler.start();
			     	 
   	        	} catch (Exception ex) {
   	        		logger.error("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedDocRef() Error occurried while running the ondemand service", ex);
   	        	}
			
		}
		
	}


	@Override
	public File runOnDemandRulForReports(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate,String typeOfStatus) {
		
		File generatedReport = null;

	        try {
//	    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
//	    		 Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
//	    		 
//	    		 jdfb.setTargetObject(this);
//		         jdfb.setTargetMethod("generateReport");
//		         jdfb.setArguments(new Object[]{ddpScheduler,fromDate, toDate,scheduler});
//		         jdfb.setName("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
//		         jdfb.afterPropertiesSet();
//		         JobDetail jd = (JobDetail)jdfb.getObject();
//		         
//		         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
//			     trigger.setName("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
//			     trigger.setGroup("OnDemandReports-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
//			     trigger.setStartTime(new Date());
//			     trigger.setEndTime(null);
//			     trigger.setRepeatCount(0);
//			     trigger.setRepeatInterval(4000L);
//		        
//		         scheduler.scheduleJob(jd, trigger);
//		     	 scheduler.start();
	        		generatedReport = this.generateReport(ddpScheduler, fromDate, toDate, null,typeOfStatus);
	        	} catch (Exception ex) {
	        		logger.error("DdpAutoLivRuleSchedulerJob.runOnDemandRuleBasedDocRef() Error occurried while running the ondemand service", ex);
	        	}
		return generatedReport;
		
	}
	
	/**
	 * 
	 * @param ddpScheduler
	 * @param fromDate
	 * @param toDate
	 * @param scheduler
	 */
	public File generateReport(DdpScheduler ddpScheduler,Calendar fromDate, Calendar toDate,Scheduler scheduler,String typeOfStatus) {
		
		File generatedReport = null;
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
			generatedReport = commonUtil.generateReports(ddpScheduler, fromDate, toDate,1,typeOfStatus);
		} catch (Exception ex) {
			logger.error("DdpAutoLivRuleSchedulerJob.generateReport() - Unable to generate reports", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpAutoLivRuleSchedulerJob.generateReport() - Unable to shutdown the scheduler", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
		}
		return generatedReport;
		
	}
}
