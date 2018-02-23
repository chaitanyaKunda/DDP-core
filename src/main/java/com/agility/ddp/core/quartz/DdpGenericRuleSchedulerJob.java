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
public class DdpGenericRuleSchedulerJob extends QuartzJobBean implements
		 DdpSchedulerJob {

	private static final Logger logger = LoggerFactory.getLogger(DdpGenericRuleSchedulerJob.class);
	
	//@Autowired
	//Environment env;
	
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
		
		logger.info("DdpGenericRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpGenericRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpGenericRuleSchedulerJob.executeInternal() executed successfully.");
		

	}
	
	
	/**
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
		
		logger.info("DdpGenericRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
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
				logger.error("DdpGenericRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj)  unable to generate reports for scheduler id :"+ddpScheduler.getSchId(), ex);
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
		
		logger.info("DdpGenericRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpGenericRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return;
		}
				
		logger.info("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
		//TODO: if the type is the properties need to change the code.
		//UI configuration this process will be executed.
		if (ddpScheduler.getSchRuleCategory() != null) {
		
		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = applicationProperties.getQuartzScheduler("GenericScheduler-_"+ddpScheduler.getSchId()+"_"+(new Date()));
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("generalSchedulerJob");
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
			     trigger.setRepeatInterval(10000L);
			        
			    
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	    	} catch (Exception ex) {
	     		logger.error("AppName : "+ddpScheduler.getSchRuleCategory().trim()+".DdpGenericRuleSchedulerJob.initiateSchedulerJob() Error occurried while running the ondemand service", ex);
	    	}
		}else {
    			logger.info("DdpGenericRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) : Rule Category is empty for scheduler id : "+ddpScheduler.getSchId());
    		
    	}
     
	
		logger.info("DdpGenericRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
	}
	
	/**
	 * Method used used for executing the general scheduler job.
	 * @param ddpScheduler
	 * @param currentDate
	 * @param appName
	 * @param presentDate
	 * @param scheduler
	 */
	public void generalSchedulerJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName,Calendar presentDate,Scheduler scheduler) {
		
		try {
			logger.info("AppName : "+appName+".DdpGenericRuleSchedulerJob.generalSchedulerJob(Object[] ddpSchedulerObj) is invoked.");
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
			ddpBackUpDocumentProcess.executeJob(ddpScheduler, currentDate, appName,presentDate);
			logger.info("AppName : "+appName+".DdpGenericRuleSchedulerJob.generalSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
		} catch (Exception ex) {
			logger.error("DdpGenericRuleSchedulerJob.generalSchedulerJob() - unable to execute the ddp scheduler : "+ddpScheduler.getSchId(),ex);
		} finally {
			if (scheduler != null)
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpGenericRuleSchedulerJob.generalSchedulerJob() - unable to  shutdown the scheduler",e);
				}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
		}
		
	}

	@Override
	public void runOnDemandRuleJob(final DdpScheduler ddpScheduler,
			final Calendar fromDate,final Calendar toDate) {
		
		logger.info("DdpGenericRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleJob() Error occurried while running the ondemand service", ex);
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
			int typeOfService,Calendar fromDate,Calendar toDate,Calendar currentDate,String jobNumber,String consignmentID,String docRef,Scheduler scheduler) {
		
		logger.info("AppName : "+appName+". DdpGenericRuleSchedulerJob.runOnDemanService() - thread invoked successfully");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
			if (typeOfService == 2)
				ddpBackUpDocumentProcess.processSchedulerJob(ddpScheduler, ddpExportRule,appName, typeOfService, fromDate, toDate,currentDate);
			else 
				ddpBackUpDocumentProcess.onDemandProcessSchedulerJob(ddpScheduler, ddpExportRule, appName, typeOfService, fromDate, toDate, currentDate, jobNumber, consignmentID, docRef);
		} catch (Exception ex) {
			logger.error("DdpGenericRuleSchedulerJob.runOnDemandService() - unable to execute the onDemand for ddp sheduler id : "+ddpScheduler.getSchId(),ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpGenericRuleSchedulerJob.runOnDemandService() - unable to shutdown the scheduler for ddp sheduler id : "+ddpScheduler.getSchId(),e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
		}
	}
	
	/**
	 * Method used for running the reprocess rule job.
	 * 
	 */
	public void reprocesRuleJob(final String appName,final Calendar currentDate,final Calendar startDate) {
		
	
		logger.info("DdpGenericRuleSchedulerJob.reprocesRuleJob() - AppName : "+appName+" : App Name : ========= Invoked into reprocess ========= invoked date : "+currentDate.getTime());
		Callable<String> r = new Callable<String>() {
			public String call() {
 	        	
 	        	List<DdpExportMissingDocs> exportDocs = commonUtil.fetchMissingDocsBasedOnAppName(appName,startDate,currentDate);
 	        	if (exportDocs == null || exportDocs.size() == 0) {
 	        		logger.info("DdpGenericRuleSchedulerJob.reprocesRuleJob() - DdpExportMissingDocs are empty");
 	        		return "failed";
 	        	}
 	        	
 	        	DdpExportRule ddpExportRule = commonUtil.getExportRuleById(exportDocs.get(0).getMisExpRuleId());
 	        	
 	        	if (ddpExportRule == null || ddpExportRule.getExpStatus() != 0) {
 	        		logger.info("DdpGenericRuleSchedulerJob.reprocesRuleJob() - DdpExportRule is null or status is not equal to zero");
 	        		return "failed";
 	        	}
 	        	IDfSession session = null;
 	        	boolean isTaskCompleted = false;
 	        	try {
 	        		commonUtil.addExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
 	        		List<DdpRuleDetail> ruleDetails = commonUtil.getMatchedSchdIDAndRuleDetailForExport(ddpExportRule.getExpSchedulerId().getSchId());
	        		
	        		if (ruleDetails == null || ruleDetails.size() == 0) {
	        			logger.info("AppName :  "+appName+". DdpGenericRuleSchedulerJob.reprocesRuleJob() - No DdpRuleDetail found for the rule id : "+ddpExportRule.getExpRuleId());
	        			return "failed";
	        		}
 	        		session = ddpDFCClientComponent.beginSession();
 	        		if (session == null) {
 	        			logger.info("DdpGenericRuleSchedulerJob.reprocesRuleJob() - IDfSession is not created.");
 	 	        		return "failed";
 	        		}
 	        		Calendar presentDate = GregorianCalendar.getInstance();
 	        		isTaskCompleted = ddpBackUpDocumentProcess.runGeneralSchedulerJob(ddpExportRule, exportDocs, currentDate, 3, appName, session, startDate,presentDate,ruleDetails,null);
 	        	} catch (Exception ex) {
 	        		logger.error("DdpGenericRuleSchedulerJob.reprocesRuleJob() - Unable to reprocess the rule", ex);
 	        	} finally {
 	        		if (session != null)
 	        			ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
 	        		if (ddpExportRule != null)
 	        			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
 	        	}
	   	 		
 	        	return (isTaskCompleted?"completed":"failed");
 	         }
  		};

  		ExecutorService executor = Executors.newCachedThreadPool();
     	Future<String> future = executor.submit(r);
        try {
            //print the return value of Future, notice the output delay in console
            // because Future.get() waits for task to get completed
            	logger.info(new Date()+ ": DdpGenericRuleSchedulerJob result :"+future.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
     	executor.shutdown();
	}

	@Override
	public void runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String jobNumbers) {
		
		logger.info("DdpGenericRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+ddpScheduler.getSchRuleCategory());
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleBasedJobNumber() Error occurried while running the ondemand service", ex);
	        }
			
		}
		
	}

	@Override
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String consignmentIDs) {
		
		
		logger.info("DdpGenericRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+ddpScheduler.getSchRuleCategory());
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandConsignmentID : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),5,fromDate, toDate,currentDate,null,consignmentIDs,null,scheduler});
			     jdfb.setName("OnDemandConsignmentID : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemandConsignmentID : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemandConsignmentID-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() Error occurried while running the ondemand service", ex);
	        }
			
		}
		
	}

	@Override
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String docRefs) {
		
		logger.info("DdpGenericRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandDocRefer : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),6,fromDate, toDate,currentDate,null,null,docRefs,scheduler});
			     jdfb.setName("OnDemandDocRefer : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemandDocRefer : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemandDocRefer-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpGenericRuleSchedulerJob.runOnDemandRuleBasedDocRef() Error occurried while running the ondemand service", ex);
	        }
			
		}
		
		
	}

	@Override
	public File runOnDemandRulForReports(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate,String typeOfStatus) {
		
		File generatedReport = null;
		try {
//   		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
//   		 Scheduler scheduler = applicationProperties.getQuartzScheduler("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
//   		 
//   		 jdfb.setTargetObject(this);
//	         jdfb.setTargetMethod("generateReport");
//	         jdfb.setArguments(new Object[]{ddpScheduler,fromDate, toDate,scheduler});
//	         jdfb.setName("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
//	         jdfb.afterPropertiesSet();
//	         JobDetail jd = (JobDetail)jdfb.getObject();
//	         
//	         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
//		     trigger.setName("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
//		     trigger.setGroup("OnDemandReports-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
//		     trigger.setStartTime(new Date());
//		     trigger.setEndTime(null);
//		     trigger.setRepeatCount(0);
//		     trigger.setRepeatInterval(4000L);
//	        
//	         scheduler.scheduleJob(jd, trigger);
//	     	 scheduler.start();
			generatedReport = this.generateReport(ddpScheduler, fromDate, toDate, null,typeOfStatus);
       	} catch (Exception ex) {
       		logger.error("DdpGenericRuleSchedulerJob.runOnDemandRulForReports() Error occurried while running the ondemand service", ex);
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
			logger.error("DdpGenericRuleSchedulerJob.generateReport() - Unable to generate reports. For DdpScheduler Id : "+ddpScheduler.getSchId(), ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpGenericRuleSchedulerJob.generateReport() - Unable to shutdown the scheduler. For DdpScheduler Id : "+ddpScheduler.getSchId(), e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
		}
		return generatedReport;
		
	}

}
