/**
 * 
 */
package com.agility.ddp.core.quartz;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
//@PropertySource({"file:///E:/DDPConfig/export.properties"})
public class DdpNOVRuleSchedulerJob extends QuartzJobBean implements
		DdpSchedulerJob {

	private static final Logger logger = LoggerFactory.getLogger(DdpNOVRuleSchedulerJob.class);
	
	@Autowired
	private ApplicationProperties applicationProperties;
	
	@Autowired
	private DdpBackUpDocumentProcess ddpBackUpDocumentProcess;
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent; 
	
	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		
		logger.info("DdpNOVRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpNOVRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpNOVRuleSchedulerJob.executeInternal() executed successfully.");
		

	}
	
	/**
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
		
		logger.info("DdpNOVRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
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
				logger.error("DdpNOVRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj)  unable to generate reports for scheduler id :"+ddpScheduler.getSchId(), ex);
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
		
		logger.info("DdpNOVRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpNOVRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return;
		}
				
		logger.info("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
		//TODO: if the type is the properties need to change the code.
		//UI configuration this process will be executed.
		
		try {
	    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
	    	 Scheduler scheduler = applicationProperties.getQuartzScheduler("DdpNovSchedulerJob-_"+ddpScheduler.getSchId()+"_"+(new Date()));;
	    	 
	    	 jdfb.setTargetObject(this);
		     jdfb.setTargetMethod("generalShedulerJob");
		     jdfb.setArguments(new Object[]{ddpScheduler,currentDate,ddpScheduler.getSchRuleCategory().trim(),currentDate,scheduler});
		     jdfb.setName("NOVProcess : "+ddpScheduler.getSchId()+"_"+(new Date()));
		     jdfb.afterPropertiesSet();
		     JobDetail jd = (JobDetail)jdfb.getObject();
		     
		     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
		     trigger.setName(ddpScheduler.getSchRuleCategory().trim()+"Process : "+ddpScheduler.getSchId()+"_"+(new Date()));
		     trigger.setGroup(ddpScheduler.getSchRuleCategory().trim()+"Process-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
		     trigger.setStartTime(new Date());
		     trigger.setEndTime(null);
		     trigger.setRepeatCount(0);
		     trigger.setRepeatInterval(8000L);
		        
		     
		     scheduler.scheduleJob(jd, trigger);
		   	 scheduler.start();
      	} catch (Exception ex) {
       		logger.error("DdpNOVRuleSchedulerJob.initiateSchedulerJob() Error occurried while running the ondemand service", ex);
       }
	
		logger.info("DdpNOVRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
	}
	
	/**
	 * 
	 * @param ddpScheduler
	 * @param currentDate
	 * @param appName
	 * @param presentDate
	 * @param scheduler
	 */
	public void generalShedulerJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName,Calendar presentDate,Scheduler scheduler) {
		
		logger.info("DdpNOVRuleSchedulerJob.generalShedulerJob(Object[] ddpSchedulerObj) is invoked.");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
			ddpBackUpDocumentProcess.executeJob(ddpScheduler, currentDate, appName,presentDate);
		} catch (Exception ex) {
			logger.error("DdpNOVRuleSchedulerJob.generalShedulerJob() - Unable to execute the BackUpDocumentProcess.execute job", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpNOVRuleSchedulerJob.generalShedulerJob() - Unable to shutdown the scheduler thread", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
		}
		logger.info("DdpNOVRuleSchedulerJob.generalShedulerJob(Object[] ddpSchedulerObj) executed successfully.");
		
	}

	@Override
	public void runOnDemandRuleJob(final DdpScheduler ddpScheduler,
			final Calendar fromDate,final Calendar toDate) {
		
		Calendar currentDate = GregorianCalendar.getInstance(); 
		logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) method invoked.");
		
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler =  applicationProperties.getQuartzScheduler("DdpNovSchedulerOnDemand-_"+ddpScheduler.getSchId()+"_"+(new Date()));;
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),2,fromDate, toDate,currentDate,null,null,null,scheduler});
			     jdfb.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName(ddpScheduler.getSchRuleCategory().trim()+"OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup(ddpScheduler.getSchRuleCategory().trim()+"OnDemand-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("DdpNOVRuleSchedulerJob.runOnDemandRuleJob() Error occurried while running the ondemand service", ex);
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
		
		logger.info("DdpNOVRuleSchedulerJob.runOnDemanService() - thread invoked successfully");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
			if (typeOfService == 2)
				ddpBackUpDocumentProcess.processSchedulerJob(ddpScheduler, ddpExportRule,appName, typeOfService, fromDate, toDate,currentDate);
			else 
				ddpBackUpDocumentProcess.onDemandProcessSchedulerJob(ddpScheduler, ddpExportRule, appName, typeOfService, fromDate, toDate, currentDate, jobNumbers, consignmentIDs, docRefs);
			
		} catch (Exception ex) {
			logger.error("DdpNOVRuleSchedulerJob.runOnDemanService() - Unable to execute the processSchedulerJob ", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpNOVRuleSchedulerJob.runOnDemanService() - Unable to shutdown the Scheduler. ", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
		}
	}
	
	
	
	public void reprocesRuleJob(final String appName,final Calendar currentDate,final Calendar startDate) {
		
	
		logger.info("DdpNOVRuleSchedulerJob.reprocesRuleJob() - AppName : "+appName+" : App Name : ========= Invoked into reprocess ========= invoked date : "+currentDate.getTime());
		Runnable r = new Runnable() {
 	         public void run() {
 	        	
 	        	List<DdpExportMissingDocs> exportDocs = commonUtil.fetchMissingDocsBasedOnAppName(appName,startDate,currentDate);
 	        	if (exportDocs == null || exportDocs.size() == 0) {
 	        		logger.info("DdpNOVRuleSchedulerJob.reprocesRuleJob() - DdpExportMissingDocs are empty");
 	        		return;
 	        	}
 	        	
 	        	DdpExportRule ddpExportRule = commonUtil.getExportRuleById(exportDocs.get(0).getMisExpRuleId());
 	        	
 	        	if (ddpExportRule == null || ddpExportRule.getExpStatus() != 0) {
 	        		logger.info("DdpNOVRuleSchedulerJob.reprocesRuleJob() - DdpExportRule is null or status is not equal to zero");
 	        		return;
 	        	}
 	        	IDfSession session = null;
 	        	try {
 	        		commonUtil.addExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
 	        		List<DdpRuleDetail> ruleDetails = commonUtil.getMatchedSchdIDAndRuleDetailForExport(ddpExportRule.getExpSchedulerId().getSchId());
	        		
	        		if (ruleDetails == null || ruleDetails.size() == 0) {
	        			logger.info("AppName :  "+appName+". DdpBackUpdDocumentProcess.runGeneralSchedulerJob() - No DdpRuleDetail found for the rule id : "+ddpExportRule.getExpRuleId());
	        			return ;
	        		}
 	        		session = ddpDFCClientComponent.beginSession();
 	        		if (session == null) {
 	        			logger.info("DdpNOVRuleSchedulerJob.reprocesRuleJob() - IDfSession is not created.");
 	 	        		return;
 	        		}
 	        		Calendar presentDate = GregorianCalendar.getInstance();
 	        		ddpBackUpDocumentProcess.runGeneralSchedulerJob(ddpExportRule, exportDocs, currentDate, 3, appName, session, startDate,presentDate,ruleDetails,null);
 	        	} catch (Exception ex) {
 	        		logger.error("DdpNOVRuleSchedulerJob.reprocesRuleJob() - Unable to reprocess the rule", ex);
 	        	} finally {
 	        		if (session != null)
 	        			ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
 	        		if (ddpExportRule != null)
 	        			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
 	        	}
	   	 		
 	         }
  		};

 	     	ExecutorService executor = Executors.newCachedThreadPool();
 	     	executor.submit(r);
	}

	@Override
	public void runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String jobNumbers) {
		
		Calendar currentDate = GregorianCalendar.getInstance(); 
		logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler) method invoked.");
		
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler =  applicationProperties.getQuartzScheduler("OnDemandJobNumber-_"+ddpScheduler.getSchId()+"_"+(new Date()));;
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),4,fromDate, toDate,currentDate,jobNumbers,null,null,scheduler});
			     jdfb.setName("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName(ddpScheduler.getSchRuleCategory().trim()+"OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup(ddpScheduler.getSchRuleCategory().trim()+"OnDemandJobNumber-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedJobNumber() Error occurried while running the ondemand service", ex);
	        }
		}
			
		
	}

	@Override
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String consignmentIDs) {
		
		Calendar currentDate = GregorianCalendar.getInstance(); 
		logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler) method invoked.");
		
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler =  applicationProperties.getQuartzScheduler("OnDemandConsId-_"+ddpScheduler.getSchId()+"_"+(new Date()));;
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),5,fromDate, toDate,currentDate,null,consignmentIDs,null,scheduler});
			     jdfb.setName("OnDemandConsId : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName(ddpScheduler.getSchRuleCategory().trim()+"OnDemandConsId : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup(ddpScheduler.getSchRuleCategory().trim()+"OnDemandConsId-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() Error occurried while running the ondemand service", ex);
	        }
		}
		
	}

	@Override
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String docRefs) {
		
		Calendar currentDate = GregorianCalendar.getInstance(); 
		logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler) method invoked.");
		
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler =  applicationProperties.getQuartzScheduler("OnDemandDocRefs-_"+ddpScheduler.getSchId()+"_"+(new Date()));;
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),6,fromDate, toDate,currentDate,null,null,docRefs,scheduler});
			     jdfb.setName("OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName(ddpScheduler.getSchRuleCategory().trim()+"OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup(ddpScheduler.getSchRuleCategory().trim()+"OnDemandDocRefs-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("DdpNOVRuleSchedulerJob.runOnDemandRuleBasedDocRef() Error occurried while running the ondemand service", ex);
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
       		logger.error("DdpNOVRuleSchedulerJob.runOnDemandRulForReports() Error occurried while running the ondemand service", ex);
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
			logger.error("DdpNOVRuleSchedulerJob.generateReport() - Unable to generate reports", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpNOVRuleSchedulerJob.generateReport() - Unable to shutdown the scheduler", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
		}
		return generatedReport;
	}
	}
