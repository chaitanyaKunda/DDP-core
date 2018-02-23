/**
 * 
 */
package com.agility.ddp.core.quartz;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import com.agility.ddp.core.components.DdpDFCClientComponent;
import com.agility.ddp.core.components.DdpTransferFactory;
import com.agility.ddp.core.components.DdpTransferObject;
import com.agility.ddp.core.task.DdpSchedulerJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.agility.ddp.data.domain.DdpCompressionSetup;
import com.agility.ddp.data.domain.DdpDocnameConv;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportMissingDocsService;
import com.agility.ddp.data.domain.DdpExportQuery;
import com.agility.ddp.data.domain.DdpExportQueryUi;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.agility.ddp.data.domain.DdpExportSuccessReportService;
import com.agility.ddp.data.domain.DdpNotification;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfSession;

/**
 * @author DGuntha
 *
 */
@Configuration
public class DdpNSNRuleSchedulerJob extends QuartzJobBean implements DdpSchedulerJob {

	private static final Logger logger = LoggerFactory.getLogger(DdpNSNRuleSchedulerJob.class);
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private TaskUtil taskUtil;
	
	@Autowired
	private DdpExportMissingDocsService ddpExportMissingDocsService;
	
	@Autowired
	private DdpTransferFactory ddpTransferFactory;
	
	@Autowired
	private DdpExportSuccessReportService ddpExportSuccessReportService;
	
	
	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRuleJob(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar)
	 */
	@Override
	public void runOnDemandRuleJob(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate) {
		
		logger.info("DdpNSNRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleJob() Error occurried while running the ondemand service", ex);
	        }
			
		}


	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#reprocesRuleJob(java.lang.String, java.util.Calendar, java.util.Calendar)
	 */
	@Override
	public void reprocesRuleJob(String appName, Calendar endDate, Calendar startDate) {
		
		endDate.add(Calendar.HOUR, -2);
		
		logger.info("DdpNSNRuleSchedulerJob.reprocesRuleJob() - AppName : "+appName+" : App Name : ========= Invoked into reprocess ========= invoked date : "+endDate.getTime());
		Runnable r = new Runnable() {
 	         public void run() {
 	        	
 	        	List<DdpExportMissingDocs> exportDocs = commonUtil.fetchMissingDocsBasedOnAppName(appName,startDate,endDate);
 	        	if (exportDocs == null || exportDocs.size() == 0) {
 	        		logger.info("DdpNSNRuleSchedulerJob.reprocesRuleJob() - DdpExportMissingDocs are empty");
 	        		return;
 	        	}
 	        	
 	        	DdpExportRule ddpExportRule = commonUtil.getExportRuleById(exportDocs.get(0).getMisExpRuleId());
 	        	
 	        	if (ddpExportRule == null || ddpExportRule.getExpStatus() != 0) {
 	        		logger.info("DdpNSNRuleSchedulerJob.reprocesRuleJob() - DdpExportRule is null or status is not equal to zero");
 	        		return;
 	        	}
 	        	IDfSession session = null;
 	        	try {
 	        		commonUtil.addExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
 	        		
 	        		session = ddpDFCClientComponent.beginSession();
 	        		if (session == null) {
 	        			logger.info("DdpNSNRuleSchedulerJob.reprocesRuleJob() - IDfSession is not created.");
 	 	        		return;
 	        		}
 	        		Calendar presentDate = GregorianCalendar.getInstance();
 	        		performSegregationProcess( ddpExportRule, appName, 3, startDate, endDate, presentDate, exportDocs, session);
 	        	} catch (Exception ex) {
 	        		logger.error("DdpNSNRuleSchedulerJob.reprocesRuleJob() - Unable to reprocess the rule", ex);
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

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRuleBasedJobNumber(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar, java.lang.String)
	 */
	@Override
	public void runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate,
			String jobNumbers) {
		
		logger.info("DdpNSNRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+ddpScheduler.getSchRuleCategory());
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleBasedJobNumber() Error occurried while running the ondemand service", ex);
	        }
			
		}
	

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRuleBasedConsignmentId(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar, java.lang.String)
	 */
	@Override
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate,
			String consignmentIDs) {
		
		logger.info("DdpNSNRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+ddpScheduler.getSchRuleCategory());
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemandConsignmentID : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() Error occurried while running the ondemand service", ex);
	        }
			
		}

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRuleBasedDocRef(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar, java.lang.String)
	 */
	@Override
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate,
			String docRefs) {
		
		logger.info("DdpNSNRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemandDocRefer : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpNSNRuleSchedulerJob.runOnDemandRuleBasedDocRef() Error occurried while running the ondemand service", ex);
	        }
			
		}


	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRulForReports(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar, java.lang.String)
	 */
	@Override
	public File runOnDemandRulForReports(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate,
			String typeOfStatus) {
		
		File generatedReport = null;
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
			generatedReport = commonUtil.generateReports(ddpScheduler, fromDate, toDate,1,typeOfStatus);
		} catch (Exception ex) {
			logger.error("DdpNSNRuleSchedulerJob.generateReport() - Unable to generate reports. For DdpScheduler Id : "+ddpScheduler.getSchId(), ex);
		} finally {
//			if (scheduler != null) {
//				try {
//					scheduler.shutdown();
//				} catch (SchedulerException e) {
//					logger.error("DdpNSNRuleSchedulerJob.generateReport() - Unable to shutdown the scheduler. For DdpScheduler Id : "+ddpScheduler.getSchId(), e);
//				}
//			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
		}
		return generatedReport;
	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#initiateSchedulerReport(java.lang.Object[])
	 */
	@Override
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
		

		logger.info("DdpNSNRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
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
				logger.error("DdpNSNRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj)  unable to generate reports for scheduler id :"+ddpScheduler.getSchId(), ex);
			} finally {
				commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-SchedulerReport :"+ddpScheduler.getSchId());
				if (generatedReportFolder != null)
					SchedulerJobUtil.deleteFolder(generatedReportFolder);
			}
		}

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#initiateSchedulerJob(java.lang.Object[])
	 */
	@Override
	public void initiateSchedulerJob(Object[] ddpSchedulerObj) {
		
		logger.info("DdpNSNRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpNSNRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return;
		}
				
		logger.info("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
		//if the type is the properties need to change the code.
		//UI configuration this process will be executed.
		if (ddpScheduler.getSchRuleCategory() != null) {
		
		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = env.getQuartzScheduler("NSNScheduler-_"+ddpScheduler.getSchId()+"_"+(new Date()));
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("generalSchedulerJob");
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpScheduler.getSchRuleCategory().trim(),currentDate,scheduler});
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
	     		logger.error("AppName : "+ddpScheduler.getSchRuleCategory().trim()+".DdpNSNRuleSchedulerJob.initiateSchedulerJob() Error occurried while running the ondemand service", ex);
	    	}
		}else {
    			logger.info("DdpNSNRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) : Rule Category is empty for scheduler id : "+ddpScheduler.getSchId());
    		
    	}
		

	}

	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		
		logger.info("DdpNSNRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpNSNRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpNSNRuleSchedulerJob.executeInternal() executed successfully.");
		

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
		
		logger.info("DdpNSNRuleSchedulerJob.runOnDemanService() - thread invoked successfully");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
			//if (typeOfService == 2)
				processSchedulerJob(ddpScheduler, ddpExportRule,appName, typeOfService, fromDate, toDate,currentDate,jobNumbers, consignmentIDs, docRefs);
			//else
				//ddpBackUpDocumentProcess.onDemandProcessSchedulerJob(ddpScheduler, ddpExportRule, appName, typeOfService, fromDate, toDate, currentDate, jobNumbers, consignmentIDs, docRefs);
		} catch (Exception ex) {
			logger.error("DdpNSNRuleSchedulerJob.runOnDemanService() - Unable to execute the processSchedulerJob in DdpBackUpDocumentProcess", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpNSNRuleSchedulerJob.runOnDemanService() - Unable to shutdown the scheduler thread", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
		}
	}
	
	/**
	 * Method used used for executing the general scheduler job.
	 * @param ddpScheduler
	 * @param currentDate
	 * @param appName
	 * @param executionDate
	 * @param scheduler
	 */
	public void generalSchedulerJob(DdpScheduler ddpScheduler,String appName,Calendar executionDate,Scheduler scheduler) {
		
		try {
			logger.info("AppName : "+appName+".DdpNSNRuleSchedulerJob.generalSchedulerJob(Object[] ddpSchedulerObj) is invoked.");
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
			executeJob(ddpScheduler, appName,executionDate);
			logger.info("AppName : "+appName+".DdpNSNRuleSchedulerJob.generalSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
		} catch (Exception ex) {
			logger.error("DdpNSNRuleSchedulerJob.generalSchedulerJob() - unable to execute the ddp scheduler : "+ddpScheduler.getSchId(),ex);
		} finally {
			if (scheduler != null)
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpNSNRuleSchedulerJob.generalSchedulerJob() - unable to  shutdown the scheduler",e);
				}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
		}
		
	}

	/**
	 * Method used for executing the scheduler job based on the application name.
	 * 
	 * @param ddpScheduler
	 * @param currentDate
	 * @param appName
	 * @return
	 */
	public boolean executeJob(DdpScheduler ddpScheduler,String appName,Calendar executionDate) {
		
		logger.info("AppName : "+appName+". DDPNSNRuleSchedulerJob.executeJob() - Invoked sucessfully.");
		boolean isJobExecuted = false;
		Calendar startDate = null;
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),appName);
		
		if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
			logger.info("AppName : "+appName+". DDPNSNRuleSchedulerJob.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+appName);
			return isJobExecuted;
		}
		DdpScheduler scheduler = ddpSchedulerService.findDdpScheduler(ddpScheduler.getSchId());
		
		if (scheduler == null || scheduler.getSchStatus() != 0) {
			logger.info("AppName : "+appName+". DDPNSNRuleSchedulerJob.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+appName);
			return isJobExecuted;
		}
		
		boolean isSchLastRunStatus = false;
		Calendar endDate = GregorianCalendar.getInstance();
		
		if (scheduler.getSchLastSuccessRun() == null) { 
			startDate = ddpExportRule.getExpActivationDate();
			isSchLastRunStatus = true;
		} else 
			startDate = scheduler.getSchLastSuccessRun();
		
		//Used for checking delay count based on the configuration.
		if (scheduler.getSchDelayCount() != null) {
			
			String dateFreq = SchedulerJobUtil.getSchFreq(scheduler.getSchCronExpressions());
			if (dateFreq.equalsIgnoreCase("monthly")) {
				endDate.add(Calendar.MONTH,-scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.MONTH, -scheduler.getSchDelayCount());
			} else if (dateFreq.equalsIgnoreCase("weekly")) {
				endDate.add(Calendar.WEEK_OF_YEAR, -scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.WEEK_OF_YEAR, -scheduler.getSchDelayCount());
			} else if (dateFreq.equalsIgnoreCase("daily")) {
				endDate.add(Calendar.DAY_OF_YEAR, -scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.DAY_OF_YEAR, -scheduler.getSchDelayCount());
			} else if (dateFreq.equalsIgnoreCase("hourly")) {
				endDate.add(Calendar.HOUR_OF_DAY, -scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.HOUR_OF_DAY, -scheduler.getSchDelayCount());
			}
		}
		
		if (endDate.getTime().before(startDate.getTime())) {
			logger.info("DDPNSNRuleSchedulerJob.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - Start Date : "+startDate.getTime()+" is greater than end date : "+endDate.getTime()+" . For Application : "+appName);
			return isJobExecuted;
		}
		
		isJobExecuted = processSchedulerJob( ddpScheduler, ddpExportRule, appName, 1, startDate, endDate,executionDate,null,null,null);
		
		return isJobExecuted;
	}
	
	/**
	 * Method used for executed the process Scheduler job
	 * 
	 * @param ddpScheduler
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService
	 * @param startDate
	 * @param endDate
	 * @param executionDate
	 * @return
	 */
	public boolean processSchedulerJob(DdpScheduler ddpScheduler,DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,
			Calendar endDate,Calendar executionDate,String jobNumbers,String consignmentIDs,String docRefs) {
		
		boolean isJobExecuted = false;
		IDfSession session = null;
		
		
		try {
			//To avoid time creation of session for each iteration of loop
	    	 session = ddpDFCClientComponent.beginSession();
	    	
	    	 if (session == null) {
	     		logger.info("AppName: "+appName+". DdpBackDocumentProcess.processSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] Unable create the DFCClient Session for this Scheduler ID."+ddpExportRule.getExpSchedulerId().getSchId());
	     		 sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "session", null);
	     		taskUtil.sendMailByDevelopers("For export module,In the DDPNSNRuleSchedulerJob.processSchedulerJob() - Application Name : "+appName+" & type of service : "+typeOfService+". Unable to connection the dfc session. So please restart the jboss server.",  "export - dfc session issue.");
	     		return isJobExecuted;
	     	}
	    	 
			String query = constructDqlQuery(appName, typeOfService, jobNumbers, consignmentIDs, docRefs, startDate, endDate,ddpScheduler,ddpExportRule);
			
			if (query != null) {
				
				List<DdpExportMissingDocs> exportDocsList = new ArrayList<DdpExportMissingDocs>();
				constructExportDocuments(appName, typeOfService, session, query, ddpExportRule,exportDocsList);
				if (exportDocsList.size() == 0) {
					sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "empty", null);
					return isJobExecuted;
				}
				
				if (typeOfService == 1) {
					//create missing records & insert into DB.
					exportDocsList = insertExportMissingDocs(exportDocsList);
					// Update ddp scheduler last success run time.
					updateDdpSchedulerTime(endDate, executionDate, ddpScheduler, typeOfService, appName);
					logger.info("DDPNSNRuleSchedulerJob.processSchedulerJob() : Total records inserted into ddp_missing_export_docs : "+exportDocsList.size());
				}
				
				isJobExecuted = performSegregationProcess( ddpExportRule, appName, typeOfService, startDate, endDate, executionDate, exportDocsList, session);
				
			} else {
				// notification alert to the dev team. Unable to construct the query.
				taskUtil.sendMailByDevelopers("For export module,In the DDPNSNRuleSchedulerJob.processSchedulerJob() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n Unable to consrtuct DQL Query",  "export issue for appName :"+appName);
			}
		} catch(Exception ex) {
			logger.error("DDPNSNRuleSchedulerJob.processSchedulerJob() - Unable to execute for appName : "+appName+" : type of Service : "+typeOfService, ex);
			taskUtil.sendMailByDevelopers("For export module,In the DDPNSNRuleSchedulerJob.processSchedulerJob() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n"+ex.getMessage(),  "export issue for appName :"+appName);
		} finally {
			if (session != null)
				ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
		}
		return isJobExecuted;
	}
	
	/**
	 * Method used for segregation process for transferring the files.
	 * 
	 * @param ddpScheduler
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService
	 * @param startDate
	 * @param endDate
	 * @param executionDate
	 * @param exportDocsList
	 * @param session
	 * @return
	 */
	private boolean performSegregationProcess(DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,
			Calendar endDate,Calendar executionDate,List<DdpExportMissingDocs> exportDocsList,IDfSession session) {
		
		boolean isJobExecuted = false;
		// String -> can be Consignment ID or Job Number.
		Map<String,List<DdpExportMissingDocs>> exportDocsMap = new HashMap<String,List<DdpExportMissingDocs>>();
		
		if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("jobNumber")) {
			groupExportDocsByJobNumber(exportDocsMap, exportDocsList);	
		} else if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("consignmentID")) {
			groupExportDocsByCosignmentID(exportDocsMap, exportDocsList);
		}
		
		DdpCommunicationSetup commSetup = ddpExportRule.getExpCommunicationId();
		List<DdpTransferObject> transferObjects = ddpTransferFactory.constructTransferObject(commSetup);
		DdpTransferObject ddpTransferObject = null;
		
		for (DdpTransferObject transferObject : transferObjects) {
			if (transferObject.isConnected()) {
				ddpTransferObject = transferObject;
				break;
			}
		}
		
		if (ddpTransferObject == null) {
			taskUtil.sendMailByDevelopers("For export module,In the DDPNSNRuleSchedulerJob.performSegregationProcess() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n Unable to Transfer data to customer.\r\n Please inform to customer to check the FTP/SFTP/UNC protocols.",  "export issue for appName :"+appName);
			sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "transferissue", null);
			return isJobExecuted;
		}
		
		isJobExecuted = performTransfer(ddpExportRule, appName, typeOfService, startDate, endDate, executionDate, exportDocsMap, session, transferObjects, ddpTransferObject);
		
		return isJobExecuted;
		
	}
	
	/**
	 * Method used for transferring the file.
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService
	 * @param startDate
	 * @param endDate
	 * @param executionDate
	 * @param exportDocsList
	 * @param uniqueNo
	 * @param session
	 * @return
	 */
	private boolean performTransfer(DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,
			Calendar endDate,Calendar executionDate,Map<String,List<DdpExportMissingDocs>> exportDocsMap,
			IDfSession session,List<DdpTransferObject> transferObjects,DdpTransferObject ddpTransferObject) {
		
		boolean isFileTransfered = false;
		logger.info("AppName : "+appName+". Invoked successfully for performTranser");
		DdpCompressionSetup compressionSetup =  ddpExportRule.getExpCompressionId();
		
		if (compressionSetup != null) {
			
			String tempFilePath = env.getProperty("ddp.export.folder");
			// creating the folder in the local for downloading purpose.
		  	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
			String tempFolderPath = tempFilePath+"/temp_"+appName+"_"+typeOfService+"_"+ddpExportRule.getExpRuleId() +"_" + dateFormat.format(new Date());
			File tmpFile = FileUtils.createFolder(tempFolderPath);
			try  {
				String sourceFolder = tempFolderPath + "/"+appName+"_" +dateFormat.format(executionDate.getTime());
				File sourceFolderFile = FileUtils.createFolder(sourceFolder);
				File endSourceFolderFile = sourceFolderFile;
				
				StringBuffer mailBody = new StringBuffer();
				DdpDocnameConv namingConvention = ddpExportRule.getExpDocnameConvId();
				List<DdpExportMissingDocs> exportedList = new ArrayList<DdpExportMissingDocs>();
				List<DdpExportMissingDocs> unExportedList = new ArrayList<DdpExportMissingDocs>();
				List<DdpExportSuccessReport> exportedReports = new ArrayList<DdpExportSuccessReport>();
				
				//Key will be based Batching criteria.
				for (String uniqueNo : exportDocsMap.keySet()) {
							
					List<DdpExportMissingDocs> exportMissingDocsList = exportDocsMap.get(uniqueNo);
					//merge
					if (compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("merge")) {
						
						performMergeOperation(appName, typeOfService, exportMissingDocsList, mailBody, namingConvention, sourceFolder, ddpTransferObject, exportedList, unExportedList, exportedReports, session);		
						
					} else {
						for (DdpExportMissingDocs docs : exportMissingDocsList) {
							downloadFileIntoLocal(appName, typeOfService, docs, mailBody, namingConvention, sourceFolder, ddpTransferObject, exportedList, unExportedList, exportedReports, session);
						}
					}
				}
				
	
				if (exportedList.size() > 0 && compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("exportAsZip")) {
					
					String zipPath = tempFolderPath +"/ZIP_"+appName;
					File zipPathFile = FileUtils.createFolder(zipPath);
					FileUtils.zipFiles(zipPath+"/"+appName+"_"+typeOfService+"_" +dateFormat.format(endDate.getTime())+".zip", sourceFolder);
					endSourceFolderFile = zipPathFile;
					
				}
				
				performFileTransferProcess(ddpExportRule,appName, typeOfService, startDate, endDate, executionDate, endSourceFolderFile, transferObjects, exportedList, unExportedList, exportedReports,tempFolderPath,mailBody);
				isFileTransfered = true;
			} catch (Exception ex) {
				logger.error("AppName : "+appName+" : DdpNSNRuleSchedulerJob.performTransfer()  Unable to transfer file for Rule ID : "+ddpExportRule.getExpRuleId(), ex);
				taskUtil.sendMailByDevelopers("For export module,In the DDPNSNRuleSchedulerJob.performTransfer() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n Unable to Transfer data to customer.\r\n Please check the code.\r\nError Message : "+ex.getMessage(),  "export issue for appName :"+appName);
			} finally {
				if (tmpFile != null)
					SchedulerJobUtil.deleteFolder(tmpFile);		
			}
			
		} else {
			logger.info("AppName : "+appName+". CompressionSetup is null for the Rule ID :"+ddpExportRule.getExpRuleId()+" & Sch ID : "+ddpExportRule.getExpSchedulerId().getSchId());
		}
		
		return isFileTransfered;
	}
	
	/**
	 * Method used for file transferring.
	 * 
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService
	 * @param startDate
	 * @param endDate
	 * @param executionDate
	 * @param endSourceFolderFile
	 * @param transferObjects
	 * @param exportedList
	 * @param unExportedList
	 * @param exportedReports
	 */
	private void performFileTransferProcess(DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,Calendar endDate,Calendar executionDate,File endSourceFolderFile,
			List<DdpTransferObject> transferObjects,List<DdpExportMissingDocs> exportedList,List<DdpExportMissingDocs> unExportedList,List<DdpExportSuccessReport> exportedReports,
			String tempFolderPath,StringBuffer mailBody) {
		
		//To know how may transfer object are transfered.
		boolean isProcessed = false;
		List<DdpTransferObject> transferList = new ArrayList<DdpTransferObject>();
		
		if (exportedList.size() > 0)
			isProcessed = transferFilesIntoSourceLocation(ddpExportRule,appName, typeOfService, transferObjects, transferList, startDate, endDate, executionDate, endSourceFolderFile);
		
		 if ((typeOfService == 1 || typeOfService == 3) && isProcessed) {
			 changeStatusForMissingDocs(exportedList, executionDate);
			 createExportSuccessReprots(exportedReports);
		 }
		 
		 if (!isProcessed) {
			 unExportedList.addAll(exportedList);
			 //exportedList = new ArrayList<>();
		 }
		 
		 Date endTime = new Date();
		 
		 if (exportedList.size() > 0 && isProcessed)
			 sendMailForSuccessExport( ddpExportRule, appName, typeOfService, endDate, startDate, mailBody, tempFolderPath,executionDate,endTime,transferList,null);
		 if (unExportedList.size() > 0)
			 sendMailForMissingDocuments(unExportedList,ddpExportRule,tempFolderPath, appName, typeOfService, endDate, startDate,executionDate,endTime,null);
	}
	
	/**
	 * Method used for transferring the files into source location.
	 * 
	 * @param appName
	 * @param typeOfService
	 * @param transferObjects
	 * @param transferList
	 * @param startDate
	 * @param endDate
	 * @param executionDate
	 * @param endSourceFolderFile
	 * @return
	 */
	private boolean transferFilesIntoSourceLocation(DdpExportRule ddpExportRule,String appName,Integer typeOfService,List<DdpTransferObject> transferObjects, List<DdpTransferObject> transferList,
			Calendar startDate,Calendar endDate, Calendar executionDate,File endSourceFolderFile) {
		
		boolean isProcessed  = false;
		
		for (DdpTransferObject transferObject : transferObjects) {
				
			if (transferObject.isConnected()) {
					
				boolean isTransferd = ddpTransferFactory.transferFilesUsingProtocol(transferObject, endSourceFolderFile,null,appName,startDate,endDate,"ruleByQuery."+typeOfService);
					
				if (isTransferd) {
					isProcessed = true;
					transferList.add(transferObject);
				}
					
					if (!isTransferd) {
						List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
						list.add(transferObject);
						sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "transferissue", null);
					}
				} else {
					List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
					list.add(transferObject);
					sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "transferissue", null);
				}
			}
			
		logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.performTransfer() - Transfered files status : "+isProcessed);
		
		return isProcessed;
	}
	
	/**
	 * Method used for changing the Status of missing documents.
	 * 
	 * @param exportedRecords
	 * @param currentDate
	 */
	private void changeStatusForMissingDocs(List<DdpExportMissingDocs> exportedRecords,Calendar currentDate) {
		
		for(DdpExportMissingDocs docs : exportedRecords) {
			//System.out.println("Documents id : "+docs.getMisId());
			docs.setMisStatus(1);
			docs.setMisLastProcessedDate(currentDate);
			ddpExportMissingDocsService.updateDdpExportMissingDocs(docs);
		}
	}
	
	/**
	 * 
	 * @param reports
	 */
	private void createExportSuccessReprots(List<DdpExportSuccessReport> reports) {
		
		for (DdpExportSuccessReport report : reports) {
			ddpExportSuccessReportService.saveDdpExportSuccessReport(report);
		}
	}
	
	/**
	 * Method used for downloading the files as single.
	 * 
	 * @param appName
	 * @param typeOfService
	 * @param exportDocs
	 * @param mailBody
	 * @param namingConvention
	 * @param sourceFolder
	 * @param ddpTransferObject
	 * @param exportedList
	 * @param unExportedList
	 * @param exportedReports
	 * @param session
	 * @return
	 */
	private boolean downloadFileIntoLocal(String appName,Integer typeOfService, DdpExportMissingDocs exportDocs, StringBuffer mailBody,
			DdpDocnameConv namingConvention,String sourceFolder,DdpTransferObject ddpTransferObject,List<DdpExportMissingDocs> exportedList,
			List<DdpExportMissingDocs> unExportedList,List<DdpExportSuccessReport> exportedReports,IDfSession session) {
		
		boolean isDownload = false;
		try {
			
			List<String> fileNameList = new ArrayList<String>();
			commonUtil.readFileNamesFromLocation(sourceFolder, ddpTransferObject, fileNameList);
			Map<String, String> loadNumberMap = commonUtil.getLoadNumberMapDetails(namingConvention, exportDocs, appName, env);
			String fileName = getDocumentFileName( namingConvention, exportDocs,fileNameList,loadNumberMap);
			isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(exportDocs.getMisRObjectId(), sourceFolder, fileName, session);
			if (!isDownload) {
				logger.info("AppName : "+appName+" DdpNSNRuleSchedulerJob.downloadFileIntoLocal() - Unable to download the documents robjectid is "+exportDocs.getMisRObjectId()+" : AppName : "+appName);
				unExportedList.add(exportDocs);
			} else {
				List<DdpExportMissingDocs> exportDocsList = new ArrayList<>();
				exportDocsList.add(exportDocs);
				constructMailBody(mailBody, appName, sourceFolder, fileName,exportDocsList, typeOfService, exportedReports);
				exportedList.add(exportDocs);
			}
		} catch (Exception ex) {
			unExportedList.add(exportDocs);
			isDownload = false;
			logger.error("DdpNSNRuleSchedulerJob.downloadFileIntoLocal() - Unable to download for r_object_id : "+exportDocs.getMisRObjectId()+" : appName : "+appName, ex);
		}
		
		return isDownload;
	}
	
	/**
	 * Method used for constructing the Mail Body.
	 * 
	 * @param mailBody
	 * @param appName
	 * @param sourceFolder
	 * @param fileName
	 * @param exportDocs
	 * @param typeOfService
	 * @param exportedReports
	 */
	private void constructMailBody(StringBuffer mailBody,String appName,String sourceFolder,String fileName,List<DdpExportMissingDocs> exportDocs,int typeOfService,List<DdpExportSuccessReport> exportedReports) {
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
		mailBody.append(format.format(new Date())+",");
		format.applyLocalizedPattern("HH:mm:ss");
		Long fileSize = FileUtils.findSizeofFileInKB(sourceFolder+"/"+fileName);
		
		List<String> jobNumbers = new ArrayList<String>();
		for (DdpExportMissingDocs docs : exportDocs) {
			if (!jobNumbers.contains(docs.getMisJobNumber()))
				jobNumbers.add(docs.getMisJobNumber());
		}
		mailBody.append(format.format(new Date())+",receive,success,"+fileSize+",00:00:01,"+exportDocs.get(0).getMisConsignmentId()+","+commonUtil.joinString(jobNumbers, "", "", "-")+","+fileName+"\n");
		if (typeOfService == 1 || typeOfService == 3) {
			exportedReports.addAll(commonUtil.constructExportReportDomainObject(exportDocs, fileSize, fileName, typeOfService));
		}
	}
	/**
	 * Method used for performing the merge operation of the list of exprot documents.
	 * 
	 * @param appName
	 * @param exportMissingDocsList
	 * @param fileNameList
	 * @param namingConvention
	 * @param session
	 * @param loadNumberMap
	 * @return
	 */
	private boolean performMergeOperation (String appName,Integer typeOfService, List<DdpExportMissingDocs> exportMissingDocsList, StringBuffer mailBody,
			DdpDocnameConv namingConvention,String sourceFolder,DdpTransferObject ddpTransferObject,List<DdpExportMissingDocs> exportedList,
			List<DdpExportMissingDocs> unExportedList,List<DdpExportSuccessReport> exportedReports,IDfSession session) {
		
		boolean isDownload = false;
		try {
			
			List<String> fileNameList = new ArrayList<String>();
			commonUtil.readFileNamesFromLocation(sourceFolder, ddpTransferObject, fileNameList);
			
			List<String> rObjectIDsList = getRobjectIDSequenceOrder(appName, exportMissingDocsList);
			Map<String, String> loadNumberMap = commonUtil.getLoadNumberMapDetails(namingConvention, exportMissingDocsList.get(0), appName, env);
			String fileName = getDocumentFileName( namingConvention, exportMissingDocsList.get(0),fileNameList,loadNumberMap);
			
			String mergeID = commonUtil.performMergeOperation(rObjectIDsList, fileName, env.getProperty("ddp.vdLocation"), env.getProperty("ddp.objectType"), session,appName);
			
			if (mergeID == null) {
				logger.info("AppName : "+appName+" DdpNSNRuleSchedulerJob.performMergeOperation() - Unable to merge the documents");
				unExportedList.addAll(exportMissingDocsList);
				taskUtil.sendMailByDevelopers("For export module,In the DDPNSNRuleSchedulerJob.performMergeOperation() - Application Name : "+appName+", & type of service : "+typeOfService+".\r\n Unable to Meger the document.\r\n Please check the Content Transfermation Server.",  "export merging issue for appName :"+appName);
				
			} else {
				 isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(mergeID, sourceFolder, fileName, session);
				if (!isDownload) {
					logger.info("AppName : "+appName+" DdpBackUpDocumentProcess.performMergeOperation() - Unable to download the documents robjectid is "+mergeID);
					unExportedList.addAll(exportMissingDocsList);
				} else {
					constructMailBody(mailBody, appName, sourceFolder, fileName, exportMissingDocsList, typeOfService, exportedReports);
					exportedList.addAll(exportMissingDocsList);
				}
			}
		} catch (Exception ex) {
			unExportedList.addAll(exportMissingDocsList);
			isDownload = false;
			logger.error("DdpNSNRuleSchedulerJob.performMergeOperation() - Unable to merger the document for appName : "+appName , ex);
		}
		
		return isDownload;
	}
	/**
	 * Method used for getting the document file name.
	 * 
	 * @param namingConvention
	 * @param exportDocs
	 * @param fileNameList
	 * @param loadNumberMap
	 * @return
	 */
	private String getDocumentFileName (DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs, List<String> fileNameList,Map<String, String> loadNumberMap) {
		
		String fileName = null;
		
		//check the name is already exists in the destination (FTP & Local) locations. if exists then add sequence number.
		 if  (namingConvention != null) 
			 fileName = commonUtil.getDocumentName(exportDocs.getMisRobjectName(), namingConvention, exportDocs, exportDocs.getMisEntryType(),fileNameList,loadNumberMap);
		 else 
			 fileName = commonUtil.getFileNameWithCopy(exportDocs.getMisRobjectName(), fileNameList, null, null);
		 
		 return fileName;
	}
	

	/**
	 * Method used for getting the List of r_object_id based on the sequence order.
	 * 
	 * @param appName
	 * @param exportDocs
	 * @return
	 */
	private List<String> getRobjectIDSequenceOrder(String appName, List<DdpExportMissingDocs> exportDocs) {
	
		Map<String,List<String>> documentsMap = new HashMap<String,List<String>>();
		Map<Integer,String> sequenceOrder = new TreeMap<Integer,String>();
		List<String> rObjecIDList = new LinkedList<String>();
		
		String sequence = env.getProperty("export.rule."+appName+".sequenceOrder");
		//Adding the sequence order.
		if (sequence != null) {
			String[] sequenceArray = sequence.split(",");
			for (String sequenceStr : sequenceArray) {
				if (sequenceStr.contains(":")) {
					String[] order = sequenceStr.split(":");
					if (order != null && !sequenceOrder.containsValue(order[0].trim())) {
						try {
							Integer sequenceNumber = Integer.parseInt(order[1].trim());
							sequenceOrder.put(sequenceNumber, order[0].trim());
						} catch (Exception ex) {
							logger.error("DdpNSNRuleSchedulerJob.getRobjectIDSequenceOrder() - Unable to parse the sequence number for appName :"+appName, ex);
						}
					}
						
				}
			}
		}
		
		//Group robject ids into one collection using document type.
		for (DdpExportMissingDocs ddpExportMissingDocs : exportDocs) {
			
			if (!documentsMap.containsKey(ddpExportMissingDocs.getMisDocType())) {
				List<String> rObjectList = new ArrayList<String>();
				rObjectList.add(ddpExportMissingDocs.getMisRObjectId());
				documentsMap.put(ddpExportMissingDocs.getMisDocType(), rObjectList);
			} else {
				List<String> rObjectList = documentsMap.get(ddpExportMissingDocs.getMisDocType());
				rObjectList.add(ddpExportMissingDocs.getMisRObjectId());
				documentsMap.put(ddpExportMissingDocs.getMisDocType(), rObjectList);
				
			}
		}
		
		//fetching the r_object_id in to the list.
		if (sequenceOrder.size() > 0) {
			
			List<Integer> keys = new ArrayList<Integer>(sequenceOrder.keySet());
			Collections.sort(keys);
			for (Integer key : keys) {
				List<String> rObjectIDs =  documentsMap.get(sequenceOrder.get(key));
				if (rObjectIDs != null)
					rObjecIDList.addAll(rObjectIDs);
			}
		} else {
			for (String key : documentsMap.keySet()) {
				List<String> rObjectList = documentsMap.get(key);
				rObjecIDList.addAll(rObjectList);
			}
		}
		
		return rObjecIDList;
	}
	
	/**
	 * Method used for groping the export documents by Job number.
	 * 
	 * @param exportDocsMap
	 * @param exportDocsList
	 */
	private void groupExportDocsByJobNumber(Map<String,List<DdpExportMissingDocs>> exportDocsMap,List<DdpExportMissingDocs> exportDocsList) {
		
		for (DdpExportMissingDocs docs: exportDocsList) {
			
			if (!exportDocsMap.containsKey(docs.getMisJobNumber())) {
				List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
				list.add(docs);
				exportDocsMap.put(docs.getMisJobNumber(), list);
			} else {
				List<DdpExportMissingDocs> list = exportDocsMap.get(docs.getMisJobNumber());
				list.add(docs);
				exportDocsMap.put(docs.getMisJobNumber(), list);
			}
		}
	}

	/**
	 * Method used for grouping the Export document by Cosignment ID>
	 * 
	 * @param exportDocsMap
	 * @param exportDocsList
	 */
	private void groupExportDocsByCosignmentID(Map<String,List<DdpExportMissingDocs>> exportDocsMap,List<DdpExportMissingDocs> exportDocsList) {
		
		for (DdpExportMissingDocs docs: exportDocsList) {
			
			if (!exportDocsMap.containsKey(docs.getMisConsignmentId())) {
				List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
				list.add(docs);
				exportDocsMap.put(docs.getMisConsignmentId(), list);
			} else {
				List<DdpExportMissingDocs> list = exportDocsMap.get(docs.getMisConsignmentId());
				list.add(docs);
				exportDocsMap.put(docs.getMisConsignmentId(), list);
			}
		}
	}

	
	/**
	 * Method used for constructing the DQL query for provided details.
	 * 
	 * @param appName
	 * @param typeOfService
	 * @param jobNumbers
	 * @param consignmentIDs
	 * @param docRefs
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private String constructDqlQuery(String appName,int typeOfService,String jobNumbers,String consignmentIDs,String docRefs,
			Calendar startDate,Calendar endDate,DdpScheduler ddpScheduler,DdpExportRule exportRule) {
		
		logger.info("DDPNSNRuleSchedulerJob.constructDqlQuery() - invoked into method for appName :"+appName);
		String query = getQueryFromProperties(appName, ddpScheduler, exportRule, typeOfService);
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		
    	String strStartDate = dateFormat.format(startDate.getTime());
    	String strEndDate = dateFormat.format(endDate.getTime());
        	
		if (query != null) {
			
			if (query.contains("%%CLINETID%%")) {
				String clientIds = env.getProperty("export.rule."+appName+".clientids");
				if (clientIds != null) {
				 query =	query.replaceAll("%%CLINETID%%", commonUtil.joinString(clientIds.split(","), "'", "'", ","));
				}
			}
			
			if (query.contains("%%AGL_DOC_TYPES%%")) {
				
				String agl_control_docs = "";
				String nonRatedDocs = env.getProperty("export.rule."+appName+".documentTypes.nonrated");
				String ratedDocs = env.getProperty("export.rule."+appName+".documentTypes.rated");
				
				if (nonRatedDocs != null && !nonRatedDocs.isEmpty()) {
					agl_control_docs = "(agl_control_doc_type in ("+commonUtil.joinString(nonRatedDocs.split(","), "'", "'", ",")+") and agl_is_rated = 0)";
				} 
				
				if (agl_control_docs.isEmpty() && ratedDocs != null && !ratedDocs.isEmpty()) {
					agl_control_docs = "(agl_control_doc_type in ("+commonUtil.joinString(ratedDocs.split(","), "'", "'", ",")+") and agl_is_rated = 1)";
				} else if ( ratedDocs != null && !ratedDocs.isEmpty()) {
					agl_control_docs += " or (agl_control_doc_type in ("+commonUtil.joinString(ratedDocs.split(","), "'", "'", ",")+") and agl_is_rated = 1)";
				}
				
				query = query.replaceAll("%%AGL_DOC_TYPES%%", agl_control_docs);
						
			}
			
			query = query.replaceAll("%%startdate%%", strStartDate);
			query = query.replaceAll("%%enddate%%", strEndDate);
			
			
			if (typeOfService == 4 && !jobNumbers.isEmpty()) 
				query = query.replaceAll("%%JOBNUMBER%%",commonUtil.joinString(jobNumbers.split(","), "'", "'", ","));
			else if (typeOfService == 5 && !consignmentIDs.isEmpty())
				query = query.replaceAll("%%CONSIGNMENTID%%",commonUtil.joinString(consignmentIDs.split(","), "'", "'", ","));
			else if (typeOfService == 6 && !docRefs.isEmpty())
				query = query.replaceAll("%%DOCREFS%%",commonUtil.joinString(docRefs.split(","), "'", "'", ","));
			
		}
		logger.info("DDPNSNRuleSchedulerJob.constructDqlQuery() - End of method : Query to execute is :"+query);
		
		return query;
	}
	
	/**
	 * Method used for getting the query from properties.
	 * 
	 * @param appName
	 * @param ddpScheduler
	 * @param exportRule
	 * @param typeOfService
	 * @return
	 */
	private String getQueryFromProperties(String appName,DdpScheduler ddpScheduler,DdpExportRule exportRule,Integer typeOfService) {

		String query = "";
		if (ddpScheduler.getSchQuerySource() == null || ddpScheduler.getSchQuerySource().intValue() == 0) {
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6)
				query = env.getProperty("export.rule."+appName+".customQuery."+env.getProperty(typeOfService+""));
			else 
				query = env.getProperty("export.rule."+appName+".customQuery");
			
		} else if (ddpScheduler.getSchQuerySource().intValue() == 1) {
			
			List<DdpExportQueryUi> queryUis = commonUtil.getExportQueryUiByRuleID(exportRule.getExpRuleId());
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6)
				query = commonUtil.constructQueryWithExportQueryUIs(queryUis,"export.rule."+appName+".customQuery."+env.getProperty(typeOfService+"")+".byUI","export.rule.customQuery."+env.getProperty(typeOfService+"")+".byUI");
			else 
				query = commonUtil.constructQueryWithExportQueryUIs(queryUis,"export.rule."+appName+".customQuery.byUI","export.rule.customQuery.byUI");
			
		} else {
			
			List<DdpExportQuery> queryList = commonUtil.getExportQueryByRuleID(exportRule.getExpRuleId());
			if (queryList != null && queryList.size() > 0)
				query = commonUtil.constructQueryFromTXT(queryList.get(0).getExqQuery(), appName);
			
		}
		
		if (!query.toLowerCase().contains("%%PRIMARYDOCTYPE%%".toLowerCase())) 
			query += " and (agl_control_doc_type in (%%PRIMARYDOCTYPE%%))";
		
		if (typeOfService.intValue() == 4 && !query.toLowerCase().contains("%%JOBNUMBER%%".toLowerCase()))
			query += " and any agl_job_numbers in (%%JOBNUMBER%%)";
		
		if (typeOfService.intValue() == 5 && !query.toLowerCase().contains("%%CONSIGNMENTID%%".toLowerCase()))
			query += " and agl_consignment_id in (%%CONSIGNMENTID%%)";
		
		if (typeOfService.intValue() == 6 && !query.toLowerCase().contains("%%DOCREFS%%".toLowerCase()))
			query += " and agl_doc_ref in (%%DOCREFS%%)";
		
		if (!query.toLowerCase().contains("'%%startdate%%".toLowerCase()) && !query.toLowerCase().contains("'%%enddate%%".toLowerCase()))
			query += " and ( agl_creation_date between Date('%%startdate%%','dd/mm/yyyy HH:mi:ss') and Date('%%enddate%%','dd/mm/yyyy HH:mi:ss'))";
	
		return query;
	}
	
	/**
	 * Method used for constructing the export documents.
	 * 
	 * @param appName
	 * @param typeOfService
	 * @param session
	 * @param query
	 * @param ddpExportRule
	 * @return
	 */
	private List<DdpExportMissingDocs> constructExportDocuments(String appName,int typeOfService,IDfSession session,String query,DdpExportRule ddpExportRule,List<DdpExportMissingDocs> exportDocsList) {
		
		
		IDfCollection idfCollection = commonUtil.getIDFCollectionDetails(query,session);
		
		if (idfCollection != null) {
			exportDocsList.addAll(commonUtil.constructMissingDocs(idfCollection,appName,ddpExportRule.getExpRuleId()));
		}
		
		return exportDocsList;

	}
	
	/**
	 * Method used for inserting export missing document details.
	 * 
	 * @param exportDocs
	 * @return
	 */
	private List<DdpExportMissingDocs> insertExportMissingDocs(List<DdpExportMissingDocs> exportDocs) {
		
		for (DdpExportMissingDocs  doc : exportDocs) {
			ddpExportMissingDocsService.saveDdpExportMissingDocs(doc);
		}
		
		return exportDocs;
	}
	
	/*
	 * Method used for updating the scheduler time.
	 */
	private void updateDdpSchedulerTime(Calendar endDate,Calendar presentDate,DdpScheduler ddpScheduler,int typeOfService,String appName) {
		
		try {
			if (typeOfService == 1) {
				ddpScheduler.setSchLastRun(presentDate);
				ddpScheduler.setSchLastSuccessRun(endDate);
				ddpSchedulerService.updateDdpScheduler(ddpScheduler);
			}
		} catch (Exception ex) {
			logger.error("AppName : "+appName+". DDPNSNRuleSchedulerJob.updateDdpSchedulerTime() - Error ", ex);
		}
	}
	
	/*======================================================= Mail Templates =======================================*/
	/**
	 * Method ussed for sending mails based on content.
	 * 
	 * @param exportRule
	 * @param appName
	 * @param typeOfService
	 * @param endDate
	 * @param startDate
	 * @param executionDate
	 * @param content
	 * @param dynamicValues
	 */
	private void sendMailBasedOnContent(DdpExportRule exportRule,String appName,int typeOfService,Calendar endDate,Calendar startDate,Calendar executionDate,String content,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		
		try {
			
			DdpCommunicationSetup commSetup = exportRule.getExpCommunicationId();
			boolean isFTPType =	commSetup.getCmsCommunicationProtocol().equalsIgnoreCase("FTP")? true : false;
			String destinationLocation = "";
			String hostName = "";
			
			String destinationLocation2 = null;
			String hostName2 = null;
			
			String destinationLocation3 = null;
			String hostName3 = null;
			
			if (isFTPType) {
				
				DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId()); 
					//need to split with ftp:// so it 6.
					destinationLocation = commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation());				
					hostName = commonUtil.getHostName(ftpDetails.getCftFtpLocation());
			} else {
				DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId());
				destinationLocation = uncDetails.getCunUncPath();
				hostName = commonUtil.getHostName(uncDetails.getCunUncPath());
			}
			
			if (commSetup.getCmsCommunicationProtocol2() != null && !commSetup.getCmsCommunicationProtocol2().isEmpty()) {
				isFTPType =	commSetup.getCmsCommunicationProtocol2().equalsIgnoreCase("FTP")? true : false;
				if (isFTPType) {
					
					DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2()); 
						//need to split with ftp:// so it 6.
						destinationLocation2 = commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation());				
						hostName2 = commonUtil.getHostName(ftpDetails.getCftFtpLocation());
				} else {
					DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2());
					destinationLocation2 = uncDetails.getCunUncPath();
					hostName2 = commonUtil.getHostName(uncDetails.getCunUncPath());
				}
			}
			
			if (commSetup.getCmsCommunicationProtocol3() != null && !commSetup.getCmsCommunicationProtocol3().isEmpty()) {
				isFTPType =	commSetup.getCmsCommunicationProtocol3().equalsIgnoreCase("FTP")? true : false;
				if (isFTPType) {
					
					DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3()); 
						//need to split with ftp:// so it 6.
						destinationLocation3 = commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation());				
						hostName3 = commonUtil.getHostName(ftpDetails.getCftFtpLocation());
				} else {
					DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3());
					destinationLocation3 = uncDetails.getCunUncPath();
					hostName3 = commonUtil.getHostName(uncDetails.getCunUncPath());
				}
			}
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByQuery."+typeOfService);			
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = env.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = env.getProperty("export.rule."+appName+".mail."+content+".subject");
			String body = env.getProperty("export.rule."+appName+".mail."+content+".body");
			//System.out.println("tableBODY : "+tableBody);
			//body = body.replace("%%TABLEDETAILS%%", tableBody);
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(endDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(executionDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%DESTIONPATH%%", destinationLocation);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			body = body.replace("%%ENDTIME%%", dateFormat.format(new Date()));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(executionDate.getTime(), new Date())+"");
			
			String transferBody = "";
			int i = 1;
			if (destinationLocation2 != null) {
				
					transferBody += "<tr><td colspan='2' align='center'><B>Transfers Location-"+(i+1)+"<B></td>"
							+ "</tr><tr><td align='center'><B>Folder</B></td><td>"+destinationLocation2+"</td></tr>"
							+ "<tr><td align='center'><B>Hosts/Mailboxes</B></td><td>"+hostName2+"</td></tr>"
							+ "<tr><td align='center'><B>Directions</B></td><td>receive</td></tr>"
							+ "<tr><td align='center'><B>Total Records Exported</B></td><td>0</td></tr>";
					i += 1; 
			}
			
			if (destinationLocation3 != null) {
				
				transferBody += "<tr><td colspan='2' align='center'><B>Transfers Location-"+(i+1)+"<B></td>"
						+ "</tr><tr><td align='center'><B>Folder</B></td><td>"+destinationLocation3+"</td></tr>"
						+ "<tr><td align='center'><B>Hosts/Mailboxes</B></td><td>"+hostName3+"</td></tr>"
						+ "<tr><td align='center'><B>Directions</B></td><td>receive</td></tr>"
						+ "<tr><td align='center'><B>Total Records Exported</B></td><td>0</td></tr>";
				
			}
			body = body.replaceAll("%%TRANSFERLOCATION%%", transferBody);
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValues+"<br/>");
			} else {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
			}
			
				taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, null);
		} catch (Exception ex) {
			logger.error("AppName : "+appName+" DdpBackUpDocumentProcess.sendMailForEmptyRecords() - Unable to send the success mail", ex);
		}
	}
	
	/**
	 * Method used for sending the mails for success export documents.
	 * 
	 * @param exportRule
	 * @param appName
	 * @param typeOfService
	 * @param currentDate
	 * @param startDate
	 * @param tableBody
	 * @param tmpFolder
	 * @param presentDate
	 * @param endTime
	 * @param ddpTransferObjects
	 * @param dynamicValues
	 */
	private void sendMailForSuccessExport(DdpExportRule exportRule,String appName,int typeOfService,Calendar currentDate,Calendar startDate,StringBuffer tableBody,String tmpFolder,Calendar presentDate,Date endTime,List<DdpTransferObject> ddpTransferObjects,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		String successFolder = tmpFolder + "/SUCCESS_FOLDER";
		File sucessFolderFile = FileUtils.createFolder(successFolder);
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			String currDate = dateFormat.format(currentDate.getTime());
			FileWriter csvFile = new FileWriter(successFolder+"/ExportedDocs-"+appName+"_"+typeOfService+"_"+currDate+".csv");
			csvFile.append("Date");
			csvFile.append(",");
			csvFile.append("Time");
			csvFile.append(",");
			csvFile.append("Directions");
			csvFile.append(",");
			csvFile.append("Status");
			csvFile.append(",");
			csvFile.append("Bytes Transfer");
			csvFile.append(",");
			csvFile.append("Transfer Time");
			csvFile.append(",");
			csvFile.append("Consignment ID");
			csvFile.append(",");
			csvFile.append("Job Number");
			csvFile.append(",");
			csvFile.append("File Name");
			csvFile.append("\n");
			csvFile.append(tableBody);
			
			csvFile.flush();
			csvFile.close();
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByQuery."+typeOfService);
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = env.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = env.getProperty("export.rule."+appName+".mail.success.subject");
			String body = env.getProperty("export.rule."+appName+".mail.success.body");
			//System.out.println("tableBODY : "+tableBody);
			//body = body.replace("%%TABLEDETAILS%%", tableBody);
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			String destLoc = (ddpTransferObjects != null && ddpTransferObjects.size() > 0) ? ddpTransferObjects.get(0).getDestLocation() : "";
			String hostName = (ddpTransferObjects != null && ddpTransferObjects.size() > 0) ? ddpTransferObjects.get(0).getHostName() : "";
			body = body.replace("%%DESTIONPATH%%", destLoc);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			body = body.replace("%%COUNT%%", FileUtils.countCharacter(tableBody.toString(), '\n')+"");
			body = body.replace("%%ENDTIME%%", dateFormat.format(endTime));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endTime)+"");
			
			String transferBody = "";
			if (ddpTransferObjects.size() > 1) {
				
				for (int i = 1; i < ddpTransferObjects.size(); i++) {
					
					DdpTransferObject transferObject = ddpTransferObjects.get(i);
					
					transferBody += "<tr><td colspan='2' align='center'><B>Transfers Location-"+(i+1)+"<B></td>"
							+ "</tr><tr><td align='center'><B>Folder</B></td><td>"+transferObject.getDestLocation()+"</td></tr>"
							+ "<tr><td align='center'><B>Hosts/Mailboxes</B></td><td>"+transferObject.getHostName()+"</td></tr>"
							+ "<tr><td align='center'><B>Directions</B></td><td>receive</td></tr>"
							+ "<tr><td align='center'><B>Total Records Exported</B></td><td>"+FileUtils.countCharacter(tableBody.toString(), '\n')+"</td></tr>";
					
				}
			}
			body = body.replaceAll("%%TRANSFERLOCATION%%", transferBody);
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValues+"<br/>");
			} else {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
			}
				taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, sucessFolderFile);
		} catch (Exception ex) {
			logger.error("AppName : "+appName+" DdpBackUpDocumentProcess-sendMailForSuccessExoprot() - Unable to send the success mail", ex);
		}
		
	}

	/**
	 * Method used for sending the mail for documents.
	 * 
	 * @param missingRecords
	 * @param exportRule
	 * @param tmpFolder
	 * @param appName
	 * @param typeOfService
	 * @param currentDate
	 * @param startDate
	 * @param presentDate
	 * @param endDate
	 * @param dynamicValues
	 */
	private void sendMailForMissingDocuments(List<DdpExportMissingDocs> missingRecords,DdpExportRule exportRule,String tmpFolder,String appName,int typeOfService,Calendar currentDate,
		Calendar startDate,Calendar presentDate,Date endDate,String dynamicValues) {
	
	DdpNotification notification = exportRule.getExpNotificationId();
	logger.info("DdpBackUpDocumentProcess-sendMailForMissingDocuments() - Mail ID : "+notification.getNotSuccessEmailAddress()+" Failure : "+notification.getNotFailureEmailAddress()+" : AppName : "+appName);
	String errorFolder = tmpFolder + "/ERROR_FOLDER";
	File errorFolderFile = FileUtils.createFolder(errorFolder);
	try {
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
		FileWriter csvFile = new FileWriter(errorFolder+"/Missing-"+appName+"_"+typeOfService+"_"+dateFormat.format(currentDate.getTime())+".csv");
		csvFile.append("Reference Number");
		csvFile.append(",");
		csvFile.append("MasterJob Number");
		csvFile.append(",");
		csvFile.append("Job Number");
		csvFile.append(",");
		csvFile.append("Consignment ID");
		csvFile.append(",");
		csvFile.append("Missing DocumentType");
		csvFile.append("\n");
		
		for (DdpExportMissingDocs export : missingRecords) {
					 
			csvFile.append(export.getMisEntryType());
			csvFile.append(",");
			csvFile.append(export.getMisMasterJob());
			csvFile.append(",");
			csvFile.append(export.getMisJobNumber());
			csvFile.append(",");
			csvFile.append(export.getMisConsignmentId());
			csvFile.append(",");
			csvFile.append(export.getMisDocType());
			csvFile.append("\n");
			
		}
		
		csvFile.flush();
		csvFile.close();
		
		String smtpAddress = env.getProperty("mail.smtpAddress");
		String toAddress = notification.getNotSuccessEmailAddress(); 
		String ccAddress = notification.getNotFailureEmailAddress();
		String fromAddress = env.getProperty("export.rule."+appName+".mail.fromAddress");
		String subject = env.getProperty("export.rule."+appName+".mail.failure.subject");
		String body = env.getProperty("export.rule."+appName+".mail.failure.body");
		
		
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
		String serviceName = env.getProperty("ruleByQuery."+typeOfService);
		
		body = body.replace("%%SERVICERUN%%", serviceName);
		body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
		body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
		body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
		body = body.replace("%%ENDTIME%%", dateFormat.format(endDate));
		body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endDate));
		
		if (toAddress == null || toAddress.length()== 0)
			toAddress = env.getProperty("mail.toAddress");
		
		if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
			body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValues+"<br/>");
		} else {
			body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
		}
		
		taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, errorFolderFile);
	} catch (IOException ex) {
		logger.error("DdpBackUpDocumentProcess - sendMailForMissingDocuments() : Unable to send the mails or create csv file", ex);
		ex.printStackTrace();
	}
	
}
	
}

