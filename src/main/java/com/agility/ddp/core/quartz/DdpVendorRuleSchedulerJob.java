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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
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
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.agility.ddp.data.domain.DdpCompressionSetup;
import com.agility.ddp.data.domain.DdpDocnameConv;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportMissingDocsService;
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
public class DdpVendorRuleSchedulerJob extends QuartzJobBean implements DdpSchedulerJob {
	
	private static final Logger logger = LoggerFactory.getLogger(DdpVendorRuleSchedulerJob.class);
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	@Autowired
	private TaskUtil taskUtil;
	
	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private ApplicationProperties env;
	
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
		
		logger.info("DdpVendorRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpVendorRuleSchedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+"NOV");
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),2,fromDate, toDate,currentDate,null,null,scheduler});
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
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpVendorRuleSchedulerJob.runOnDemandRuleJob() Error occurried while running the ondemand service", ex);
	        }
			
		}

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#reprocesRuleJob(java.lang.String, java.util.Calendar, java.util.Calendar)
	 */
	@Override
	public void reprocesRuleJob(String appName, Calendar endDate, Calendar startDate) {
		

		endDate.add(Calendar.HOUR, -2);
		
		logger.info("DdpVendorRuleSchedulerJob.reprocesRuleJob() - AppName : "+appName+" : App Name : ========= Invoked into reprocess ========= invoked date : "+endDate.getTime());
		Runnable r = new Runnable() {
 	         public void run() {
 	        	
 	        	List<DdpExportMissingDocs> exportDocs = commonUtil.fetchMissingDocsBasedOnAppName(appName,startDate,endDate);
 	        	if (exportDocs == null || exportDocs.size() == 0) {
 	        		logger.info("DdpVendorRuleSchedulerJob.reprocesRuleJob() - DdpExportMissingDocs are empty");
 	        		return;
 	        	}
 	        	
 	        	DdpExportRule ddpExportRule = commonUtil.getExportRuleById(exportDocs.get(0).getMisExpRuleId());
 	        	
 	        	if (ddpExportRule == null || ddpExportRule.getExpStatus() != 0) {
 	        		logger.info("DdpVendorRuleSchedulerJob.reprocesRuleJob() - DdpExportRule is null or status is not equal to zero");
 	        		return;
 	        	}
 	        	IDfSession session = null;
 	        	try {
 	        		commonUtil.addExportSetupThread("RuleByQuery - "+ddpExportRule.getExpSchedulerId().getSchRuleCategory()+"-Reporcess :"+ddpExportRule.getExpSchedulerId().getSchId());
 	        		
 	        		session = ddpDFCClientComponent.beginSession();
 	        		if (session == null) {
 	        			logger.info("DdpVendorRuleSchedulerJob.reprocesRuleJob() - IDfSession is not created.");
 	 	        		return;
 	        		}
 	        		Calendar presentDate = GregorianCalendar.getInstance();
 	        		performSegregationProcess( ddpExportRule, appName, 3, startDate, endDate, presentDate, exportDocs, session);
 	        	} catch (Exception ex) {
 	        		logger.error("DdpVendorRuleSchedulerJob.reprocesRuleJob() - Unable to reprocess the rule", ex);
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
			String suppilerNames) {
		
		logger.info("DdpVendorRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpVendorRuleSchedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+ddpScheduler.getSchRuleCategory());
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemandSuppilerName : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),4,fromDate, toDate,currentDate,suppilerNames,null,scheduler});
			     jdfb.setName("OnDemandSuppilerName : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemandSuppilerName : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemandSuppilerName-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
			   	 
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpVendorRuleSchedulerJob.runOnDemandRuleBasedJobNumber() Error occurried while running the ondemand service", ex);
	        }
			
		}

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRuleBasedConsignmentId(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar, java.lang.String)
	 */
	@Override
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate,
			String c2cNumbers) {
		
		logger.info("DdpVendorRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			final DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
			if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
   	 			logger.info("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpVendorRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+ddpScheduler.getSchRuleCategory());
   	 			return ;
   	 		}
			 try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     Scheduler scheduler = env.getQuartzScheduler("OnDemandC2CNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.setArguments(new Object[]{ddpScheduler,ddpExportRule, ddpScheduler.getSchRuleCategory(),5,fromDate, toDate,currentDate,null,c2cNumbers,scheduler});
			     jdfb.setName("OnDemandC2CNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemandC2CNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemandC2CNumber-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpVendorRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() Error occurried while running the ondemand service", ex);
	        }
			
		}

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#runOnDemandRuleBasedDocRef(com.agility.ddp.data.domain.DdpScheduler, java.util.Calendar, java.util.Calendar, java.lang.String)
	 */
	@Override
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler, Calendar fromDate, Calendar toDate,
			String docRefs) {
		// TODO Auto-generated method stub

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
			logger.error("DdpVendorRuleSchedulerJob.generateReport() - Unable to generate reports. For DdpScheduler Id : "+ddpScheduler.getSchId(), ex);
		} finally {
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-GenerateReports :"+ddpScheduler.getSchId());
		}
		return generatedReport;
	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.DdpSchedulerJob#initiateSchedulerReport(java.lang.Object[])
	 */
	@Override
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
	
		logger.info("DdpVendorRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
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
				logger.error("DdpVendorRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj)  unable to generate reports for scheduler id :"+ddpScheduler.getSchId(), ex);
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
		
		logger.info("DdpVendorRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked.");
		Calendar currentDate = GregorianCalendar.getInstance();
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpVendorRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return;
		}
				
		logger.info("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
		//if the type is the properties need to change the code.
		//UI configuration this process will be executed.
		if (ddpScheduler.getSchRuleCategory() != null) {
		
		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = env.getQuartzScheduler("VendorSetScheduler-_"+ddpScheduler.getSchId()+"_"+(new Date()));
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
	     		logger.error("AppName : "+ddpScheduler.getSchRuleCategory().trim()+".DdpVendorRuleSchedulerJob.initiateSchedulerJob() Error occurried while running the ondemand service", ex);
	    	}
		}else {
    			logger.info("DdpVendorRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) : Rule Category is empty for scheduler id : "+ddpScheduler.getSchId());
    		
    	}

	}

	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		
		logger.info("DdpVendorRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpVendorRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpVendorRuleSchedulerJob.executeInternal() executed successfully.");
		
	}
	
	
	public void generalSchedulerJob(DdpScheduler ddpScheduler,String appName,Calendar executionDate,Scheduler scheduler) {
		
		try {
			logger.info("AppName : "+appName+".DdpVendorRuleSchedulerJob.generalSchedulerJob(Object[] ddpSchedulerObj) is invoked.");
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
			executeJob(ddpScheduler, appName,executionDate);
			logger.info("AppName : "+appName+".DdpVendorRuleSchedulerJob.generalSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
		} catch (Exception ex) {
			logger.error("DdpVendorRuleSchedulerJob.generalSchedulerJob() - unable to execute the ddp scheduler : "+ddpScheduler.getSchId(),ex);
		} finally {
			if (scheduler != null)
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpVendorRuleSchedulerJob.generalSchedulerJob() - unable to  shutdown the scheduler",e);
				}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-General :"+ddpScheduler.getSchId());
		}
		
	}
	
	public void runOnDemandService(DdpScheduler ddpScheduler,DdpExportRule ddpExportRule,String appName,
			int typeOfService,Calendar fromDate,Calendar toDate,Calendar currentDate,String suppilerNames,String c2cNumbers,Scheduler scheduler) {
		
		logger.info("DdpVendorRuleSchedulerJob.runOnDemanService() - thread invoked successfully");
		try {
			commonUtil.addExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
			//if (typeOfService == 2)
				processSchedulerJob(ddpScheduler, ddpExportRule,appName, typeOfService, fromDate, toDate,currentDate,suppilerNames, c2cNumbers);
			//else
				//ddpBackUpDocumentProcess.onDemandProcessSchedulerJob(ddpScheduler, ddpExportRule, appName, typeOfService, fromDate, toDate, currentDate, jobNumbers, consignmentIDs, docRefs);
		} catch (Exception ex) {
			logger.error("DdpVendorRuleSchedulerJob.runOnDemanService() - Unable to execute the processSchedulerJob in DdpBackUpDocumentProcess", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpVendorRuleSchedulerJob.runOnDemanService() - Unable to shutdown the scheduler thread", e);
				}
			}
			commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-OnDemand-"+typeOfService+" :"+ddpScheduler.getSchId());
		}
	}
	
	

	public boolean executeJob(DdpScheduler ddpScheduler,String appName,Calendar executionDate) {
		
		logger.info("AppName : "+appName+". DdpVendorRuleSchedulerJob.executeJob() - Invoked sucessfully.");
		boolean isJobExecuted = false;
		Calendar startDate = null;
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),appName);
		
		if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
			logger.info("AppName : "+appName+". DdpVendorRuleSchedulerJob.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+appName);
			return isJobExecuted;
		}
		DdpScheduler scheduler = ddpSchedulerService.findDdpScheduler(ddpScheduler.getSchId());
		
		if (scheduler == null || scheduler.getSchStatus() != 0) {
			logger.info("AppName : "+appName+". DdpVendorRuleSchedulerJob.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+appName);
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
			logger.info("DdpVendorRuleSchedulerJob.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - Start Date : "+startDate.getTime()+" is greater than end date : "+endDate.getTime()+" . For Application : "+appName);
			return isJobExecuted;
		}
		
		isJobExecuted = processSchedulerJob( ddpScheduler, ddpExportRule, appName, 1, startDate, endDate,executionDate,null,null);
		
		return isJobExecuted;
	}
	
	
	public boolean processSchedulerJob(DdpScheduler ddpScheduler,DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,
			Calendar endDate,Calendar executionDate,String suppNames,String c2cNumbers) {
		
		boolean isJobExecuted = false;
		IDfSession session = null;
		
		
		try {
			//To avoid time creation of session for each iteration of loop
	    	 session = ddpDFCClientComponent.beginSession();
	    	
	    	 if (session == null) {
	     		logger.info("AppName: "+appName+". DdpVendorRuleSchedulerJob.processSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] Unable create the DFCClient Session for this Scheduler ID."+ddpExportRule.getExpSchedulerId().getSchId());
	     		sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "session", null);
	     		taskUtil.sendMailByDevelopers("For export module,In the DdpVendorRuleSchedulerJob.processSchedulerJob() - Application Name : "+appName+" & type of service : "+typeOfService+". Unable to connection the dfc session. So please restart the jboss server.",  "export - dfc session issue.");
	     		return isJobExecuted;
	     	}
	    	 
	    	 String dqlQuery = constructDqlQuery(appName, typeOfService, suppNames, c2cNumbers, startDate, endDate, ddpScheduler, ddpExportRule);
	    	 
	    	 if (dqlQuery != null) {
					
					List<DdpExportMissingDocs> exportDocsList = new ArrayList<DdpExportMissingDocs>();
					constructExportDocuments(appName, typeOfService, session, dqlQuery, ddpExportRule,exportDocsList);
					if (exportDocsList.size() == 0) {
						sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "empty", null);
						return isJobExecuted;
					}
					if (typeOfService == 1) {
						//create missing records & insert into DB.
						exportDocsList = insertExportMissingDocs(exportDocsList);
						// Update ddp scheduler last success run time.
						updateDdpSchedulerTime(endDate, executionDate, ddpScheduler, typeOfService, appName);
						logger.info("DdpVendorRuleSchedulerJob.processSchedulerJob() : Total records inserted into ddp_missing_export_docs : "+exportDocsList.size());
					}
					
					isJobExecuted = performSegregationProcess( ddpExportRule, appName, typeOfService, startDate, endDate, executionDate, exportDocsList, session);
					
				} else {
					// notification alert to the dev team. Unable to construct the query.
					taskUtil.sendMailByDevelopers("For export module,In the DdpVendorRuleSchedulerJob.processSchedulerJob() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n Unable to consrtuct DQL Query",  "export issue for appName :"+appName);
				}
	    	 
		} catch(Exception ex) {
			logger.error("DdpVendorRuleSchedulerJob.processSchedulerJob() - Unable to execute for appName : "+appName+" : type of Service : "+typeOfService, ex);
			taskUtil.sendMailByDevelopers("For export module,In the DdpVendorRuleSchedulerJob.processSchedulerJob() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n"+ex.getMessage(),  "export issue for appName :"+appName);
		} finally {
			if (session != null)
				ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
		}
	    	 
		return isJobExecuted;
	}
	
	private boolean performSegregationProcess(DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,
			Calendar endDate,Calendar executionDate,List<DdpExportMissingDocs> exportDocsList,IDfSession session) {
		
		boolean isJobExecuted = false;
				
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
			taskUtil.sendMailByDevelopers("For export module,In the DdpVendorRuleSchedulerJob.performSegregationProcess() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n Unable to Transfer data to customer.\r\n Please inform to customer to check the FTP/SFTP/UNC protocols.",  "export issue for appName :"+appName);
			sendMailBasedOnContent(ddpExportRule, appName, typeOfService, endDate, startDate, executionDate, "transferissue", null);
			return isJobExecuted;
		}
		
		isJobExecuted = performTransfer(ddpExportRule, appName, typeOfService, startDate, endDate, executionDate, exportDocsList, session, transferObjects, ddpTransferObject);
		
		return isJobExecuted;
		
	}
	
	private boolean performTransfer(DdpExportRule ddpExportRule,String appName,Integer typeOfService,Calendar startDate,
			Calendar endDate,Calendar executionDate,List<DdpExportMissingDocs> exportDocsList,
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
				
				//merge
				if (compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("merge")) {
						
					performMergeOperation(appName, typeOfService, exportDocsList, mailBody, namingConvention, sourceFolder, ddpTransferObject, exportedList, unExportedList, exportedReports, session);		
						
				} else {
					for (DdpExportMissingDocs docs : exportDocsList) {
						downloadFileIntoLocal(appName, typeOfService, docs, mailBody, namingConvention, sourceFolder, ddpTransferObject, exportedList, unExportedList, exportedReports, session);
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
				logger.error("AppName : "+appName+" : DdpVendorRuleSchedulerJob.performTransfer()  Unable to transfer file for Rule ID : "+ddpExportRule.getExpRuleId(), ex);
				taskUtil.sendMailByDevelopers("For export module,In the DdpVendorRuleSchedulerJob.performTransfer() - Application Name : "+appName+" & type of service : "+typeOfService+".\r\n Unable to Transfer data to customer.\r\n Please check the code.\r\nError Message : "+ex.getMessage(),  "export issue for appName :"+appName);
			} finally {
				if (tmpFile != null)
					SchedulerJobUtil.deleteFolder(tmpFile);		
			}
			
		} else {
			logger.info("AppName : "+appName+". CompressionSetup is null for the Rule ID :"+ddpExportRule.getExpRuleId()+" & Sch ID : "+ddpExportRule.getExpSchedulerId().getSchId());
		}
		
		return isFileTransfered;
	}
	
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
	 * 
	 * @param reports
	 */
	private void createExportSuccessReprots(List<DdpExportSuccessReport> reports) {
		
		for (DdpExportSuccessReport report : reports) {
			ddpExportSuccessReportService.saveDdpExportSuccessReport(report);
		}
	
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
				logger.info("AppName : "+appName+" DdpVendorRuleSchedulerJob.downloadFileIntoLocal() - Unable to download the documents robjectid is "+exportDocs.getMisRObjectId()+" : AppName : "+appName);
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
			logger.error("DdpVendorRuleSchedulerJob.downloadFileIntoLocal() - Unable to download for r_object_id : "+exportDocs.getMisRObjectId()+" : appName : "+appName, ex);
		}
		
		return isDownload;
	}
	
	private boolean performMergeOperation (String appName,Integer typeOfService, List<DdpExportMissingDocs> exportMissingDocsList, StringBuffer mailBody,
			DdpDocnameConv namingConvention,String sourceFolder,DdpTransferObject ddpTransferObject,List<DdpExportMissingDocs> exportedList,
			List<DdpExportMissingDocs> unExportedList,List<DdpExportSuccessReport> exportedReports,IDfSession session) {
		
		boolean isDownload = false;
		try {
			
			List<String> fileNameList = new ArrayList<String>();
			commonUtil.readFileNamesFromLocation(sourceFolder, ddpTransferObject, fileNameList);
			
			List<String> rObjectIDsList = new ArrayList<String>();
			for (DdpExportMissingDocs export : exportMissingDocsList)
				rObjectIDsList.add(export.getMisRObjectId());
			
			Map<String, String> loadNumberMap = commonUtil.getLoadNumberMapDetails(namingConvention, exportMissingDocsList.get(0), appName, env);
			String fileName = getDocumentFileName( namingConvention, exportMissingDocsList.get(0),fileNameList,loadNumberMap);
			
			String mergeID = commonUtil.performMergeOperation(rObjectIDsList, fileName, env.getProperty("ddp.vdLocation"), env.getProperty("ddp.objectType"), session,appName);
			
			if (mergeID == null) {
				logger.info("AppName : "+appName+" DdpVendorRuleSchedulerJob.performMergeOperation() - Unable to merge the documents");
				unExportedList.addAll(exportMissingDocsList);
				taskUtil.sendMailByDevelopers("For export module,In the DdpVendorRuleSchedulerJob.performMergeOperation() - Application Name : "+appName+", & type of service : "+typeOfService+".\r\n Unable to Meger the document.\r\n Please check the Content Transfermation Server.",  "export merging issue for appName :"+appName);
				
			} else {
				 isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(mergeID, sourceFolder, fileName, session);
				if (!isDownload) {
					logger.info("AppName : "+appName+" DdpVendorRuleSchedulerJob.performMergeOperation() - Unable to download the documents robjectid is "+mergeID);
					unExportedList.addAll(exportMissingDocsList);
				} else {
					constructMailBody(mailBody, appName, sourceFolder, fileName, exportMissingDocsList, typeOfService, exportedReports);
					exportedList.addAll(exportMissingDocsList);
				}
			}
		} catch (Exception ex) {
			unExportedList.addAll(exportMissingDocsList);
			isDownload = false;
			logger.error("DdpVendorRuleSchedulerJob.performMergeOperation() - Unable to merger the document for appName : "+appName , ex);
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
		
		
		mailBody.append(format.format(new Date())+",receive,success,"+fileSize+",00:00:01,"+exportDocs.get(0).getMisSuppName()+","+exportDocs.get(0).getMisC2cNum()+","+fileName+"\n");
		if (typeOfService == 1 || typeOfService == 3) {
			exportedReports.addAll(commonUtil.constructExportReportDomainObject(exportDocs, fileSize, fileName, typeOfService));
		}
	}
	
	
	private String constructDqlQuery(String appName,int typeOfService,String suppNames,String c2cNumbers,
			Calendar startDate,Calendar endDate,DdpScheduler ddpScheduler,DdpExportRule exportRule) {
		
		logger.info("DdpVendorRuleSchedulerJob.constructDqlQuery() - invoked into method for appName :"+appName);
		String query = getQueryFromProperties(appName, ddpScheduler, exportRule, typeOfService);
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		
    	String strStartDate = dateFormat.format(startDate.getTime());
    	String strEndDate = dateFormat.format(endDate.getTime());
        	
		if (query != null) {
			
			//This statement will not be executed due client id will not be configured.
			if (query.contains("%%CLINETID%%")) {
				String clientIds = env.getProperty("export.rule."+appName+".clientids");
				if (clientIds != null) {
				 query =	query.replaceAll("%%CLINETID%%", commonUtil.joinString(clientIds.split(","), "'", "'", ","));
				}
			}
			
			//This statement will not be executed due to agl_doc_types will not be configured in query.
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
			
			
			if (typeOfService == 4 && !suppNames.isEmpty()) 
				query = query.replaceAll("%%SUPP_NAME%%",commonUtil.joinString(suppNames.split(","), "'", "'", ","));
			else if (typeOfService == 5 && !c2cNumbers.isEmpty())
				query = query.replaceAll("%%C2C_NUM%%",commonUtil.joinString(c2cNumbers.split(","), "'", "'", ","));
			
		}
		logger.info("DdpVendorRuleSchedulerJob.constructDqlQuery() - End of method : Query to execute is :"+query);
		
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
			exportDocsList.addAll(constructMissingDocs(idfCollection,appName,ddpExportRule.getExpRuleId()));
		}
		
		return exportDocsList;

	}
	
	
	/**
	 * Method used for constructing the Missing export documents.
	 * 
	 * @param iDfCollection
	 * @return List<DdpExportMissingDocs>
	 */
	public List<DdpExportMissingDocs> constructMissingDocs(IDfCollection iDfCollection,String appName,Integer exportId) {
		
		List<DdpExportMissingDocs> missingDocs = new ArrayList<DdpExportMissingDocs>();
		try {
			while(iDfCollection.next()) {
				
				DdpExportMissingDocs exportDocs = new DdpExportMissingDocs();
				Calendar createdDate = GregorianCalendar.getInstance();
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				
				exportDocs.setMisRObjectId(iDfCollection.getString("r_object_id"));
			//	exportDocs.setMisEntryNo(iDfCollection.getString("agl_customs_entry_no"));
				//exportDocs.setMisMasterJob(iDfCollection.getString("agl_master_job_number"));
				//exportDocs.setMisJobNumber(iDfCollection.getString("agl_job_numbers"));
			//	exportDocs.setMisConsignmentId(iDfCollection.getString("agl_consignment_id"));
				//Invoice number for sales document reference
			//	exportDocs.setMisEntryType(iDfCollection.getString("agl_doc_ref"));
				//Content size
				exportDocs.setMisCheckDigit(iDfCollection.getString("r_content_size"));
				System.out.println("Inside the construnctMissingDocs : "+exportDocs.getMisEntryType());
				exportDocs.setMisRobjectName(iDfCollection.getString("object_name"));
				exportDocs.setMisSuppName(iDfCollection.getString("agl_sup_name"));
				exportDocs.setMisC2cNum(iDfCollection.getString("agl_c2c_number"));
				exportDocs.setMisAppName(appName);
				exportDocs.setMisStatus(0);
				exportDocs.setMisExpRuleId(exportId);
				exportDocs.setMisCreatedDate(GregorianCalendar.getInstance());
				//exportDocs.setMisDocVersion(iDfCollection.getString("r_version_label"));
				
				try {
				createdDate.setTime(dateFormat.parse(iDfCollection.getString("r_creation_date")));
				exportDocs.setMisDmsRCreationDate(createdDate);
				
				} catch(Exception ex) {
					logger.error("CommonUtils.constructMissingDocs() - Unable to parse string to date : for App Name : "+appName,ex.getMessage());
				}
				missingDocs.add(exportDocs);
			}
		} catch (Exception ex) {
			missingDocs = new ArrayList<DdpExportMissingDocs>();
			logger.error("DdpVendorRuleSchedulerJob.constructMissingDocs() - Unable to construct the Missing docs for App Name : "+appName, ex.getMessage());
		}
		return missingDocs;
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
	//	if (ddpScheduler.getSchQuerySource() == null || ddpScheduler.getSchQuerySource().intValue() == 0) {
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6)
				query = env.getProperty("export.rule."+appName+".customQuery."+env.getProperty(typeOfService+""));
			else 
				query = env.getProperty("export.rule."+appName+".customQuery");
			
//		} else if (ddpScheduler.getSchQuerySource().intValue() == 1) {
//			
//			List<DdpExportQueryUi> queryUis = commonUtil.getExportQueryUiByRuleID(exportRule.getExpRuleId());
//			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6)
//				query = commonUtil.constructQueryWithExportQueryUIs(queryUis,"export.rule."+appName+".customQuery."+env.getProperty(typeOfService+"")+".byUI","export.rule.customQuery."+env.getProperty(typeOfService+"")+".byUI");
//			else 
//				query = commonUtil.constructQueryWithExportQueryUIs(queryUis,"export.rule."+appName+".customQuery.byUI","export.rule.customQuery.byUI");
//			
//		} else {
//			
//			List<DdpExportQuery> queryList = commonUtil.getExportQueryByRuleID(exportRule.getExpRuleId());
//			if (queryList != null && queryList.size() > 0)
//				query = commonUtil.constructQueryFromTXT(queryList.get(0).getExqQuery(), appName);
//			
//		}
		
//			
//		if (!query.toLowerCase().contains("%%PRIMARYDOCTYPE%%".toLowerCase())) 
//			query += " and (agl_control_doc_type in (%%PRIMARYDOCTYPE%%))";
			
		//if type of service is 4 as Suppier name then if n
		if (typeOfService.intValue() == 4 && !query.toLowerCase().contains("%%SUPP_NAME%%".toLowerCase()))
			query += " and any agl_sup_name in (%%SUPP_NAME%%)";
		
		if (typeOfService.intValue() == 5 && !query.toLowerCase().contains("%%C2C_NUM%%".toLowerCase()))
			query += " and agl_c2c_number in (%%C2C_NUM%%)";
		
//		if (typeOfService.intValue() == 6 && !query.toLowerCase().contains("%%DOCREFS%%".toLowerCase()))
//			query += " and agl_doc_ref in (%%DOCREFS%%)";
		
		if (!query.toLowerCase().contains("'%%startdate%%".toLowerCase()) && !query.toLowerCase().contains("'%%enddate%%".toLowerCase()))
			query += " and ( agl_creation_date between Date('%%startdate%%','dd/mm/yyyy HH:mi:ss') and Date('%%enddate%%','dd/mm/yyyy HH:mi:ss'))";
	
		return query;
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
	
	/**
	 * 
	 * Method used for updating the scheduler time.
	 *
	 * @param endDate
	 * @param presentDate
	 * @param ddpScheduler
	 * @param typeOfService
	 * @param appName
	 */
	private void updateDdpSchedulerTime(Calendar endDate,Calendar presentDate,DdpScheduler ddpScheduler,int typeOfService,String appName) {
		
		try {
			if (typeOfService == 1) {
				ddpScheduler.setSchLastRun(presentDate);
				ddpScheduler.setSchLastSuccessRun(endDate);
				ddpSchedulerService.updateDdpScheduler(ddpScheduler);
			}
		} catch (Exception ex) {
			logger.error("AppName : "+appName+". DdpVendorRuleSchedulerJob.updateDdpSchedulerTime() - Error ", ex);
		}
	}
	
	/************************************************ Sending Email Code *****************************************************/
	/**
	 * Method used for sending the success emails.
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
			csvFile.append("Supplier Name");
			csvFile.append(",");
			csvFile.append("C2C Number");
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
	 * Method used for sending the missing the email details.
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
			csvFile.append("Customer Number");
			csvFile.append(",");
			csvFile.append("Supplier Name");
			csvFile.append(",");
			csvFile.append("C2C Number");
			csvFile.append("\n");
			
			for (DdpExportMissingDocs export : missingRecords) {
						 
				csvFile.append(appName);
				csvFile.append(",");
				csvFile.append(export.getMisSuppName());
				csvFile.append(",");
				csvFile.append(export.getMisC2cNum());
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
	
	/**
	 * Method used for sending the email based on the content.
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
				if (commSetup.getCmsCommunicationProtocol().equalsIgnoreCase("smtp")) {
					DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId());
					hostName = ddpCommEmail.getCemEmailTo();
				} else {
					DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId());
					destinationLocation = uncDetails.getCunUncPath();
					hostName = commonUtil.getHostName(uncDetails.getCunUncPath());
				}
			}
			
			if (commSetup.getCmsCommunicationProtocol2() != null && !commSetup.getCmsCommunicationProtocol2().isEmpty()) {
				isFTPType =	commSetup.getCmsCommunicationProtocol2().equalsIgnoreCase("FTP")? true : false;
				if (isFTPType) {
					
					DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2()); 
						//need to split with ftp:// so it 6.
						destinationLocation2 = commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation());				
						hostName2 = commonUtil.getHostName(ftpDetails.getCftFtpLocation());
				} else {
					if (commSetup.getCmsCommunicationProtocol2().equalsIgnoreCase("smtp")) {
						DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2());
						hostName2 = ddpCommEmail.getCemEmailTo();
					} else {
						DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2());
						destinationLocation2 = uncDetails.getCunUncPath();
						hostName2 = commonUtil.getHostName(uncDetails.getCunUncPath());
					}
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
					if (commSetup.getCmsCommunicationProtocol3().equalsIgnoreCase("smtp")) {
						DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3());
						hostName3 = ddpCommEmail.getCemEmailTo();
					} else {
						DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3());
						destinationLocation3 = uncDetails.getCunUncPath();
						hostName3 = commonUtil.getHostName(uncDetails.getCunUncPath());
					}
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
}
