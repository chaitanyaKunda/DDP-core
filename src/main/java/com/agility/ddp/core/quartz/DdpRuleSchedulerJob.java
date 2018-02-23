package com.agility.ddp.core.quartz;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.persistence.TypedQuery;

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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
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
import com.agility.ddp.core.util.NamingConventionUtil;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommFtpService;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.agility.ddp.data.domain.DdpCommUncService;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.agility.ddp.data.domain.DdpCommunicationSetupService;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpDmsDocsDetailService;
import com.agility.ddp.data.domain.DdpDocnameConv;
import com.agility.ddp.data.domain.DdpDocnameConvService;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.agility.ddp.data.domain.DdpExportSuccessReportService;
import com.agility.ddp.data.domain.DdpExportVersionSetup;
import com.agility.ddp.data.domain.DdpNotification;
import com.agility.ddp.data.domain.DdpRateSetup;
import com.agility.ddp.data.domain.DdpRule;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpRuleService;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;

/**
 * Class is used for the Export Document setup.
 * 
 * @author DGuntha
 *
 */
@Configuration
//@PropertySource({"file:///E:/DDPConfig/custom.properties","file:///E:/DDPConfig/mail.properties","file:///E:/DDPConfig/ddp.properties","file:///E:/DDPConfig/export.properties"})
public class DdpRuleSchedulerJob extends QuartzJobBean implements DdpSchedulerJob
{
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
	DdpCommFtpService ddpCommFtpService;
	
	@Autowired
	DdpCommUncService ddpCommUncService;
	
	@Autowired
	DdpDmsDocsDetailService ddpDmsDocsDetailService;
	
	@Autowired
	DdpCommunicationSetupService ddpCommunicationSetupService;
	
	@Autowired
	DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	DdpDocnameConvService ddpDocnameConvService;
	
	@Autowired
	DdpSchedulerService ddpSchedulerService;
	
	@Autowired
	private DdpExportSuccessReportService ddpExportSuccessReportService;
	
//	@Value( "${ddp.export.folder}" )
//    String tempFilePath;
	
	private static final Logger logger = LoggerFactory.getLogger(DdpRuleSchedulerJob.class);
	
	@Autowired
    DdpRuleDetailService ddpRuleDetailService;
	
	@Autowired
	DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	private TaskUtil taskUtil;
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpTransferFactory ddpTransferFactory;
	
	@Autowired
	private DdpRuleService ddpRuleServive;
	
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("DdpRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpRuleSchedulerJob.executeInternal() executed successfully.");
	}
	
	//@AuditLog(dbAuditBefore=true, message="DdpRuleSchedulerJob.initiateCronJob(ddpSchedulerObj) Method Invoked.")
	public void initiateSchedulerJob(Object[] ddpSchedulerObj) 
	{
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		logger.info("DdpRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked  for Scheduer ID :"+ddpScheduler.getSchId());
		try {
			commonUtil.addExportSetupThread("RuleByClientID - General :"+ddpScheduler.getSchId());
				
			logger.debug("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
			executeSchedulerJob(ddpScheduler,1);
		} catch (Exception ex) {
			logger.error("DdpRuleSchedulerJob.initiateSchedulerJob() - Unable to peform export operation for scheduler id : "+ddpScheduler.getSchId(), ex);
		} finally {
			commonUtil.removeExportSetupThread("RuleByClientID - General :"+ddpScheduler.getSchId());
		}
		logger.info("DdpRuleSchedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
	}
	
	
	/**
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
		
		logger.info("DdpRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		File generatedReportFolder = null;
		if (ddpScheduler.getSchStatus() == 0 && ddpScheduler.getSchReportFrequency() != null && !ddpScheduler.getSchReportFrequency().isEmpty() && !ddpScheduler.getSchReportFrequency().equalsIgnoreCase("none")) {
			try {
				commonUtil.addExportSetupThread("RuleByClientID - SchedulerReports :"+ddpScheduler.getSchId());
				String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchReportFrequency());
				Calendar startCalendar = Calendar.getInstance();
				Calendar endCalendar = Calendar.getInstance();  
				SimpleDateFormat dateFor = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss") ;
		   		//Call below method to get date range - This needs to be implemented
		   		Calendar startDate = SchedulerJobUtil.getQueryStartDate(strFeq, startCalendar);
		   		Calendar endDate = endCalendar;
		   		logger.info("DdpRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) - Start Date : "+dateFor.format(startDate.getTime())+" : End Date : "+dateFor.format(endDate.getTime()));
		   		generatedReportFolder = commonUtil.generateReports(ddpScheduler, startDate, endDate, 2,Constant.EXECUTION_STATUS_SUCCESS);
			} catch (Exception ex) {
				logger.error("DdpRuleSchedulerJob.initiateSchedulerReport() unable generate reports for scheduler id :"+ddpScheduler.getSchId(), ex);
			} finally {
				commonUtil.removeExportSetupThread("RuleByClientID - SchedulerReports :"+ddpScheduler.getSchId());
				if (generatedReportFolder != null)
					SchedulerJobUtil.deleteFolder(generatedReportFolder);
			}
	   		
		}
	}
	
	/**
	 * Method used for the executing the S
	 * @param ddpScheduler
	 * @return
	 */
	public boolean executeSchedulerJob(DdpScheduler ddpScheduler,int typeOfService) {
		
		boolean isSchedulerExuected = false;
		ddpScheduler = ddpSchedulerService.findDdpScheduler(ddpScheduler.getSchId());
		//Check for SCH_STATUS. If active = 0, process further and if inactive = 1 simply return the job without any further execution
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpRuleSchedulerJob.executeShedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return isSchedulerExuected;
		}
		try {				
			//Check for START DATE. If later than today, simply return the job without any further execution
			Calendar today = Calendar.getInstance();
			today.setTime(new Date());
			Calendar currentCalendar = Calendar.getInstance();
			Calendar startCalendar = Calendar.getInstance();
			Calendar endCalendar = Calendar.getInstance();
			currentCalendar.set(Calendar.YEAR, today.get(Calendar.YEAR) );
			currentCalendar.set(Calendar.MONTH, today.get(Calendar.MONTH)+1);
			currentCalendar.set(Calendar.DAY_OF_MONTH, today.get(Calendar.DAY_OF_MONTH));
			        
			GregorianCalendar actDate = (GregorianCalendar) ddpScheduler.getSchStartDate().clone();
			Calendar actCalendar = Calendar.getInstance();
			                        
			actCalendar.set(Calendar.YEAR, actDate.get(Calendar.YEAR) );
			actCalendar.set(Calendar.MONTH, actDate.get(Calendar.MONTH));
			actCalendar.set(Calendar.DAY_OF_MONTH, actDate.get(Calendar.DAY_OF_MONTH));
					
			if( (currentCalendar.equals(actCalendar) ) || (currentCalendar.after(actCalendar) ))
			 {
	        	//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
	       		String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchCronExpressions());
			 		   
	       		//Call below method to get date range - This needs to be implemented
	       		Calendar queryStartDate = SchedulerJobUtil.getQueryStartDate(strFeq, startCalendar,ddpScheduler.getSchCronExpressions());
	       		Calendar queryEndDate = endCalendar;
		        
	    		if (ddpScheduler.getSchLastSuccessRun() != null)  {
	  		  		queryStartDate = ddpScheduler.getSchLastSuccessRun();
	    		}
	    		
	       		//checking the delay count.
	       		if (ddpScheduler.getSchDelayCount() != null && ddpScheduler.getSchDelayCount().intValue() != 0 && typeOfService == 1) {
	    			
	    			if (strFeq.equalsIgnoreCase("monthly")) {
	    				queryStartDate.add(Calendar.MONTH,-ddpScheduler.getSchDelayCount());
	    				queryEndDate.add(Calendar.MONTH, -ddpScheduler.getSchDelayCount());
	    			} else if (strFeq.equalsIgnoreCase("weekly")) {
	    				queryStartDate.add(Calendar.WEEK_OF_YEAR, -ddpScheduler.getSchDelayCount());
	    				queryEndDate.add(Calendar.WEEK_OF_YEAR, -ddpScheduler.getSchDelayCount());
	    			} else if (strFeq.equalsIgnoreCase("daily")) {
	    				queryStartDate.add(Calendar.DAY_OF_YEAR, -ddpScheduler.getSchDelayCount());
	    				queryEndDate.add(Calendar.DAY_OF_YEAR, -ddpScheduler.getSchDelayCount());
	    			} else if (strFeq.equalsIgnoreCase("hourly")) {
	    				queryStartDate.add(Calendar.HOUR_OF_DAY, -ddpScheduler.getSchDelayCount());
	    				queryEndDate.add(Calendar.HOUR_OF_DAY, -ddpScheduler.getSchDelayCount());
	    			}
	    		}
	       		
	       		//if start Date is greater than end date then we following below step.
	       		if( (queryStartDate.equals(queryEndDate) ) || (queryStartDate.after(queryEndDate) )){
	       			queryStartDate = SchedulerJobUtil.getQueryStartDate(strFeq, Calendar.getInstance(),ddpScheduler.getSchCronExpressions());
	       		}
	       		
	       		isSchedulerExuected = executeSchedulerWithDateRange(ddpScheduler,queryStartDate,queryEndDate,today,typeOfService,null,null,null);
	       		
	        } else  {
			   	logger.info("DdpRuleSchedulerJob.executeShedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] STRAT DATE is NOT reached.",ddpScheduler.getSchId());
				return isSchedulerExuected;
	        }
		} catch (Exception ex) {
			logger.error("DdpRulescheduler.executeSchedulerJob()- Unable toe execute query. For scheduler id: "+ddpScheduler.getSchId(),ex);
		}
		return isSchedulerExuected;

	}
	
	/**
	 * Method used for running the onDemand service.
	 * 
	 * @param ddpScheduler
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param today
	 * @param typeOfService
	 * @param jobNumbers
	 * @param consignmentIDs
	 * @param docRefs
	 * @param scheduler
	 */
	public void onDemandScheduler(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,Calendar today,
			int typeOfService,String jobNumbers,String consignmentIDs,String docRefs,Scheduler scheduler) {
		
		try {
			
			 commonUtil.addExportSetupThread("RuleByClientID - "+env.getProperty(typeOfService+"") +" :"+ddpScheduler.getSchId());
			 executeOnDemandScheduler(ddpScheduler, queryStartDate, queryEndDate, today, typeOfService, jobNumbers, consignmentIDs, docRefs);
			 
		} catch (Exception ex) {
			logger.error("DdpRuleschedulerJob.runOnDemanService() - Unable to execute the processSchedulerJob in executeOnDemandScheduler method", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpRuleschedulerJob.runOnDemanService() - Unable to shutdown the scheduler thread", e);
				}
			}
			 commonUtil.addExportSetupThread("RuleByClientID - "+env.getProperty(typeOfService+"") +" :"+ddpScheduler.getSchId());
		}
	}
	
	/**
	 * Method used for exporting the document using OnDemand Services.
	 * 
	 * @param ddpScheduler
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param today
	 * @param typeOfService
	 * @param jobNumbers
	 * @param consignmentIDs
	 * @param docRefs
	 * @return
	 */
	public boolean executeOnDemandScheduler(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,Calendar today,
			int typeOfService,String jobNumbers,String consignmentIDs,String docRefs) {
		
		boolean isDocumentsExported = false;
		String dynamicValues = "";
		SimpleDateFormat dateFor = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss") ;
		logger.info("DdpRuleShedulerJob.executeOnDemandScheduler() -Invoked susccesfully. Query Start Date :"+dateFor.format(queryStartDate.getTime())+" . Endate : "+dateFor.format(queryEndDate.getTime())+" > For scheduler ID : "+ddpScheduler.getSchId());
		
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),"Export By Rule By Client ID");
		
		if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
			logger.info("DdpRuleSchedulerJob.executeOnDemandScheduler(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,Calendar today) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
			return isDocumentsExported;
		}
		
    	//Fetch matched rule detail id (RDT_ID) from DDP_RULE_DETAIL table for give Scheduler ID (SCH_ID)
		List<DdpRuleDetail> ruleDetails = commonUtil.getAllRuleDetails(ddpExportRule.getExpRuleId());
		
		//For fetching the unique branches
		Map<String, List<String>> uniqueBraches = new HashMap<String, List<String>>();
		Map<String, List<DdpRuleDetail>> uniqueDocType = new HashMap<String, List<DdpRuleDetail>>();
		List<DdpTransferObject> ddpTransferObjects = new ArrayList<DdpTransferObject>();
    	DdpTransferObject ddpTransferObject = null;
    	
    	if (typeOfService == 4 && !jobNumbers.isEmpty()) 
    		dynamicValues = jobNumbers;
		else if (typeOfService == 5 && !consignmentIDs.isEmpty())
			dynamicValues = consignmentIDs;
		else if (typeOfService == 6 && !docRefs.isEmpty())
			dynamicValues = docRefs;
    	
    	/**
    	 * Based on the Scheduler ID the DDP_RULE_DETAIL are fetched so the Rule_ID will be same for all the
    	 * DdpCategorizedDocslist. So that getting the FTP or UNC details based on the one of zeroth record of DdpCategorizedDocs list.
    	 */
    	ddpTransferObject = commonUtil.getTransferObjectBasedRuleDetails(ruleDetails, ddpTransferObjects);
    	
    	if (ddpTransferObject == null) {
    		logger.info("DdpRuleSchedulerJob.executeOnDemandScheduler(Object[] ddpSchedulerObj) - SCHEDULER [{}] FTP & UNC Details are not available.",ddpScheduler.getSchId());
    		sendMailForIssueExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, today,  new Date(), typeOfService, ddpTransferObjects,dynamicValues);
    		return isDocumentsExported;
    	}
		
		//Based on Document type adding setting the set of branches
		commonUtil.filterDocTypesBasedOnRuleDetails(ruleDetails, uniqueBraches, uniqueDocType);
    	
		//To avoid time creation of session for each iteration of loop
		IDfSession session = ddpDFCClientComponent.beginSession();
	    
    	if (session == null) {
    		logger.info("DdpRuleSchedulerJob.executeOnDemandScheduler(Object[] ddpSchedulerObj) - SCHEDULER [{}] Unable create the DFCClient Session for this Scheduler ID.",ddpScheduler.getSchId());
    		return isDocumentsExported;
    	}
    	
    	String tempFilePath = env.getProperty("ddp.export.folder");
    	List<DdpExportMissingDocs> exportList = new ArrayList<DdpExportMissingDocs>();
    	List<DdpExportMissingDocs> missingList = new ArrayList<DdpExportMissingDocs>();
    	StringBuffer mailBody = new StringBuffer();
    	List<DdpExportSuccessReport> exportedReports = new ArrayList<DdpExportSuccessReport>();
    	//creating the temporary file with time stamp.
    	Date date = new Date() ;
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
    	String tempFolderPath = tempFilePath+"/temp-"+typeOfService+"-"+ddpExportRule.getExpRuleId()+ "-" + dateFormat.format(date);
    	File tmpFile = new File(tempFolderPath);
    	tmpFile.mkdir();
    	try {
    		
    		if (checkToEnterJobNumberProcess(ruleDetails.get(0).getRdtPartyId().toLowerCase()))  {
				//List<DdpExportMissingDocs> missingDocs = new ArrayList<DdpExportMissingDocs>();
				//TODO:Based on job number
				jobNumberBasedDocumentsOnDemand(ruleDetails, uniqueDocType, exportList, uniqueBraches, queryStartDate, queryEndDate, dynamicValues, typeOfService, session);
			} else {
    		
		    	for (String documentType : uniqueDocType.keySet()) {
		    		
		    		IDfCollection iDfCollection = null;
		    		List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(documentType);
		    		List<String> branchSet = uniqueBraches.get(documentType);
		    		String dqlQuery = constructDQLQueryForOnDemandService(ruleDetailSet, typeOfService, branchSet, documentType, queryStartDate, queryEndDate, dynamicValues);
		    		logger.info("DdpRuleSchedulerJob.executeOnDemandScheduler() - DQL query : "+dqlQuery);
		    		try {
		    			iDfCollection = getIDFCollectionDetails(dqlQuery, session);
		    			if (iDfCollection != null) {
		    				
		    					exportList.addAll(constructMissingDocs(iDfCollection,"RuleByClientID",ddpExportRule.getExpRuleId()));
		    				
		    				iDfCollection.close();
		    			}
		    		} catch (Exception ex) {
		    			logger.error("DdpRuleSchedulerJob,executeOnDemandScheduler(Object[] ddpSchedulerObj) - Unable to fetch result "+dqlQuery, ex);
		    		}
		    		
		    	}
			}
	    	
	    	if (exportList.size() > 0) {
	    		
	    		preformFileDownloadForOnDemandService(ddpExportRule, exportList, session, tempFolderPath, missingList, mailBody, exportedReports, typeOfService);
	    		List<DdpTransferObject> transferObjects = new ArrayList<DdpTransferObject>();
		    	
		    	for (DdpTransferObject transferObject : ddpTransferObjects) {
					
					if (transferObject.isConnected()) {
						
						boolean isTransferd = ddpTransferFactory.transferFilesUsingProtocol(transferObject, tmpFile,ruleDetails.get(0).getRdtCompany().getComCompanyName(),
								ruleDetails.get(0).getRdtPartyId(),queryStartDate,queryEndDate,"ruleByClientID."+typeOfService);
						 
						if (isTransferd) {
							isDocumentsExported = true;
							transferObjects.add(transferObject);						
						}
						
						if (!isTransferd) {
							List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
							list.add(transferObject);
							sendMailForIssueExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, today,  new Date(), typeOfService, list,dynamicValues);
						}
					} else {
						List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
						list.add(transferObject);
						sendMailForIssueExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, today,  new Date(), typeOfService, list,dynamicValues);
					}
				}
			    if (exportedReports.size() > 0)
			    	createExportSuccessReprots(exportedReports);
			    
			    if (transferObjects.size() > 0) 
				    sendMailForSuccessExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, mailBody,  tempFolderPath, today, new Date(),typeOfService, transferObjects,dynamicValues);
				if (missingList.size() > 0)
				    sendMailForMissingDocumentsOnDemand(ruleDetails.get(0).getRdtPartyId(), missingList, ddpExportRule, tempFolderPath, queryEndDate, queryStartDate,today, new Date(),typeOfService,dynamicValues);
	    	}
    	} catch(Exception ex) {
    		logger.error("DdpRuleSchedulerJob.executeOnDemandScheduler() - exporting documents ",ex);
    	} finally {
    		SchedulerJobUtil.deleteFolder(new File(tempFolderPath));
    		if (session != null)
    			ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
    	}
    
		return isDocumentsExported;
		
	}
	
	/**
	 * Method used for getting the details based on Job number.
	 * 
	 * @param ruleDetails
	 * @param uniqueDocType
	 * @param exportList
	 * @param uniqueBraches
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param dynamicValues
	 * @param typeOfService
	 * @param session
	 */
	private void jobNumberBasedDocumentsOnDemand(List<DdpRuleDetail> ruleDetails,Map<String, List<DdpRuleDetail>> uniqueDocType,
			List<DdpExportMissingDocs> exportList,Map<String, List<String>> uniqueBraches,Calendar queryStartDate,Calendar queryEndDate,
			String dynamicValues,Integer typeOfService,IDfSession session) {
	
	//	Map<String,List<DdpExportMissingDocs>> jobNumberMap = new HashMap<String,List<DdpExportMissingDocs>>();
		//per Job number fetching the details
		Map<String,List<String>> documentTypesMandatory = new HashMap<String,List<String>>();
		
		List<String> propertiesNotPresentDocType = new ArrayList<String>();
		
		Map<Integer,List<String>> propertyDocMap = new HashMap<Integer,List<String>>();
		String documentTypes = env.getProperty("export.ruleByClientID.clientName."+ruleDetails.get(0).getRdtPartyId());
		
		if (documentTypes != null) {
			String[] documentTypeArray = documentTypes.split(",");
			
			//Grouping the Document type into map based on the priority provided in the properties file.
			for (String documentType : documentTypeArray) {
				if (documentType.contains(":")) {
					try {
						String[] priorityDocType = documentType.split(":");
						//Checking rule consists for configured document type
						if (!uniqueDocType.containsKey(priorityDocType[0]))
							continue;
						
						//Document type : priorityDocType
						if (!propertyDocMap.containsKey(Integer.parseInt(priorityDocType[1]))) {
							List<String> docTypeList = new ArrayList<String>();
							docTypeList.add(priorityDocType[0]);
							propertyDocMap.put(Integer.parseInt(priorityDocType[1]),docTypeList);
						} else {
							List<String> docTypeList = propertyDocMap.get(Integer.parseInt(priorityDocType[1]));
							//if document type is not duplicated.
							if (!docTypeList.contains(priorityDocType[0])) {
								docTypeList.add(priorityDocType[0]);
								propertyDocMap.put(Integer.parseInt(priorityDocType[1]),docTypeList);
							}
						}
					} catch (Exception ex) {
						logger.error("DdpRuleSchedulerJob.groupPriorityDocuemnt() - Based on the docuemnt types from properties file : "+documentTypes, ex);
					}
				}
			}
		 
			fetchNotPresentDocumentInConfiguration(propertyDocMap, uniqueDocType, propertiesNotPresentDocType);
			
			if (propertyDocMap != null) {
				//Fetching mandatory document type
				List<String> mandatoryDocType = propertyDocMap.get(1);
				if (mandatoryDocType != null) {
					for (String documentType : mandatoryDocType) {
						
						List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(documentType);
			    		List<String> branchSet = uniqueBraches.get(documentType);
			    		String dqlQuery = constructDQLQueryForOnDemandService(ruleDetailSet, typeOfService, branchSet, documentType, queryStartDate, queryEndDate, dynamicValues);
			    		logger.info("DdpRuleSchedulerJob.jobNumberBasedDocumentsOnDemand() - Runing dql query is : "+dqlQuery);
			    		try {
			    			IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
			    			if (iDfCollection != null) {
			    				
			    				List<DdpExportMissingDocs> exportDocs = constructMissingDocs(iDfCollection,"RuleByClientID",ruleDetails.get(0).getRdtRuleId().getRulId());
			    				if (exportDocs.size() > 0) {
			    					for (DdpExportMissingDocs exportDoc :exportDocs ) {
				    					exportList.add(exportDoc);
										
										if (!documentTypesMandatory.containsKey(exportDoc.getMisJobNumber())) {
											List<String> doclist = new ArrayList<String>();
											doclist.add(exportDoc.getMisDocType());
											documentTypesMandatory.put(exportDoc.getMisJobNumber(), doclist);
										} else {
											List<String> doclist = documentTypesMandatory.get(exportDoc.getMisJobNumber()) ;
											if (!doclist.contains(exportDoc.getMisDocType())) {
												if (!doclist.contains(exportDoc.getMisDocType())) {
													doclist.add(exportDoc.getMisDocType());
													documentTypesMandatory.put(exportDoc.getMisJobNumber(), doclist);
												}
											}
										}
			    					}
			    				}
			    				iDfCollection.close();
			    			}
			    		} catch (Exception ex) {
			    			logger.error("DdpRuleSchedulerJob.jobNumberBasedDocumentsOnDemand() - Unable to load details for dql query : "+dqlQuery, ex);
			    		}
					}
				}
				//checking the optional documents.
				List<String> optionalDocType = propertyDocMap.get(0);
				if(optionalDocType != null)	{
					
					for (String documentType : optionalDocType) {
						
						List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(documentType);
			    		List<String> branchSet = uniqueBraches.get(documentType);
			    		String dqlQuery = constructDQLQueryForOnDemandService(ruleDetailSet, typeOfService, branchSet, documentType, queryStartDate, queryEndDate, dynamicValues);
			    		logger.info("DdpRuleSchedulerJob.jobNumberBasedDocumentsOnDemand() - Runing dql query is : "+dqlQuery);
			    		try {
			    			IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
			    			if (iDfCollection != null) {
			    				
			    				List<DdpExportMissingDocs> exportDocs = constructMissingDocs(iDfCollection,"RuleByClientID",ruleDetails.get(0).getRdtRuleId().getRulId());
			    				if (exportDocs.size() > 0) {
			    					for (DdpExportMissingDocs exportDoc : exportDocs) {
				    					List<String> docList = documentTypesMandatory.get(exportDoc.getMisJobNumber());
				    					if (docList == null) {
				    						exportList.add(exportDoc);
				    					}
			    					}
			    				}
			    			}
			    		} catch (Exception e) {
			    			logger.error("DdpRuleSchedulerJob.jobNumberBasedDocumentsOnDemand() - Unable to load details for dql query : "+dqlQuery, e);
						}
					}
				
				}
			}
			
			if (propertiesNotPresentDocType.size()  > 0) {
				
				for (String documentType : propertiesNotPresentDocType) {
					List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(documentType);
		    		List<String> branchSet = uniqueBraches.get(documentType);
		    		String dqlQuery = constructDQLQueryForOnDemandService(ruleDetailSet, typeOfService, branchSet, documentType, queryStartDate, queryEndDate, dynamicValues);
		    		logger.info("DdpRuleSchedulerJob.jobNumberBasedDocumentsOnDemand() - Runing dql query is : "+dqlQuery);
		    		try {
		    			IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
		    			if (iDfCollection != null) {
		    				
		    				List<DdpExportMissingDocs> exportDocs = constructMissingDocs(iDfCollection,"RuleByClientID",ruleDetails.get(0).getRdtRuleId().getRulId());
		    				if (exportDocs.size() > 0) {
		    					exportList.addAll(exportDocs);
		    				}
		    			}
		    		} catch (Exception e) {
		    			logger.error("DdpRuleSchedulerJob.jobNumberBasedDocumentsOnDemand() - Unable to load details for dql query : "+dqlQuery, e);
					}
				}
				
			}
		}
		
	}
	
	/**
	 * Method used for fetching the not configured document types in properties file.
	 * 
	 * @param propertyDocMap
	 * @param uniqueDocType
	 * @param propertiesNotPresentDocType
	 */
	private void fetchNotPresentDocumentInConfiguration(Map<Integer,List<String>> propertyDocMap,
			Map<String, List<DdpRuleDetail>> uniqueDocType,List<String> propertiesNotPresentDocType) {
		
		List<String> documentType = new ArrayList<String>();
		
		//optional Document type list
		List<String> propDoc_0 = propertyDocMap.get(0);
		if (propDoc_0 != null) 
			documentType.addAll(propDoc_0);
		
		//Mandatory document type list
		List<String> propDoc_1= propertyDocMap.get(1);
		
		if (propDoc_1 != null) 
			documentType.addAll(propDoc_1);
		
		
		for (String docType : uniqueDocType.keySet()) {
			//Document not configured in the properties file			
			if (!documentType.contains(docType))
				propertiesNotPresentDocType.add(docType);
		}
		
	}
	
	/**
	 * Method used for executing 
	 * @param ddpScheduler
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param today
	 * @return
	 */
	public boolean executeSchedulerWithDateRange(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,
			Calendar today,int typeOfService,String jobNumbers,String consignmentIDs,String docRefs) {
		
		boolean isSchedulerExuected = false;
		SimpleDateFormat dateFor = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss") ;
   		logger.info("DdpRuleShedulerJob.executeSchedulerWithDateRange() -Invoked susccesfully. Query Start Date :"+dateFor.format(queryStartDate.getTime())+" & Endate : "+dateFor.format(queryEndDate.getTime())+" > For scheduler ID : "+ddpScheduler.getSchId());
   		
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),"Export By Rule By Client ID");
		
		if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
			logger.info("DdpRuleSchedulerJob.executeSchedulerWithDateRange(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,Calendar today) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
			return isSchedulerExuected;
		}
		
    	//Fetch matched rule detail id (RDT_ID) from DDP_RULE_DETAIL table for give Scheduler ID (SCH_ID)
		List<DdpRuleDetail> ruleDetails = commonUtil.getAllRuleDetails(ddpExportRule.getExpRuleId());
    	
    	
    	List<DdpCategorizedDocs> ddpCategorizedDocsList = new ArrayList<DdpCategorizedDocs>();
    	
    	/*************************
    	 * 1.	IMPLEMENT CORRUPTION CHECK
    	 * 2.	ACCESS FTP location to move files
    	 *************************/
    	//TODO: Group the Categorized docs
    	groupCategorizedDocs(typeOfService, ruleDetails, jobNumbers, consignmentIDs, docRefs, ddpCategorizedDocsList, queryStartDate, queryEndDate);
    	
    
    	
    	List<DdpTransferObject> ddpTransferObjects = null;
    	DdpTransferObject ddpTransferObject = null;
    	/**
    	 * Based on the Scheduler ID the DDP_RULE_DETAIL are fetched so the Rule_ID will be same for all the
    	 * DdpCategorizedDocslist. So that getting the FTP or UNC details based on the one of zeroth record of DdpCategorizedDocs list.
    	 */
    	if (ruleDetails.size() > 0) {
    		
    		//DdpCategorizedDocs docs = ddpCategorizedDocsList.get(0);
    		DdpCommunicationSetup commSetup = getMatchedCommunicationSetup(ruleDetails.get(0).getRdtId());
    		
    		ddpTransferObjects = ddpTransferFactory.constructTransferObject(commSetup);
    			
    		if (ddpTransferObjects != null) {
    			for (DdpTransferObject transferObject : ddpTransferObjects) {
    				if (transferObject.isConnected()) {
						ddpTransferObject = transferObject;
						break;
					}
				}
    		}
    	}
    	
    
    	
    	if (ddpTransferObjects == null || ddpTransferObject == null) {
    		logger.info("DdpRuleSchedulerJob.executedSchedularWithDateRange(Object[] ddpSchedulerObj) - SCHEDULER [{}] FTP & UNC Details are not available.",ddpScheduler.getSchId());
    		sendMailForIssueExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, today,  new Date(), typeOfService, ddpTransferObjects,null);
    		return isSchedulerExuected;
    	}
    	
    	if (ddpCategorizedDocsList == null || ddpCategorizedDocsList.size() == 0) {
    		logger.info("DdpRuleSchedulerJob.executedSchedularWithDateRange(Object[] ddpSchedulerObj) - SCHEDULER [{}] DdpCategorizedDoc are empty for this scheduler id.",ddpScheduler.getSchId());
    		sendMailForSuccessExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate,  new StringBuffer(), null, today, new Date(),typeOfService,ddpTransferObjects,null);
    		return isSchedulerExuected;
    	}
    	
    	/*************************
    	 * Once identified all categorized document list, it should send FTP/UNC based on one CommunicationSetup
    	 * get communication id from ddp_export_rule
    	 * fetch communication protocol id from ddp_communication_setup
    	 * fetch details from ddp_comm_ftp for the matched communication protocol id
    	 * move all the files from temp location to the details fetched from ddp_comm_ftp table
    	 *************************/
        String tempFilePath = env.getProperty("ddp.export.folder");
    	
    	//creating the temporary file with time stamp.
    	Date date = new Date() ;
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
    	String tempFolderPath = tempFilePath+"/temp-"+ddpExportRule.getExpRuleId()+ "-" + dateFormat.format(date);
    	File tmpFile = new File(tempFolderPath);
    	tmpFile.mkdir();
    	
    	List<DdpCategorizedDocs> allVersionDocs = new ArrayList<DdpCategorizedDocs>();
    	//Map<String,DdpCategorizedDocs> latestVersionDocs = new HashMap<String, DdpCategorizedDocs>();
    	Map<String,Map<String,DdpCategorizedDocs>> latestVesrsionDocs = new HashMap<String, Map<String,DdpCategorizedDocs>>();
    	IDfSession session = null;
    	StringBuffer exportedDetails = new StringBuffer();
    	List<DdpExportSuccessReport> exportedReports = new ArrayList<DdpExportSuccessReport>();
    	try { 
	    	//Checking the version based documents
	    	for (DdpCategorizedDocs docs :ddpCategorizedDocsList) {
	    		
	    		DdpRuleDetail ddpRuleDetail = ddpRuleDetailService.findDdpRuleDetail(docs.getCatRdtId());
	    		TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ddpRuleDetail);
	    		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
	    		DdpExportVersionSetup ddpExportVersionSetup = ddpExportVersionSetups.get(0);
	    		//getting option
	    		String option = ddpExportVersionSetup.getEvsOption();
	    		DdpDmsDocsDetail dmsDocsDetails = getddpDmsDocsDetails(docs.getCatDtxId().getDtxId());
	    		
	    		//Excluding the document based configured in properties file.
				String excludeDocs = env.getProperty("export.exclude.documents."+dmsDocsDetails.getDddCompanySource().trim());
				if (excludeDocs != null && !excludeDocs.isEmpty()) {
					boolean isDocExclude = false;
					String[] execludeDocuments = excludeDocs.split(",");
					for(String execludeDoc : execludeDocuments) {
						if (execludeDoc.equalsIgnoreCase(dmsDocsDetails.getDddContentType().toLowerCase())) {
							//Excluding the documents so the changed the status.
							docs.setCatStatus(5);
							ddpCategorizedDocsService.updateDdpCategorizedDocs(docs);
							isDocExclude = true;
							break;
						}
					}
					if (isDocExclude)
						continue;
				}
	    		
	    		if (option.equals("All")) {
	    			allVersionDocs.add(docs);
	    		} else {
	    			
	    			 if (!latestVesrsionDocs.containsKey(dmsDocsDetails.getDddControlDocType())) {
	    				 Map<String,DdpCategorizedDocs> latestVersions = new HashMap<String, DdpCategorizedDocs>();
			    			latestVersions.put(dmsDocsDetails.getDddObjectName(), docs);
			    			latestVesrsionDocs.put(dmsDocsDetails.getDddControlDocType(), latestVersions);
			    	} else {
			    		 Map<String,DdpCategorizedDocs> latestVersions = latestVesrsionDocs.get(dmsDocsDetails.getDddControlDocType());
			    		if (!latestVersions.containsKey(dmsDocsDetails.getDddObjectName())) {
		    				latestVersions.put(dmsDocsDetails.getDddObjectName(), docs);
		    			} else {
		    				DdpCategorizedDocs oldDocs = latestVersions.get(dmsDocsDetails.getDddObjectName());
		    				if (oldDocs.getCatDtxId().getDtxId() < docs.getCatDtxId().getDtxId()) {
		    					latestVersions.put(dmsDocsDetails.getDddObjectName(), docs);
		    				}
		    			}
			    		latestVesrsionDocs.put(dmsDocsDetails.getDddControlDocType(), latestVersions);
			    	}
	    		}
	    	}
	    
	    	
	    	//To avoid time creation of session for each iteration of loop
	    	session = ddpDFCClientComponent.beginSession();
	    
	    	if (session == null) {
	    		logger.info("DdpRuleSchedulerJob.executedSchedularWithDateRange(Object[] ddpSchedulerObj) - SCHEDULER [{}] Unable create the DFCClient Session for this Scheduler ID.",ddpScheduler.getSchId());
	    		return isSchedulerExuected;
	    	}
	    	
	    	dateFormat.applyLocalizedPattern("dd/MM/yyyy HH:mm:ss");
	    	String startDate = dateFormat.format(queryStartDate.getTime());
	    	String endDate = dateFormat.format(queryEndDate.getTime());
	    	List<String> fileNameList = new ArrayList<String>();
	    	commonUtil.readFileNamesFromLocation(tempFolderPath, ddpTransferObject, fileNameList);
	    	
	    	//for all version documents.
	    	for (DdpCategorizedDocs docs : allVersionDocs)
	    		transferFileIntoLocalFolder(docs, today, true, tempFolderPath,session,startDate,endDate,exportedDetails,fileNameList,typeOfService,exportedReports);
	    	 
	    	//for latest version documents
	    	for (String docType :latestVesrsionDocs.keySet()) {
	    		Map<String,DdpCategorizedDocs> latestVesionBasedDocType  = latestVesrsionDocs.get(docType);
	    		for (String docsName :latestVesionBasedDocType.keySet()) {
	    			DdpCategorizedDocs docs = latestVesionBasedDocType.get(docsName);
	    			transferFileIntoLocalFolder(docs, today, false, tempFolderPath,session,startDate,endDate,exportedDetails,fileNameList,typeOfService,exportedReports);
	    		}
	    		
	    	}
	    	
	    	List<DdpTransferObject> transferObjects = new ArrayList<DdpTransferObject>();
	    	
	    	for (DdpTransferObject transferObject : ddpTransferObjects) {
				
				if (transferObject.isConnected()) {
					
					boolean isTransferd = ddpTransferFactory.transferFilesUsingProtocol(transferObject, tmpFile,ruleDetails.get(0).getRdtCompany().getComCompanyName(),
							ruleDetails.get(0).getRdtPartyId(),queryStartDate,queryEndDate,"ruleByClientID."+typeOfService);
					 
					if (isTransferd) {
						isSchedulerExuected = true;
						transferObjects.add(transferObject);						
					}
					
					if (!isTransferd) {
						List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
						list.add(transferObject);
						sendMailForIssueExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, today,  new Date(), typeOfService, list,null);
					}
				} else {
					List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
					list.add(transferObject);
					sendMailForIssueExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, today,  new Date(), typeOfService, list,null);
				}
			}
	    	
	    	//Copying all the files in FTP or UNC
	    	
	    	
	    	//Categorized need to change to 1.
	    	Set<Integer> categrizedDocsSet = new HashSet<Integer>();
	    	if (isSchedulerExuected) {
	    		for (String docType :latestVesrsionDocs.keySet()) {
		    		Map<String,DdpCategorizedDocs> latestVesionBasedDocType  = latestVesrsionDocs.get(docType);
		    		allVersionDocs.addAll(latestVesionBasedDocType.values());
	    		}
		    	for (DdpCategorizedDocs docs : allVersionDocs) {
		    		docs.setCatStatus(1);        		
		    		ddpCategorizedDocsService.updateDdpCategorizedDocs(docs);
		    		categrizedDocsSet.add(docs.getCatId());
		    	}
		    	createExportSuccessReprots(exportedReports);
	    	}
	    	
	    	
	    	//updating the categorized docs changing status if the file is not exported.
	    	List<DdpCategorizedDocs> failureList = new ArrayList<DdpCategorizedDocs>();
	    	for (DdpCategorizedDocs docs :ddpCategorizedDocsList) {
	    		if (!categrizedDocsSet.contains(docs.getCatId())) {
	    			docs.setCatStatus(2);        		
	    			ddpCategorizedDocsService.updateDdpCategorizedDocs(docs);
	    			failureList.add(docs);
	    		}
	    	}
	    	
	    	//Based on the job number checking if the duplicate documents are arrived then no need to send notification.
	    	if (isSchedulerExuected && checkToEnterJobNumberProcess(ruleDetails.get(0).getRdtPartyId().toLowerCase()))  
	    		failureList = new ArrayList<DdpCategorizedDocs>();
	    	
	    	if (isSchedulerExuected && (typeOfService == 1 || typeOfService == 3)) {
	    		
	    		ddpScheduler.setSchLastRun(today);
	    		ddpScheduler.setSchLastSuccessRun(queryEndDate);
	    		ddpSchedulerService.updateDdpScheduler(ddpScheduler);
	    	}
	    	
	    	isSchedulerExuected = true;
    		
	    	
	    	if (categrizedDocsSet.size() > 0) 
	    	   sendMailForSuccessExport(ddpExportRule, ruleDetails.get(0).getRdtPartyId(), queryEndDate, queryStartDate, exportedDetails,  tempFolderPath, today, new Date(),typeOfService, transferObjects,null);
	    	if (failureList.size() > 0)
	    		sendMailForMissingDocuments(ruleDetails.get(0).getRdtPartyId(), failureList, ddpExportRule, tempFolderPath, queryEndDate, queryStartDate,today, new Date(),typeOfService,null);
    	} catch (Exception e) {
    		 logger.error("DdpRuleSchedulerJob.executedSchedularWithDateRange() - exporting documents ",e);
    	} finally {
    		SchedulerJobUtil.deleteFolder(new File(tempFolderPath));
    		if (session != null)
    			ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
    	}
    	
    	
    	return isSchedulerExuected;
	}
	
	
	/**
	 * Method used for grouping the categorized documents into list.
	 * 
	 * @param typeOfService
	 * @param ruleDetails
	 * @param jobNumbers
	 * @param consignmentIDs
	 * @param docRefs
	 * @param ddpCategorizedDocsList
	 * @param queryStartDate
	 * @param queryEndDate
	 */
	private void groupCategorizedDocs(Integer typeOfService,List<DdpRuleDetail> ruleDetails,String jobNumbers,
			String consignmentIDs,String docRefs, List<DdpCategorizedDocs> ddpCategorizedDocsList,Calendar queryStartDate,Calendar queryEndDate) {
		
		List<DdpCategorizedDocs> catDocsList = new ArrayList<DdpCategorizedDocs>();
		for(DdpRuleDetail ruleDetail : ruleDetails)
    	{
    		//Query all matched categorised documents

			if (typeOfService == 4 && !jobNumbers.isEmpty()) 
				catDocsList.addAll(getMatchedCatDocs(Constant.DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_JOB_NUMBER,commonUtil.joinString(jobNumbers.split(","), "'", "'", ","),ruleDetail.getRdtId(),queryStartDate,queryEndDate));
			else if (typeOfService == 5 && !consignmentIDs.isEmpty())
				catDocsList.addAll(getMatchedCatDocs(Constant.DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_CONSIGNMENT_ID,commonUtil.joinString(consignmentIDs.split(","), "'", "'", ","),ruleDetail.getRdtId(),queryStartDate,queryEndDate));
			else if (typeOfService == 6 && !docRefs.isEmpty())
				catDocsList.addAll(getMatchedCatDocs(Constant.DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_DOC_REFS,commonUtil.joinString(docRefs.split(","), "'", "'", ","),ruleDetail.getRdtId(),queryStartDate,queryEndDate));
			else 
				catDocsList.addAll(getMatchedCatDocs(ruleDetail.getRdtId(),queryStartDate,queryEndDate));
    		
    	}
    	
		if (catDocsList.size() > 0 && checkToEnterJobNumberProcess(ruleDetails.get(0).getRdtPartyId().toLowerCase()))  {
			
			logger.info("DdpRuleSchedulerJob.checkToEnterJobNumberProcess() : total catDocs list : "+catDocsList);
			
			Map<String, List<DdpCategorizedDocs>> jobNumberMaP = new HashMap<String, List<DdpCategorizedDocs>>();
			
			String documentTypes = env.getProperty("export.ruleByClientID.clientName."+ruleDetails.get(0).getRdtPartyId());
			
			if (documentTypes != null) {
				Map<Integer, List<Integer>> documentTypeMap = new HashMap<Integer, List<Integer>>();
				List<Integer> docTypeNotPresentInProp = new ArrayList<Integer>();
				groupJobNumbersIntoMap(catDocsList, jobNumberMaP);
				groupPriorityDocumentMap(documentTypeMap, ruleDetails, documentTypes,docTypeNotPresentInProp);
				filterToCategorizedDocsBasedJobNumber(jobNumberMaP, documentTypeMap, ddpCategorizedDocsList,docTypeNotPresentInProp);
				
			} else {
				ddpCategorizedDocsList.addAll(catDocsList);
			}
		} else {
			ddpCategorizedDocsList.addAll(catDocsList);
		}
	}
	
	/**
	 * Method sued for filtering the categorized documents based on the Job Number.
	 * 
	 * @param jobNumberMap
	 * @param documentTypeMap
	 * @param ddpCategorizedDocsList
	 */
	private void filterToCategorizedDocsBasedJobNumber(Map<String, List<DdpCategorizedDocs>> jobNumberMap,Map<Integer, List<Integer>> documentTypeMap,
			List<DdpCategorizedDocs> ddpCategorizedDocsList,List<Integer> docTypeNotPresentInProp) {
		
		for (String unquieJobNumber : jobNumberMap.keySet()) {
			
			boolean isRequiredDocsAvail = false;
			List<DdpCategorizedDocs> catDocList = jobNumberMap.get(unquieJobNumber);
			
			//0 is mandatory document is is null also consider as all docs adding into list.
			if (catDocList.size() == 1 || documentTypeMap.size() == 0 ||(documentTypeMap.get(0) == null) ) {
				ddpCategorizedDocsList.addAll(catDocList);
				isRequiredDocsAvail = true;
				continue;
			}
			
			//Check for the mandatory documents are available or not.
			for (DdpCategorizedDocs catDocs : catDocList) {
			
				//checking Priority docs; 1 is primary documents.
				List<Integer> rdtList = documentTypeMap.get(1);
				if (rdtList != null && rdtList.contains(catDocs.getCatRdtId()))  {
					ddpCategorizedDocsList.add(catDocs);
					isRequiredDocsAvail =true;
				}
				if (docTypeNotPresentInProp.contains(catDocs.getCatRdtId()))
					ddpCategorizedDocsList.add(catDocs);
			}
			
			//If the mandatory documents are not available then check for optional documents.
			if (!isRequiredDocsAvail) {
			//	ddpCategorizedDocsList.addAll(catDocList);
				for (DdpCategorizedDocs catDocs : catDocList) {
					
					//checking Priority docs; 1 is mandatory documents.
					List<Integer> rdtList = documentTypeMap.get(0);
					if (rdtList != null && rdtList.contains(catDocs.getCatRdtId()))  {
						ddpCategorizedDocsList.add(catDocs);
						isRequiredDocsAvail =true;
					}
				}
			} else {
				List<DdpCategorizedDocs> duplicateList = new ArrayList<DdpCategorizedDocs>();
				for (DdpCategorizedDocs catDocs : catDocList) {
					
					//checking Priority docs; 1 is mandatory documents.
					List<Integer> rdtList = documentTypeMap.get(0);
					if (rdtList != null && rdtList.contains(catDocs.getCatRdtId()))  {
						duplicateList.add(catDocs);
					}
				}
				if (duplicateList.size()  > 0) {
					for (DdpCategorizedDocs docs : duplicateList) {
						docs.setCatStatus(5);
						ddpCategorizedDocsService.updateDdpCategorizedDocs(docs);
					}
				}
			}
		}
	}
	
	/**
	 * Grouping the Priority document map taken from the properties file.
	 * 
	 * @param documentTypeMap
	 * @param ruleDetails
	 * @param documentTypes
	 */
	private void groupPriorityDocumentMap(Map<Integer, List<Integer>> documentTypeMap,List<DdpRuleDetail> ruleDetails,String documentTypes,List<Integer> docTypeNotPresentInProp) {
		
		Map<String,Integer> propertyDocMap = new HashMap<String,Integer>();
		List<String> uniqueDocTypes = new ArrayList<String>();
		String[] documentTypeArray = documentTypes.split(",");
		//finding the unique document type.
		for (DdpRuleDetail rule : ruleDetails) {
			if (!uniqueDocTypes.contains(rule.getRdtDocType().getDtyDocTypeCode().toLowerCase()))
				uniqueDocTypes.add(rule.getRdtDocType().getDtyDocTypeCode().toLowerCase());
		}
		
		//Grouping the Document type into map based on the priority provided in the properties file.
		for (String documentType : documentTypeArray) {
			if (documentType.contains(":")) {
				try {
					String[] priorityDocType = documentType.split(":");
					//Document type : priorityDocType
					if (!uniqueDocTypes.contains(priorityDocType[0].toLowerCase()))
						continue;
					
					propertyDocMap.put(priorityDocType[0].toLowerCase(), Integer.parseInt(priorityDocType[1]));
				} catch (Exception ex) {
					logger.error("DdpRuleSchedulerJob.groupPriorityDocuemnt() - Based on the docuemnt types from properties file : "+documentTypes, ex);
				}
			}
		}
		
		//From the property document map - adding into the document type map key as Priority(configured in properties file) & value as rdt id of rule
		//Written due to For each rule, there can be "n" branches so to make unique identification added this code.
		for (DdpRuleDetail ruleDetail : ruleDetails) {
			if (propertyDocMap.containsKey(ruleDetail.getRdtDocType().getDtyDocTypeCode().toLowerCase())) {
				
				Integer priority = propertyDocMap.get(ruleDetail.getRdtDocType().getDtyDocTypeCode().toLowerCase()); 
				
				if (!documentTypeMap.containsKey(priority)) {
					List<Integer> rdtList = new ArrayList<Integer>();
					rdtList.add(ruleDetail.getRdtId());
					documentTypeMap.put(priority, rdtList);
				} else {
					List<Integer> rdtList = documentTypeMap.get(priority);
					rdtList.add(ruleDetail.getRdtId());
					documentTypeMap.put(priority, rdtList);
				}
			}
		}
		
		//added if document type is not configured in properties file then take it as common.
		for (String documentType : uniqueDocTypes) {
			if (!propertyDocMap.containsKey(documentType.toLowerCase()))
				for (DdpRuleDetail rule : ruleDetails) {
					if (rule.getRdtDocType().getDtyDocTypeCode().equalsIgnoreCase(documentType))
						if (!docTypeNotPresentInProp.contains(rule.getRdtId()))
								docTypeNotPresentInProp.add(rule.getRdtId());
				}
		}
	}
	
	/**
	 * Group JobNumbers into map.
	 * 
	 * @param catDocsList
	 * @param map
	 */
	private void groupJobNumbersIntoMap(List<DdpCategorizedDocs> catDocsList,Map<String, List<DdpCategorizedDocs>> map) {
		
		//From the available cat documents list. Group based on the Job number, so  map key as Job Number & map value is the DdpCategorizedDocs
		for (DdpCategorizedDocs catDoc : catDocsList) {
			List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(catDoc.getCatDtxId().getDtxId());
			
			if (!map.containsKey(ddpDmsDocsDetails.get(0).getDddJobNumber())) {
				
				List<DdpCategorizedDocs> list = new ArrayList<DdpCategorizedDocs>();
				list.add(catDoc);
				map.put(ddpDmsDocsDetails.get(0).getDddJobNumber(), list);
			} else {
				
				List<DdpCategorizedDocs> list = map.get(ddpDmsDocsDetails.get(0).getDddJobNumber());
				list.add(catDoc);
				map.put(ddpDmsDocsDetails.get(0).getDddJobNumber(), list);
			}
		}
	}
	
	/**
	 * Method used for checking the process for job number criteria.
	 * @param clinetName
	 * @return
	 */
	private boolean checkToEnterJobNumberProcess(String clinetName) {
		
		boolean isProcessEnter = false;
		logger.info("DdpRuleScehdulerJob.checkToEnterJobNumberProcess() - Invoked for client name : "+clinetName);
		
		try {
			String clientNames = env.getProperty("export.ruleByClientID.groupJobNumbers.clientNames");
			if (clientNames != null && !clientNames.isEmpty()) {
				
				List<String> clientNameList = new ArrayList<String>();
				String[] clientArray = clientNames.split(",");
				for (String clientName : clientArray) {
					clientNameList.add(clientName.toLowerCase());
				}
				if (clientNameList.contains(clinetName.toLowerCase())) {
					isProcessEnter = true;
				}
			}
		} catch (Exception ex) {
			logger.error("DdpRuleSchedulerJob.checkToEnterJobNumberProcess() - For Client Name : "+clinetName, ex);
		}
			
		return isProcessEnter;
	}
	/**
	 * Method used for getting the matched document name convention.
	 * 
	 * @param expRuleID
	 * @return
	 */
	public DdpDocnameConv getMatchedDocnameCon(Integer expRuleID) {
		
		DdpDocnameConv convName = null;
		try {
			List<Integer> convLIstID =	this.jdbcTemplate.queryForList(Constant.DEF_SQL_SELECT_MATCHED_CONV_ID,new Object[]{expRuleID},Integer.class); 
			
			if (convLIstID != null &&convLIstID.size() > 0) {
				convName = ddpDocnameConvService.findDdpDocnameConv(convLIstID.get(0));
			}
		} catch(Exception ex) {
			logger.error("DdpRuleSchedulerJob.getMatchedDocnameCon() - Unable to find Naming convension for Rule id : "+expRuleID, ex);
		}
		return convName;
	}
	
	/**
	 * 
	 * @param expRuleID
	 * @return {@link DdpCommunicationSetup}
	 */
	public DdpCommunicationSetup getMatchedCommunicationSetup(Integer expRuleID) {
		
		DdpCommunicationSetup setup = null;
		
		logger.debug("DdpRuleSchedulerJob.getMatchedCommunicationSetup() method invoked");
		try {
			
			List<DdpCommunicationSetup> commSetupList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_COMMUNICATION_SET_ID, new Object[]{expRuleID},new RowMapper<DdpCommunicationSetup>(){
				@Override
				public DdpCommunicationSetup mapRow(ResultSet rs, int rowNum) throws SQLException {
					//int commFTPId = rs.getInt("CFT_FTP_ID");
					DdpCommunicationSetup ddpCommunicationSetup = ddpCommunicationSetupService.findDdpCommunicationSetup(rs.getInt("CMS_COMMUNICATION_ID"));
					return ddpCommunicationSetup;
				}
			});
					
			if (commSetupList != null && commSetupList.size() > 0)
				setup = commSetupList.get(0);
		} catch (Exception ex) {
			logger.error("DdpRuleSchedulerJob.getMatchedCommunicationSetup() - Exception while retrieving Matched  Export id - Error message [{}].",ex.getMessage());
			//ex.printStackTrace();
		}
		
		logger.info("DdpRuleSchedulerJob.getMatchedCommunicationSetup() executed successfully.");
		return setup;
		
	}
	
	/**
	 * Method used for getting the matched FTP details based on the export rule ID.
	 * 
	 * @param expRuleID
	 * @return {@link DdpCommFtp}
	 */
	public DdpCommFtp getMatchedFTPDetails(Integer expRuleID) {
		
		DdpCommFtp commFtp = null;
		logger.debug("DdpRuleSchedulerJob.getMatchedFTPDetails() method invoked"); 
		try {
			List<DdpCommFtp> commFTPList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_EXPORT_RULE_COMM_ID, new Object[]{expRuleID},new RowMapper<DdpCommFtp>(){
				@Override
				public DdpCommFtp mapRow(ResultSet rs, int rowNum) throws SQLException {
					//int commFTPId = rs.getInt("CFT_FTP_ID");
					DdpCommFtp ddpCommFtp = ddpCommFtpService.findDdpCommFtp(Long.parseLong(rs.getInt("CFT_FTP_ID")+""));
					return ddpCommFtp;
				}
			});
					
			if (commFTPList != null && commFTPList.size() > 0)
				commFtp = commFTPList.get(0);
		} 
		catch(Exception ex)
        {
        	logger.error("DdpRuleSchedulerJob.getMatchedFTPDetails() - Exception while retrieving Matched  Export id - Error message [{}].",ex.getMessage());
        	//ex.printStackTrace();
        }
		
		logger.info("DdpRuleSchedulerJob.getMatchedFTPDetails() executed successfully.");
		
		return commFtp;
	}
	
	/**
	 * Method used for getting the matched UNC Details based on the export rule id.
	 * 
	 * @param expRuleID
	 * @return {@link DdpCommUnc}
	 */
	public DdpCommUnc getMatchedUNCDetails(Integer expRuleID) {
		
		DdpCommUnc commUNC = null;
		
		logger.debug("DdpRuleSchedulerJob.getMatchedUNCDetails() method invoked"); 
		try {
			List<DdpCommUnc> commUNCList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_EXPORT_RULE_UNC_COMM_ID, new Object[]{expRuleID},new RowMapper<DdpCommUnc>(){
				@Override
				public DdpCommUnc mapRow(ResultSet rs, int rowNum) throws SQLException {
					//int commFTPId = rs.getInt("CFT_FTP_ID");
					DdpCommUnc ddpCommUNC = ddpCommUncService.findDdpCommUnc(Long.parseLong(rs.getInt("CUN_UNC_ID")+""));
					return ddpCommUNC;
				}
			});
					
			if (commUNCList != null && commUNCList.size() > 0)
				commUNC = commUNCList.get(0);
		} 
		catch(Exception ex)
        {
        	logger.error("DdpRuleSchedulerJob.getMatchedUNCDetails() - Exception while retrieving Matched  Export id - Error message [{}].",ex.getMessage());
        	//ex.printStackTrace();
        }
		
		logger.info("DdpRuleSchedulerJob.getMatchedUNCDetails() executed successfully.");
		
		
		return commUNC;
	}
	
	public DdpDmsDocsDetail getddpDmsDocsDetails(Integer tnx_id) {
		
		DdpDmsDocsDetail dmsDocsDetail = null;
		logger.debug("DdpRuleSchedulerJob.getddpDmsDocsDetails() method invoked");
		try {
			List<DdpDmsDocsDetail> dmsDocsList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_DMS_DOCS_DETAILS, new Object[]{tnx_id},new RowMapper<DdpDmsDocsDetail>(){

				@Override
				public DdpDmsDocsDetail mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					DdpDmsDocsDetail ddpDmsDocsDetail = ddpDmsDocsDetailService.findDdpDmsDocsDetail(rs.getInt("DDD_ID"));
					return ddpDmsDocsDetail;
				}
				
			});
			if (dmsDocsList != null && dmsDocsList.size() > 0)
				dmsDocsDetail = dmsDocsList.get(0);
			
		} catch (Exception ex) {
			logger.error("DdpRuleSchedulerJob.getddpDmsDocsDetails() - Exception while retrieving Matched  Transaction id - Error message [{}].",ex.getMessage());
			ex.printStackTrace();
		}
		logger.info("DdpRuleSchedulerJob.getddpDmsDocsDetails() executed successfully.");
		return dmsDocsDetail;
	}
	
	
	
	public List<Integer> getMatchedSchdIDAndRuleDetailForExport(Integer intSchId)
    {
		logger.debug("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() method invoked.");
                   
		List<Integer> lsRdtIds = null;
                    
		try
        {
			lsRdtIds = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_SCH_RULE_DET_EXPORT, new Object[] {intSchId}, new RowMapper<Integer>() {
            
				public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					int rdtId = rs.getInt("RDT_ID");
					return rdtId;
				}
			});
		}
        catch(Exception ex)
        {
        	logger.error("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() - Exception while retrieving Matched Schduler ID and Rule Detail ID for Export rules - Error message [{}].",ex.getMessage());
        	ex.printStackTrace();
        }
        
        logger.info("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() executed successfully.");
        return lsRdtIds;
    }
	
	//NEED TO IMPLEMENT
	public List<DdpCategorizedDocs> getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate)
    {
		logger.debug("DdpRuleSchedulerJob.getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate) method invoked.");
		
		List<DdpCategorizedDocs> ddpCategorizationdocsList = null;
		
		try
		{
			ddpCategorizationdocsList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_CAT_DOCS, new Object[] { intSchId, queryStartDate, queryEndDate }, new RowMapper<DdpCategorizedDocs>() {
							
				public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
					
					return ddpCategorizedDocs;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			logger.error("DdpRuleSchedulerJob.getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate) - Exception while retrieving Matched CategorizedDoc for scheduler [{}]."+intSchId, ex);
			//System.out.println(ex);
			//ex.printStackTrace();
		}
		
		logger.debug("DdpRuleSchedulerJob.getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate) executed successfully.");
		return ddpCategorizationdocsList;
    }
	
	
	
	/**
	 * Method used for OnDemand Services based on JobNumbers, ConsignmentIDs & DocRefs.
	 * 
	 * @param query
	 * @param ids
	 * @param intSchId
	 * @param queryStartDate
	 * @param queryEndDate
	 * @return
	 */
	public List<DdpCategorizedDocs> getMatchedCatDocs(String query,String ids,Integer intSchId, Calendar queryStartDate, Calendar queryEndDate)
	    {
			logger.debug("DdpRuleSchedulerJob.getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate) method invoked.");
			
			List<DdpCategorizedDocs> ddpCategorizationdocsList = null;
			
			try
			{
				query = query.replaceAll("dynamiccondition", ids);
				ddpCategorizationdocsList = this.jdbcTemplate.query(query, new Object[]{intSchId,queryStartDate,queryEndDate}, new RowMapper<DdpCategorizedDocs>() {
								
					public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum) throws SQLException {
						
						DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
						
						return ddpCategorizedDocs;
			           }
				});		
				
			}
			catch(Exception ex)
			{
				logger.error("DdpRuleSchedulerJob.getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate) - Exception while retrieving Matched CategorizedDoc for scheduler [{}]."+intSchId, ex);
				//System.out.println(ex);
				//ex.printStackTrace();
			}
			
			logger.debug("DdpRuleSchedulerJob.getMatchedCatDocs(Integer intSchId, Calendar queryStartDate, Calendar queryEndDate) executed successfully.");
			return ddpCategorizationdocsList;
	    }
	
	
	
	/**
	 * Method used for copying the file to destination location.
	 * 
	 * @param sourceFile
	 * @param destFile
	 * @return
	 */
	private boolean copyFile(File sourceFile, File destFile)  {
			
		boolean isFileCopied = false;
		FileChannel source = null;
		FileChannel destination = null;
		
		try {
			if (!sourceFile.exists()) {
				return false;
			}
			if (!destFile.exists()) {
				destFile.createNewFile();
			}
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			if (destination != null && source != null) {
				destination.transferFrom(source, 0, source.size());
			}
			
			isFileCopied = true;
		} catch(Exception ex) {
			logger.error("DdpRuleSchedulerJob.copyFile() : Source file does not exists / due crash while copying into destiion folder ",ex.getMessage());
			ex.printStackTrace();
		} finally {
			
			if (source != null) {
				try {
					source.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (destination != null) {
				try {
					destination.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		
		}
		return isFileCopied;

	}
	
	
	/**
	 * Method used for transferring the files to local folder.
	 * 
	 * @param docs
	 * @param today
	 * @param isAllVersions
	 * @param tempFolderPath
	 */
	private void transferFileIntoLocalFolder(DdpCategorizedDocs docs,Calendar today,boolean isAllVersions,String tempFolderPath,IDfSession session,String startDate,String endDate,
			StringBuffer exportedDetails,List<String> fileNameList,int typeOfService,List<DdpExportSuccessReport> exportedReports) {
		
		logger.debug("DdpRuleSchedulerJob.transferFileIntoLocalFolder() is invoked");
		
		DdpDmsDocsDetail dmsDocsDetails = getddpDmsDocsDetails(docs.getCatDtxId().getDtxId());
		//checking version name 
		DdpDocnameConv convName  = getMatchedDocnameCon(docs.getCatRulId().getRulId());
		
	   	if (dmsDocsDetails == null) {
			logger.info("DdpRuleSchedulerJob.transferFileIntoLocalFolder is empty : "+ dmsDocsDetails);
			return;
		}
	   	String tempFilePath = env.getProperty("ddp.export.folder");
	   	
	   	Map<String, String> loadNumberMap = commonUtil.getGenericLoadNumberMap(convName, "ruleByClientID", env, dmsDocsDetails.getDddJobNumber(), dmsDocsDetails.getDddConsignmentId());
	  //check the file local repository else dms repository & copy in to temp folder
	   	File dmsDestDoc  = null;
	   	File dmsSourceDoc = new File(tempFilePath+"/"+(docs.getCatDtxId().getDtxId()+"_"+dmsDocsDetails.getDddObjectName()));
	  //latest version & Naming Convention filed.
	   	if (!isAllVersions && convName != null && convName.getDcvGenNamingConv() != null && !convName.getDcvGenNamingConv().isEmpty()) {	   	
	   		String ext = ".";
	   		if ( dmsDocsDetails.getDddObjectName() != null && dmsDocsDetails.getDddObjectName().contains(".")) {
		   		String [] fileNames = dmsDocsDetails.getDddObjectName().split("\\.");
				 ext = ext +fileNames[1];
	   		} else {
	   			ext = ".pdf"; 
	   		}
	   		dmsDestDoc = new File(tempFolderPath+"/"+NamingConventionUtil.getDocName(convName.getDcvGenNamingConv(), dmsDocsDetails,"001",loadNumberMap)+ext);
	   	} else {	
	   		dmsDestDoc = new File(tempFolderPath+"/"+dmsDocsDetails.getDddObjectName());
	  	}
	   	
	   	if (!isAllVersions) {
		   	boolean isFileCopied = copyFile(dmsSourceDoc, dmsDestDoc); 
		   	
		   	if (isFileCopied && !fileNameList.contains(dmsDestDoc.getName())) {
		   		constructFileName(isFileCopied, exportedDetails, dmsDestDoc,dmsDocsDetails);
		   		exportedReports.add(commonUtil.constructExportReportDomainObject(docs.getCatId(), docs.getCatRulId().getRulId(), dmsDocsDetails.getDddJobNumber(), 
		   				dmsDocsDetails.getDddConsignmentId(), "Rule By ClientID", dmsDestDoc.getName(), (int)FileUtils.findSizeofFileInKB(dmsDestDoc), typeOfService,
		   				dmsDocsDetails.getDddDocRef(), dmsDocsDetails.getDddMasterJobNumber(), docs.getCatCreatedDate()));
		   		fileNameList.add(dmsDestDoc.getName());
		   	}
		   		
		   	if (!isFileCopied)
		   		isFileCopied = ddpDFCClientComponent.downloadDocumentFromDFCWithOutCatID(tempFolderPath, dmsDocsDetails,session,exportedDetails,fileNameList,dmsDocsDetails,convName,typeOfService,exportedReports,docs,loadNumberMap);
		   	
		   	if (!isFileCopied)
		   		logger.info("DdpRuleSchedulerJob. "+dmsDocsDetails.getDddObjectName()+" file is not copied due to not available in DFC Client ");
	   	}else {
	   		boolean isFileCopied = ddpDFCClientComponent.downloadAllDocumentBasedOnVersions(tempFolderPath, dmsDocsDetails, session,convName,startDate,endDate,exportedDetails,fileNameList,typeOfService,exportedReports,docs,loadNumberMap);
	   		if (!isFileCopied)
		   		logger.info("DdpRuleSchedulerJob. "+dmsDocsDetails.getDddObjectName()+" file is not copied due to not available in DFC Client for all Versions ");
	   	}
	   	
	   	logger.debug("DdpRuleSchedulerJob.transferFileIntoLocalFolder() is successfully executed...");
	}
	
	
	/**
	 * Method used for running the onDemand service when click on the run button.
	 * 
	 * @param exportRuleID
	 * @return
	 */
	public boolean runOnDemandRuleJob(Integer exportRuleID) {
		
		logger.debug("DdpRuleSchedulerJob.runOnDemandRuleJob(String exportRuleID) is invoked...");
		boolean isJobExecuted = false;
//		Integer ruleID = null;
//		try {
//			ruleID = Integer.parseInt(exportRuleID);
//		} catch(Exception ex) {
//			logger.error("DdpRuleSchedulerJob.runOnDemandRuleJob(Integer exportRuleID) - Exception while converting the string exportRuleID to Integer [{}].", ex.getMessage());
//		}
		
		if (exportRuleID == null)
			return isJobExecuted;
		
		DdpScheduler ddpScheduler = getSchedulerDetails(exportRuleID);
		
		if(ddpScheduler == null)
			return isJobExecuted;
		//for running the every hourly based.
		//ddpScheduler.setSchCronExpressions("0 0 0/1 * * ?");
		
		isJobExecuted = executeSchedulerJob(ddpScheduler,2);
		
		logger.debug("DdpRuleSchedulerJob.runOnDemandRuleJob(String exportRuleID) is successfully executed...");
		
		return isJobExecuted;
	}
	
	
	/**
	 * Method used for getting the Scheduler details based on the export rule id.
	 * 
	 * @param exportRuleID
	 * @return
	 */
	public DdpScheduler getSchedulerDetails(Integer exportRuleID) {
	
		logger.debug("DdpRuleSchedulerJob.getSchedulerDetails(Integer exportRuleID) is invoked...");
		
		DdpScheduler ddpScheduler = null;
		try {
			List<DdpScheduler> ddpSchedulers = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_SCHEDULER_ID, new Object[] { exportRuleID }, new RowMapper<DdpScheduler>() {
				
				public DdpScheduler mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpScheduler scheduler = ddpSchedulerService.findDdpScheduler(rs.getInt("EXP_SCHEDULER_ID"));
					
					return scheduler;
		           }
			});		
			
			if (ddpSchedulers != null && ddpSchedulers.size() > 0) {
				ddpScheduler = ddpSchedulers.get(0);
			}
		}
		catch(Exception ex)
		{
			logger.error("DdpRuleSchedulerJob.getSchedulerDetails(Integer exportRuleID) - Exception while retrieving Matched DDPSheduler for scheduler [{}].", ex.getMessage());
			System.out.println(ex);
			//ex.printStackTrace();
		}
		
		logger.debug("DdpRuleSchedulerJob.getSchedulerDetails(Integer exportRuleID) is successfully executed...");
		return ddpScheduler;
	}

	@Override
	public void runOnDemandRuleJob(final DdpScheduler ddpScheduler,final Calendar fromDate,final Calendar toDate) {
		
		if(ddpScheduler!= null) {
		//for running the every hourly based.
		//ddpScheduler.setSchCronExpressions("0 0 0/1 * * ?");
		
		final Calendar today = Calendar.getInstance();
	
		
		Runnable r = new Runnable() {
	         public void run() {
	        	 try {
	        		 commonUtil.addExportSetupThread("RuleByClientID - onDemandRuleJob :"+ddpScheduler.getSchId());
	        		 executeOnDemandScheduler(ddpScheduler, fromDate, toDate, today,2,null,null,null);
	        	 } catch (Exception ex) {
	        		 logger.error("DdpRuleSchedulerJob.runOnDemandRuleJob() unable to run for scheduler ID :  "+ddpScheduler.getSchId(), ex);
	        	 } finally {
	        		 commonUtil.removeExportSetupThread("RuleByClientID - onDemandRuleJob :"+ddpScheduler.getSchId());
	        	 }
	        	 
	         }
	     };

	     ExecutorService executor = Executors.newCachedThreadPool();
	     executor.submit(r);
		}
		
		logger.debug("DdpRuleSchedulerJob.runOnDemandRuleJob(String exportRuleID) is successfully executed...");
	}

	@Override
	public void reprocesRuleJob(String appName, Calendar currentDate,Calendar startDate) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Method used for constructing file name.
	 *  
	 * @param isFileCopied
	 * @param exportedDetails
	 * @param dmsDestDoc
	 */
	private void constructFileName(boolean isFileCopied,StringBuffer exportedDetails,File dmsDestDoc,DdpDmsDocsDetail dmsDocsDetail) {
		
		if (isFileCopied) {
	   		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
	   		exportedDetails.append(format.format(new Date())+",");
			format.applyLocalizedPattern("HH:mm:ss");
			exportedDetails.append(format.format(new Date())+",receive,success,"+FileUtils.findSizeofFileInKB(dmsDestDoc)+",00:00:01,"+dmsDocsDetail.getDddConsignmentId()+","+dmsDocsDetail.getDddJobNumber()+","+dmsDestDoc.getName()+"\n");
	   	}
	}
	
	/**
	 * Method used for sending the success mail details.
	 * @param exportRule
	 * @param clientID
	 * @param currentDate
	 * @param startDate
	 * @param tableBody
	 * @param hostName
	 * @param destLoc
	 * @param tmpFolder
	 * @param presentDate
	 * @param endTime
	 */
	private void sendMailForSuccessExport(DdpExportRule exportRule,String clientID,Calendar currentDate,Calendar startDate,StringBuffer tableBody,String tmpFolder,Calendar presentDate,Date endTime,int typeOfService,List<DdpTransferObject> ddpTransferObjects,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		
		try {
			clientID = (clientID.length() > 15) ? clientID.substring(0, 15) : clientID ;
			String hostName = "";
			String destLoc = "";
			if (ddpTransferObjects != null && ddpTransferObjects.size() > 0) {
				hostName = ddpTransferObjects.get(0).getHostName();
				destLoc = ddpTransferObjects.get(0).getDestLocation();
			}
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			String currDate = dateFormat.format(currentDate.getTime());
			FileWriter csvFile = null;
			File sucessFolderFile = null;
			
			if (tableBody.length() > 0) {
				String successFolder = tmpFolder + "/SUCCESS_FOLDER";
				sucessFolderFile = FileUtils.createFolder(successFolder);
				
				csvFile = new FileWriter(successFolder+"/ExportedDocs-"+clientID.replaceAll("[^a-zA-Z0-9_.]", "")+"-"+currDate+".csv");
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
			}
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByClientID."+typeOfService);
			
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotSuccessEmailAddress();
			String ccAddress = "";
			//String ccAddress = notification.getNotFailureEmailAddress();
			String fromAddress = env.getProperty("export.ruleByClientID.fromAddress");
			String subject = env.getProperty("export.ruleByClientID.subject");
			
			if (tableBody.length() > 0)
				subject = subject.replace("%%STATUS%%", "Successfully");
			else 
				subject = subject.replace("%%STATUS%%", "");
			
			subject = subject.replace("%%DDPClIENTID%%", clientID);
			String body = env.getProperty("export.ruleByClientID.success.body");
			//System.out.println("tableBODY : "+tableBody);
			//body = body.replace("%%TABLEDETAILS%%", tableBody);
			body = body.replaceAll("%%DDPClIENTID%%", clientID);			
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%DESTIONPATH%%", destLoc);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			body = body.replace("%%COUNT%%", FileUtils.countCharacter(tableBody.toString(), '\n')+"");
			body = body.replace("%%ENDTIME%%", dateFormat.format(endTime));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endTime)+"");
			
			DdpRule ddpRule = null;
			if(exportRule !=null )
				ddpRule = ddpRuleServive.findDdpRule(exportRule.getExpRuleId());
			body = body.replaceAll("%%DDPEXPORTRULENAME%%", (ddpRule == null ? "" : ddpRule.getRulDescription().trim()));
						
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
			logger.error("DdpRuleSchedulerJob-sendMailForSuccessExoprot() - Unable to send the success mail for the Rule ID : "+exportRule.getExpRuleId(), ex);
		}
		
	}
	
	/**
	 * Method used for sending the success mail details.
	 * @param exportRule
	 * @param clientID
	 * @param currentDate
	 * @param startDate
	 * @param tableBody
	 * @param hostName
	 * @param destLoc
	 * @param tmpFolder
	 * @param presentDate
	 * @param endTime
	 */
	private void sendMailForIssueExport(DdpExportRule exportRule,String clientID,Calendar currentDate,Calendar startDate,Calendar presentDate,Date endTime,int typeOfService,List<DdpTransferObject> ddpTransferObjects,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		
		try {
			
			String hostName = "";
			String destLoc = "";
			if (ddpTransferObjects != null && ddpTransferObjects.size() > 0) {
				hostName = ddpTransferObjects.get(0).getHostName();
				destLoc = ddpTransferObjects.get(0).getDestLocation();
			}
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			//String currDate = dateFormat.format(currentDate.getTime());
		
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByClientID."+typeOfService);
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = env.getProperty("export.ruleByClientID.fromAddress");
			String subject = env.getProperty("export.ruleByClientID.subject");
			
			subject = subject.replace("%%STATUS%%", "Transfer protocol issue unable to ");
			
			subject = subject.replace("%%DDPClIENTID%%", clientID);
			String body = env.getProperty("export.ruleByClientID.success.body");
			//System.out.println("tableBODY : "+tableBody);
			//body = body.replace("%%TABLEDETAILS%%", tableBody);
			body = body.replaceAll("%%DDPClIENTID%%", clientID);
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%DESTIONPATH%%", destLoc);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			body = body.replace("%%ENDTIME%%", dateFormat.format(endTime));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endTime)+"");
			body = body.replace("%%REASON%%", "Unable to export documents to destination location for the below details.");
			body = body.replace("%%COUNT%%", 0+"");
			
			DdpRule ddpRule = null;
			if(exportRule !=null )
				ddpRule = ddpRuleServive.findDdpRule(exportRule.getExpRuleId());
			body = body.replaceAll("%%DDPEXPORTRULENAME%%", (ddpRule == null ? "" : ddpRule.getRulDescription().trim()));
			
			String transferBody = "";
			if (ddpTransferObjects.size() > 1) {
				
				for (int i = 1; i < ddpTransferObjects.size(); i++) {
					
					DdpTransferObject transferObject = ddpTransferObjects.get(i);
					
					transferBody += "<tr><td colspan='2' align='center'><B>Transfers Location-"+(i+1)+"<B></td>"
							+ "</tr><tr><td align='center'><B>Folder</B></td><td>"+transferObject.getDestLocation()+"</td></tr>"
							+ "<tr><td align='center'><B>Hosts/Mailboxes</B></td><td>"+transferObject.getHostName()+"</td></tr>"
							+ "<tr><td align='center'><B>Directions</B></td><td>receive</td></tr>"
							+ "<tr><td align='center'><B>Total Records Exported</B></td><td>0</td></tr>";
					
				}
			}
			body = body.replaceAll("%%TRANSFERLOCATION%%", transferBody);
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValues+"<br/>");
			} else {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
			}
				taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, null);
		} catch (Exception ex) {
			logger.error("DdpRuleSchedulerJob-sendMailForSuccessExoprot() - Unable to send the success mail for the Rule ID : "+exportRule.getExpRuleId(), ex);
		}
		
	}
	
	/**
	 * Method used for sending the failure mails.
	 * 
	 * @param clientID
	 * @param missingCatDocs
	 * @param exportRule
	 * @param tmpFolder
	 * @param currentDate
	 * @param startDate
	 * @param presentDate
	 * @param endDate
	 * @param typeOfService
	 */
	private void sendMailForMissingDocuments(String clientID,List<DdpCategorizedDocs> missingCatDocs,DdpExportRule exportRule,String tmpFolder,Calendar currentDate,Calendar startDate,Calendar presentDate,Date endDate,int typeOfService,String dynamicValue) {
		
		DdpNotification notification = exportRule.getExpNotificationId();
		logger.info("DdpBackUpDocumentProcess-sendMailForMissingDocuments() - Mail ID : "+notification.getNotSuccessEmailAddress()+" Failure : "+notification.getNotFailureEmailAddress());
		String errorFolder = tmpFolder + "/ERROR_FOLDER";
		File errorFolderFile = FileUtils.createFolder(errorFolder);
		clientID = (clientID.length()>15)? clientID.substring(0, 15) : clientID ;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat();
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
			FileWriter csvFile = new FileWriter(errorFolder+"/Missing-"+clientID.replaceAll("[^a-zA-Z0-9_.]", "")+"_"+dateFormat.format(currentDate.getTime())+".csv");
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
			
			for (DdpCategorizedDocs missingDoc : missingCatDocs) {
				
				DdpDmsDocsDetail dmsDocsDetails = getddpDmsDocsDetails(missingDoc.getCatDtxId().getDtxId());
				
				if (dmsDocsDetails != null ) {
					csvFile.append(dmsDocsDetails.getDddDocRef());
					csvFile.append(",");
					csvFile.append(dmsDocsDetails.getDddMasterJobNumber());
					csvFile.append(",");
					csvFile.append(dmsDocsDetails.getDddJobNumber());
					csvFile.append(",");
					csvFile.append(dmsDocsDetails.getDddConsignmentId());
					csvFile.append(",");
					csvFile.append(dmsDocsDetails.getDddControlDocType());
					csvFile.append("\n");
				
				}
			}
			csvFile.flush();
			csvFile.close();
			
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress(); 
			String ccAddress = "";
			String fromAddress = env.getProperty("export.ruleByClientID.fromAddress");
			String subject = env.getProperty("export.ruleByClientID.subject");
			subject = subject.replace("%%STATUS%%", "Failure to ");
			subject = subject.replace("%%DDPClIENTID%%", clientID);
			String body = env.getProperty("export.ruleByClientID.failure.body");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByClientID."+typeOfService);
			
			body = body.replaceAll("%%DDPClIENTID%%", clientID);
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%ENDTIME%%", dateFormat.format(endDate));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endDate));
			
			if (toAddress == null || toAddress.length()== 0)
				toAddress = env.getProperty("mail.toAddress");
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValue+"<br/>");
			} else {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
			}
			
			taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, errorFolderFile);
		} catch (IOException ex) {
			logger.error("DdpRuleSchedulerJob - sendMailForMissingDocuments() : Unable to send the mails or create csv file. For the rule id : "+exportRule.getExpRuleId(), ex);
		}
		
	}

	/**
	 * Method used for seding missing document details.
	 * 
	 * @param clientID
	 * @param missingDocs
	 * @param exportRule
	 * @param tmpFolder
	 * @param currentDate
	 * @param startDate
	 * @param presentDate
	 * @param endDate
	 * @param typeOfService
	 */
	private void sendMailForMissingDocumentsOnDemand(String clientID,List<DdpExportMissingDocs> missingDocs,DdpExportRule exportRule,String tmpFolder,Calendar currentDate,Calendar startDate,Calendar presentDate,Date endDate,int typeOfService,String dynamicValues) {
		
		DdpNotification notification = exportRule.getExpNotificationId();
		logger.info("DdpBackUpDocumentProcess-sendMailForMissingDocuments() - Mail ID : "+notification.getNotSuccessEmailAddress()+" Failure : "+notification.getNotFailureEmailAddress());
		String errorFolder = tmpFolder + "/ERROR_FOLDER";
		File errorFolderFile = FileUtils.createFolder(errorFolder);
		clientID = (clientID.length()>15)? clientID.substring(0, 15): clientID ;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat();
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
			FileWriter csvFile = new FileWriter(errorFolder+"/Missing-"+clientID.replaceAll("[^a-zA-Z0-9_.]", "")+"_"+dateFormat.format(currentDate.getTime())+".csv");
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
			
			for (DdpExportMissingDocs missingDoc : missingDocs) {
				
				csvFile.append(missingDoc.getMisEntryType());
				csvFile.append(",");
				csvFile.append(missingDoc.getMisMasterJob());
				csvFile.append(",");
				csvFile.append(missingDoc.getMisJobNumber());
				csvFile.append(",");
				csvFile.append(missingDoc.getMisConsignmentId());
				csvFile.append(",");
				csvFile.append(missingDoc.getMisDocType());
				csvFile.append("\n");
				
			}
			csvFile.flush();
			csvFile.close();
			
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress(); 
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = env.getProperty("export.ruleByClientID.fromAddress");
			String subject = env.getProperty("export.ruleByClientID.subject");
			subject = subject.replace("%%STATUS%%", "Failure to ");
			subject = subject.replace("%%DDPClIENTID%%", clientID);
			String body = env.getProperty("export.ruleByClientID.failure.body");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByClientID."+typeOfService);
			
			body = body.replaceAll("%%DDPClIENTID%%", clientID);
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
			logger.error("DdpRuleSchedulerJob - sendMailForMissingDocuments() : Unable to send the mails or create csv file. For the rule id : "+exportRule.getExpRuleId(), ex);
		}
		
	}


	@Override
	public void runOnDemandRuleBasedJobNumber(final DdpScheduler ddpScheduler,
			final Calendar fromDate, final Calendar toDate,final String jobNumbers) {
		
		if(ddpScheduler!= null) {
			//for running the every hourly based.
			//ddpScheduler.setSchCronExpressions("0 0 0/1 * * ?");
			
			final Calendar today = Calendar.getInstance();
			try {
	    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
	    		 Scheduler scheduler = env.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
	    		 
	    		 jdfb.setTargetObject(this);
		         jdfb.setTargetMethod("onDemandScheduler");
		         jdfb.setArguments(new Object[]{ddpScheduler, fromDate, toDate, today,4,jobNumbers,null,null,scheduler});
		         jdfb.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
		         jdfb.afterPropertiesSet();
		         JobDetail jd = (JobDetail)jdfb.getObject();
		         
		         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemand-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(1000L);
		        
		         scheduler.scheduleJob(jd, trigger);
		     	 scheduler.start();
		     	 
	        	} catch (Exception ex) {
	        		logger.error("DdpRuleSchedulerJob.runOnDemandRuleBasedJobNumber(String exportRuleID)Error occurried while running the ondemand service", ex);
	        	}
			
//			Runnable r = new Runnable() {
//		         public void run() {
//		        	 try {
//		        		 commonUtil.addExportSetupThread("RuleByClientID - OnDemandJobNumber :"+ddpScheduler.getSchId());
//		        		 executeOnDemandScheduler(ddpScheduler, fromDate, toDate, today,4,jobNumbers,null,null);
//		        	 } catch (Exception ex) {
//		        		 logger.error("DdpRuleSchedulerJob.runOnDemandRuleBasedJobNumber() - Unbale to execute OnDemandRuleBased By JobNumber for Scheduler : "+ddpScheduler.getSchId(),ex);
//		        	 } finally {
//		        		 commonUtil.removeExportSetupThread("RuleByClientID - OnDemandJobNumber :"+ddpScheduler.getSchId());
//		        	 }
//		         }
//		     };
//
//		     ExecutorService executor = Executors.newCachedThreadPool();
//		     executor.submit(r);
			}
			
			logger.debug("DdpRuleSchedulerJob.runOnDemandRuleBasedJobNumber(String exportRuleID) is successfully executed...");
	}

	@Override
	public void runOnDemandRuleBasedConsignmentId(final DdpScheduler ddpScheduler,
			final Calendar fromDate,final Calendar toDate,final String consignmentIDs) {
		
		if(ddpScheduler!= null) {
			//for running the every hourly based.
			//ddpScheduler.setSchCronExpressions("0 0 0/1 * * ?");
			
			final Calendar today = Calendar.getInstance();
			try {
	    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
	    		 Scheduler scheduler = env.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
	    		 
	    		 jdfb.setTargetObject(this);
		         jdfb.setTargetMethod("onDemandScheduler");
		         jdfb.setArguments(new Object[]{ddpScheduler, fromDate, toDate, today,5,null,consignmentIDs,null,scheduler});
		         jdfb.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
		         jdfb.afterPropertiesSet();
		         JobDetail jd = (JobDetail)jdfb.getObject();
		         
		         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemand-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(1000L);
		        
		         scheduler.scheduleJob(jd, trigger);
		     	 scheduler.start();
		     	 
	        	} catch (Exception ex) {
	        		logger.error("DdpRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(String exportRuleID)Error occurried while running the ondemand service", ex);
	        	}
//			
//			Runnable r = new Runnable() {
//		         public void run() {
//		        	 try {
//		        		 commonUtil.addExportSetupThread("RuleByClientID - OnDemandConsignmentID :"+ddpScheduler.getSchId());
//		        		 executeOnDemandScheduler(ddpScheduler, fromDate, toDate, today,5,null,consignmentIDs,null);
//		        	 } catch (Exception ex) {
//		        		 logger.error("DdpRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() unable to run for consignment ID "+ddpScheduler.getSchId(), ex);
//		        	 } finally {
//		        		 commonUtil.removeExportSetupThread("RuleByClientID - OnDemandConsignmentID :"+ddpScheduler.getSchId());
//		        	 }
//		         }
//		     };
//
//		     ExecutorService executor = Executors.newCachedThreadPool();
//		     executor.submit(r);
			}
			
			logger.debug("DdpRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(String exportRuleID) is successfully executed...");
		
	}

	@Override
	public void runOnDemandRuleBasedDocRef(final DdpScheduler ddpScheduler,
			final Calendar fromDate,final Calendar toDate,final String docRefs) {
		
		if(ddpScheduler!= null) {
			//for running the every hourly based.
			//ddpScheduler.setSchCronExpressions("0 0 0/1 * * ?");
			
			final Calendar today = Calendar.getInstance();
		
			try {
	    		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
	    		 Scheduler scheduler = env.getQuartzScheduler("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
	    		 
	    		 jdfb.setTargetObject(this);
		         jdfb.setTargetMethod("onDemandScheduler");
		         jdfb.setArguments(new Object[]{ddpScheduler, fromDate, toDate, today,6,null,null,docRefs,scheduler});
		         jdfb.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
		         jdfb.afterPropertiesSet();
		         JobDetail jd = (JobDetail)jdfb.getObject();
		         
		         SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemand-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(1000L);
		        
		         scheduler.scheduleJob(jd, trigger);
		     	 scheduler.start();
		     	 
	        	} catch (Exception ex) {
	        		logger.error("DdpRuleSchedulerJob.runOnDemandRuleBasedConsignmentId(String exportRuleID)Error occurried while running the ondemand service", ex);
	        	}
			
//			Runnable r = new Runnable() {
//		         public void run() {
//		        	  try {
//			        		 commonUtil.addExportSetupThread("RuleByClientID - OnDemandDocRef :"+ddpScheduler.getSchId());
//			        		 executeOnDemandScheduler(ddpScheduler, fromDate, toDate, today,6,null,null,docRefs);
//			        	 } catch (Exception ex) {
//			        		 logger.error("DdpRuleSchedulerJob.runOnDemandRuleBasedDocRef() unable to run for scheduler ID :  "+ddpScheduler.getSchId(), ex);
//			        	 } finally {
//			        		 commonUtil.removeExportSetupThread("RuleByClientID - OnDemandDocRef :"+ddpScheduler.getSchId());
//			        	 }
//		         }
//		     };
//
//		     ExecutorService executor = Executors.newCachedThreadPool();
//		     executor.submit(r);
			}
			
			logger.debug("DdpRuleSchedulerJob.runOnDemandRuleBasedDocRef(String exportRuleID) is successfully executed...");
		
	}

	@Override
	public File runOnDemandRulForReports(final DdpScheduler ddpScheduler,
			final Calendar fromDate,final Calendar toDate,String typeOfStatus) {
	
		File reportsGenerated = null;
		if(ddpScheduler!= null) {
			//for running the every hourly based.
									
//			Runnable r = new Runnable() {
//		         public void run() {
		        	  try {
			        		 commonUtil.addExportSetupThread("RuleByClientID - GenerateReports :"+ddpScheduler.getSchId());
			        		 reportsGenerated = commonUtil.generateReports(ddpScheduler, fromDate, toDate,1,typeOfStatus);
			        	 } catch (Exception ex) {
			        		 logger.error("DdpRuleSchedulerJob.runOnDemandRulForReports() unable to run for scheduler ID :  "+ddpScheduler.getSchId(), ex);
			        	 } finally {
			        		 commonUtil.removeExportSetupThread("RuleByClientID - GenerateReports :"+ddpScheduler.getSchId());
			        	 }
//		         }
//		     };
//
//		     ExecutorService executor = Executors.newCachedThreadPool();
//		     executor.submit(r);
			}
		return reportsGenerated;
		
	}
	
	/**
	 * 
	 * @param reports
	 */
	private void createExportSuccessReprots(List<DdpExportSuccessReport> reports) {
	 try {
			for (DdpExportSuccessReport report : reports) {
				ddpExportSuccessReportService.saveDdpExportSuccessReport(report);
			}
	 } catch (Exception ex) {
		 logger.error("DdpRuleSchedulerJob.createExportSuccessReports() - Unable to createExportSuccessReprots", ex);
	 }
	}
	
	/**
	 * Method used for constructing the dql Query.
	 * 
	 * @param ruleDetailSet
	 * @param typeOfService
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param jobNumbers
	 * @param consignmentIDs
	 * @param docRefs
	 * 
	 * @return DqlQuery.
	 */
	private String constructDQLQueryForOnDemandService(List<DdpRuleDetail> ruleDetailSet,int typeOfService,List<String> branchSet,String documentType,Calendar queryStartDate,Calendar queryEndDate,
				String dynamicValue) {
		
		String dynamicQuery = "";	
		TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ruleDetailSet.get(0));
		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
		DdpExportVersionSetup ddpExportVersionSetup = ddpExportVersionSetups.get(0);
		//getting option
		String option = ddpExportVersionSetup.getEvsOption();
		
		//Checking the RATED & NON-RATED option
		TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetailSet.get(0));
		 List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
		boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
				
		String typeOfServiceValue = (typeOfService == 4 ? "jobNumber" : (typeOfService == 5 ? "consignmentID" :(typeOfService == 6 ? "docReference" : "customQuery")));
		String dqlQuery = (option.equalsIgnoreCase("All") ? env.getProperty("export.ruleByClientID."+typeOfServiceValue+".all") : env.getProperty("export.ruleByClientID."+typeOfServiceValue+".latest")); 
		
		if(!ruleDetailSet.get(0).getRdtCompany().getComCompanyCode().equalsIgnoreCase("GIL")){
		
			if (branchSet.size() > 1) {
				dynamicQuery = " and agl_branch_source in ("+commonUtil.joinString(branchSet, "'", "'", ",") +")";
			} else {
				if (!branchSet.get(0).equalsIgnoreCase("All"))
					dynamicQuery = " and agl_branch_source in ('"+branchSet.get(0) +"')";
			}
		}
		
		String excludeDocs = env.getProperty("export.exclude.documents."+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode());
		if (excludeDocs != null && !excludeDocs.isEmpty()) {
			dynamicQuery +=  " and a_content_type not in (" + commonUtil.joinString(excludeDocs.split(","), "'", "'", ",")  + ")";
		}
		
		dynamicQuery += " and " + env.getProperty(ruleDetailSet.get(0).getRdtPartyCode().getPtyPartyCode()) +" in ('"+ getQueryParams(ruleDetailSet.get(0).getRdtPartyId().trim())+"')";
		if( !ruleDetailSet.get(0).getRdtCompany().getComCompanyCode().equalsIgnoreCase("GIL"))
			dynamicQuery += " and agl_company_source in ('"+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode()+"')";
		
		if (isRated) {
			dynamicQuery += " and agl_is_rated = 1";
		} else {
			dynamicQuery += " and agl_is_rated = 0";
		}
		
		dqlQuery = dqlQuery.replaceAll("%%DYNANMICQUERY%%", dynamicQuery);
		dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%", "'"+documentType+"'");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		
    	String strStartDate = dateFormat.format(queryStartDate.getTime());
    	String strEndDate = dateFormat.format(queryEndDate.getTime());
    	dqlQuery = dqlQuery.replaceAll("%%startdate%%", strStartDate);
    	dqlQuery = dqlQuery.replaceAll("%%enddate%%", strEndDate);
		
    	if ((typeOfService == 4 || typeOfService == 5 || typeOfService == 6)&& !dynamicValue.isEmpty()) 
    		dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%",commonUtil.joinString(dynamicValue.split(","), "'", "'", ","));
//		else if (typeOfService == 5 && !consignmentIDs.isEmpty())
//			dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%",commonUtil.joinString(consignmentIDs.split(","), "'", "'", ","));
//		else if (typeOfService == 6 && !docRefs.isEmpty())
//			dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%",commonUtil.joinString(docRefs.split(","), "'", "'", ","));
//    	
    	return dqlQuery;
	}
	
	public static String getQueryParams(String clientID){
		List<String> list = Arrays.asList(clientID.split(","));
		String qry = "";
		for(String client:list)
			qry =qry+"'"+client+"',";
		qry = qry.substring(1, qry.length()-2);
		return qry;
	}
	
	/**
	 * Method used for getting the detail of IDFCollection
	 * @param query
	 * @param session
	 * 
	 * @return IDFCollection
	 */
	private IDfCollection getIDFCollectionDetails(String query,IDfSession session) {
		
		IDfCollection dfCollection = null;
		try {
			 IDfQuery dfQuery = new DfQuery();
		 	 dfQuery.setDQL(query);
		 	 dfCollection = dfQuery.execute(session,IDfQuery.DF_EXEC_QUERY);
		 		 		 
		} catch (Exception ex) {
			logger.error("DdpRuleSchedluerJob.getIDFCollectionDetails(String query,IDfSession session) - unable to get details of DFCCollections", ex.getMessage());
			taskUtil.sendMailByDevelopers("Unable to execute this query : "+query+"<br/>"+ex, "DdpRuleSchedluerJob.getIDFCollection() not able to fetch.");
		}
		
		return dfCollection;
	}
	
	/**
	 * Method used for constructing the Missing export documents.
	 * 
	 * @param iDfCollection
	 * @return List<DdpExportMissingDocs>
	 */
	private List<DdpExportMissingDocs> constructMissingDocs(IDfCollection iDfCollection,String appName,Integer exportId) {
		
		List<DdpExportMissingDocs> missingDocs = new ArrayList<DdpExportMissingDocs>();
		try {
			while(iDfCollection.next()) {
				
				DdpExportMissingDocs exportDocs = new DdpExportMissingDocs();
				Calendar createdDate = GregorianCalendar.getInstance();
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				
				exportDocs.setMisRObjectId(iDfCollection.getString("r_object_id"));
				exportDocs.setMisEntryNo(iDfCollection.getString("agl_customs_entry_no"));
				exportDocs.setMisMasterJob(iDfCollection.getString("agl_master_job_number"));
				exportDocs.setMisJobNumber(iDfCollection.getString("agl_job_numbers"));
				exportDocs.setMisConsignmentId(iDfCollection.getString("agl_consignment_id"));
				//Invoice number for sales document reference
				exportDocs.setMisEntryType(iDfCollection.getString("agl_doc_ref"));
				//Content size
				exportDocs.setMisCheckDigit(iDfCollection.getString("r_content_size"));
				//System.out.println("Inside the construnctMissingDocs : "+exportDocs.getMisEntryType());
				exportDocs.setMisDocType(iDfCollection.getString("agl_control_doc_type"));
				exportDocs.setMisBranch(iDfCollection.getString("agl_branch_source"));
				exportDocs.setMisCompany(iDfCollection.getString("agl_company_source"));
				exportDocs.setMisRobjectName(iDfCollection.getString("object_name"));
				exportDocs.setMisAppName(appName);
				exportDocs.setMisStatus(0);
				exportDocs.setMisExpRuleId(exportId);
				exportDocs.setMisCreatedDate(GregorianCalendar.getInstance());
				exportDocs.setMisDocVersion(iDfCollection.getString("r_version_label"));
				
				try {
				createdDate.setTime(dateFormat.parse(iDfCollection.getString("r_creation_date")));
				exportDocs.setMisDmsRCreationDate(createdDate);
				
				} catch(Exception ex) {
					logger.error("DdpRuleSchedulerJob.constructMissingDocs() - Unable to parse string to date",ex.getMessage());
				}
				missingDocs.add(exportDocs);
			}
		} catch (Exception ex) {
			missingDocs = new ArrayList<DdpExportMissingDocs>();
			logger.error("DdpRuleSchedulerJob.constructMissingDocs() - Unable to construct the Missing docs", ex.getMessage());
		}
		return missingDocs;
	}
	
	/**
	 * Method used for performing the file download operations for OnDemand Services.
	 * 
	 * @param ddpExportRule
	 * @param exportDocs
	 * @param session
	 * @param sourceFolder
	 * @param missingDocs
	 * @param mailBody
	 * @param exportedReports
	 * @param typeOfService
	 * @return
	 */
	private boolean preformFileDownloadForOnDemandService(DdpExportRule ddpExportRule,List<DdpExportMissingDocs> exportDocs,IDfSession session,
			String sourceFolder,List<DdpExportMissingDocs> missingDocs,StringBuffer mailBody,List<DdpExportSuccessReport> exportedReports,int typeOfService) {
		
		boolean isDownload = false;
		DdpDocnameConv convName  = getMatchedDocnameCon(ddpExportRule.getExpRuleId());
		List<String> fileNameList = new ArrayList<String>();
		
		try {
			for (DdpExportMissingDocs exportDoc : exportDocs ) {
				boolean isDownloadLocal = false;
				
				String fileName = getDocumentFileName(convName, exportDoc, fileNameList, commonUtil.getLoadNumberMapDetails(convName, exportDoc, "ruleByClientID", env));
				isDownloadLocal  = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(exportDoc.getMisRObjectId(), sourceFolder, fileName, session);
				if (!isDownloadLocal) {
					missingDocs.add(exportDoc);
				} else {
					SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
			    	mailBody.append(format.format(new Date())+","); 
					format.applyLocalizedPattern("HH:mm:ss");
					mailBody.append(format.format(new Date())+",receive,success,"+exportDoc.getMisCheckDigit()+",00:00:01,"+exportDoc.getMisConsignmentId()+","+exportDoc.getMisJobNumber()+","+fileName+"\n");
			   		exportedReports.add(commonUtil.constructExportReportDomainObject(ddpExportRule.getExpRuleId(), ddpExportRule.getExpRuleId(), exportDoc.getMisJobNumber(), 
			   				exportDoc.getMisConsignmentId(), "Rule By ClientID", fileName, (int)FileUtils.findSizeofFileInKB(sourceFolder+"/"+fileName), typeOfService,
			   				exportDoc.getMisEntryType(), exportDoc.getMisMasterJob(), exportDoc.getMisCreatedDate()));
			   		fileNameList.add(fileName);
				}
			}
		} catch (Exception ex) {
			logger.error("DdpRuleSchedulerJob.preformFileDownloadForOnDemadService() - unable to download file.",ex);
		}
		logger.info("DdpRuleSchedulerJob.preformFileDownloadForOnDemandService() - End of the downloading the files into local");
		isDownload = true;
		
		return isDownload;
		
	}
	/**
	 * Method used for getting the document name.
	 * 
	 * @param objectName
	 * @param namingConvention
	 * @param exportDocs
	 * @param sourceFolder
	 * @param isFTPType
	 * @param ftpClient
	 * @param destinationLocation
	 * @param smbFile
	 * @param invoiceNumber
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

}