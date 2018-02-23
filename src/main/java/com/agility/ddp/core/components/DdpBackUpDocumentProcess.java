/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.TypedQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.DdpDFCClient;
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
import com.agility.ddp.data.domain.DdpExportQuery;
import com.agility.ddp.data.domain.DdpExportQueryUi;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpExportRuleService;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.agility.ddp.data.domain.DdpExportSuccessReportService;
import com.agility.ddp.data.domain.DdpNotification;
import com.agility.ddp.data.domain.DdpRateSetup;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;

/**
 * @author DGuntha
 *
 */
@Component
public class DdpBackUpDocumentProcess {

	private static final Logger logger = LoggerFactory.getLogger(DdpBackUpDocumentProcess.class);
	
	//@Autowired
	//Environment env;
	
	@Autowired
	private ApplicationProperties applicationProperties; 
	
	//@Autowired
	//private DdpExportRuleService ddpExportRuleService;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private DdpExportMissingDocsService ddpExportMissingDocsService;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	@Autowired
	private DdpRuleDetailService ddpRuleDetailService;
	
	
	// private String tempFilePath = applicationProperties.getProperty("ddp.export.folder");
	 
	 @Autowired
	 private CommonUtil commonUtil;
	 
	 @Autowired
	 private TaskUtil taskUtil;
	 
	 @Autowired
	 private DdpExportSuccessReportService ddpExportSuccessReportService;
	 
	 @Autowired
	 private DdpTransferFactory ddpTransferFactory;
	 
	
	/**
	 * Method used for executing the scheduler job based on the application name.
	 * 
	 * @param ddpScheduler
	 * @param currentDate
	 * @param appName
	 * @return
	 */
	public boolean executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName,Calendar presentDate) {
		
		logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.executeJob() - Invoked sucessfully.");
		boolean isJobExecuted = false;
		Calendar startDate = null;
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),appName);
		
		if (ddpExportRule == null || ddpExportRule.getExpSchedulerId().getSchStatus() != 0) {
			logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+appName);
			return isJobExecuted;
		}
		DdpScheduler scheduler = ddpSchedulerService.findDdpScheduler(ddpScheduler.getSchId());
		
		if (scheduler == null || scheduler.getSchStatus() != 0) {
			logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped. For application : "+appName);
			return isJobExecuted;
		}
		
		boolean isSchLastRunStatus = false;
		if (scheduler.getSchLastSuccessRun() == null) { 
			startDate = ddpExportRule.getExpActivationDate();
			isSchLastRunStatus = true;
		} else 
			startDate = scheduler.getSchLastSuccessRun();
		
		if (scheduler.getSchDelayCount() != null) {
			
			String dateFreq = SchedulerJobUtil.getSchFreq(scheduler.getSchCronExpressions());
			if (dateFreq.equalsIgnoreCase("monthly")) {
				currentDate.add(Calendar.MONTH,-scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.MONTH, -scheduler.getSchDelayCount());
			} else if (dateFreq.equalsIgnoreCase("weekly")) {
				currentDate.add(Calendar.WEEK_OF_YEAR, -scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.WEEK_OF_YEAR, -scheduler.getSchDelayCount());
			} else if (dateFreq.equalsIgnoreCase("daily")) {
				currentDate.add(Calendar.DAY_OF_YEAR, -scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.DAY_OF_YEAR, -scheduler.getSchDelayCount());
			} else if (dateFreq.equalsIgnoreCase("hourly")) {
				currentDate.add(Calendar.HOUR_OF_DAY, -scheduler.getSchDelayCount());
				if (isSchLastRunStatus)
					startDate.add(Calendar.HOUR_OF_DAY, -scheduler.getSchDelayCount());
			}
		}

		
		//SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		//logger.info("ddpExportRule.getExpSchedulerId().getSchLastSuccessRun() : "+dateFormat.format(ddpExportRule.getExpSchedulerId().getSchLastSuccessRun())+" : form scheduler : "+dateFormat.format(ddpScheduler.getSchLastSuccessRun()));
		
		if (currentDate.getTime().before(startDate.getTime())) {
			logger.info("DdpBackDocumentProcess.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - Start Date : "+startDate.getTime()+" is greater than end date : "+currentDate.getTime()+" . For Application : "+appName);
			return isJobExecuted;
		}
		
		isJobExecuted = processSchedulerJob( ddpScheduler, ddpExportRule, appName, 1, startDate, currentDate,presentDate);
		
		return isJobExecuted;
	}
	
	/**
	 * Method is used for processing the scheduler job.
	 * 
	 * @param ddpScheduler
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService - 1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
	 * @param startDate
	 * @param endDate
	 * 
	 * @return
	 */
	public boolean processSchedulerJob(DdpScheduler ddpScheduler,DdpExportRule ddpExportRule,String appName,
						int typeOfService,Calendar startDate,Calendar endDate,Calendar presentDate) {
	
		boolean isProcessed = false;
		List<DdpExportMissingDocs> exportList = null;
		
		List<DdpRuleDetail> ruleDetails = commonUtil.getMatchedSchdIDAndRuleDetailForExport(ddpExportRule.getExpSchedulerId().getSchId());
		
		if (ruleDetails == null || ruleDetails.size() == 0) {
			logger.info("AppName :  "+appName+". DdpBackUpdDocumentProcess.runGeneralSchedulerJob() - No DdpRuleDetail found for the rule id : "+ddpExportRule.getExpRuleId());
			return isProcessed;
		}
		
		//To avoid time creation of session for each iteration of loop
    	IDfSession session = ddpDFCClientComponent.beginSession();
    	
    	if (session == null) {
    		logger.info("AppName: "+appName+". DdpBackDocumentProcess.processSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] Unable create the DFCClient Session for this Scheduler ID."+ddpExportRule.getExpSchedulerId().getSchId());
    		sendMailForEmptyRecords(ddpExportRule, appName, typeOfService, endDate, startDate, presentDate,"session",null);
    		taskUtil.sendMailByDevelopers("For export module, Application Name : "+appName+". Unable to connection the dfc session. So please restart the jboss server.",  "export - dfc session issue.");
    		return isProcessed;
    	}
    	try {
	    	String primaryDocType = "";
	    	for (DdpRuleDetail ruleDetail : ruleDetails) {
	    		//As the primary document details are fetched using dql query.
				if (ruleDetail.getRdtRelavantType() == 1)
					primaryDocType += "'" + ruleDetail.getRdtDocType().getDtyDocTypeCode() + "',";
	    	}
	    	
	    	if (primaryDocType.endsWith(","))
	    		primaryDocType = primaryDocType.substring(0, primaryDocType.length()-1);
	    	
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		
	    	String strStartDate = dateFormat.format(startDate.getTime());
	    	String strEndDate = dateFormat.format(endDate.getTime());
	    	
			String query =  constructDyanmicDQLQuery(ddpScheduler, ddpExportRule,appName);
			query = query.replaceAll("%%PRIMARYDOCTYPE%%", primaryDocType);
			query = query.replaceAll("%%startdate%%", strStartDate);
			query = query.replaceAll("%%enddate%%", strEndDate);
			
			logger.info("AppName : "+appName+". DdpBackDocumentProcess.processSchedulerJob() - Query for prccess Scheduler JOb  : "+query);
			
			IDfCollection idfCollection = getIDFCollectionDetails(query,session);
			
			if (idfCollection == null) {
				logger.info("AppName : "+appName+".DdpBackDocumentProcess.processSchedulerJob() - No Records found for query "+query);
				//updateDdpSchedulerTime(endDate, presentDate, ddpScheduler, typeOfService, appName);
				sendMailForEmptyRecords(ddpExportRule, appName, typeOfService, endDate, startDate, presentDate,"empty",null);
				return isProcessed;
			}
			
			exportList = constructMissingDocs(idfCollection,appName,ddpExportRule.getExpRuleId());
			
			if (exportList.size() == 0) {
				logger.info("AppName : "+appName+". DdpBackDocumentProcess.processSchedulerJob() - DdpMissingExportDocs is empty list ");
				updateDdpSchedulerTime(endDate, presentDate, ddpScheduler, typeOfService, appName);
				sendMailForEmptyRecords(ddpExportRule, appName, typeOfService, endDate, startDate, presentDate,"empty",null);
				return isProcessed;
			}
			
			logger.info("AppName : "+appName+". Export List size of the records : "+exportList.size());
			// if it scheduler running type
			if (typeOfService == 1) {
				//create missing records & insert into DB.
				exportList = insertExportMissingDocs(exportList);
				//TODO : update the DMS server db with little.
				// Update ddp scheduler last success run time.
				updateDdpSchedulerTime(endDate, presentDate, ddpScheduler, typeOfService, appName);
				logger.info("DdpBackUpDocumentProcess.processSchedulerJob() : Total records inserted into ddp_missing_export_docs : "+exportList.size());
			}
			
			isProcessed = runGeneralSchedulerJob(ddpExportRule, exportList, endDate, typeOfService,appName,session,startDate,presentDate,ruleDetails,null);
			
		} catch (Exception ex) {
			logger.error("DdpBackUpDocumentProces.processSchedulerJob() - exception occurried while exporting the documents.", ex);
		} finally {
			if (session != null)
				ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
		}
		return isProcessed;
	}
	
	
	/**
	 * Method is used for processing the scheduler job.
	 * 
	 * @param ddpScheduler
	 * @param ddpExportRule
	 * @param appName
	 * @param typeOfService - 1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
	 * @param startDate
	 * @param endDate
	 * 
	 * @return
	 */
	public boolean onDemandProcessSchedulerJob(DdpScheduler ddpScheduler,DdpExportRule ddpExportRule,String appName,
						int typeOfService,Calendar startDate,Calendar endDate,Calendar presentDate,String jobNumbers,String consignmentIDs,String docRefs) {
	
		boolean isProcessed = false;
		List<DdpExportMissingDocs> exportList = null;
		String dynamicValues = "";
		
		List<DdpRuleDetail> ruleDetails = commonUtil.getMatchedSchdIDAndRuleDetailForExport(ddpExportRule.getExpSchedulerId().getSchId());
		
		if (ruleDetails == null || ruleDetails.size() == 0) {
			logger.info("AppName :  "+appName+". DdpBackUpdDocumentProcess.onDemandProcessSchedulerJob() - No DdpRuleDetail found for the rule id : "+ddpExportRule.getExpRuleId());
			return isProcessed;
		}
		
		if (typeOfService == 4 && !jobNumbers.isEmpty()) 
    		dynamicValues = jobNumbers;
		else if (typeOfService == 5 && !consignmentIDs.isEmpty())
			dynamicValues = consignmentIDs;
		else if (typeOfService == 6 && !docRefs.isEmpty())
			dynamicValues = docRefs;
		
		//To avoid time creation of session for each iteration of loop
    	IDfSession session = ddpDFCClientComponent.beginSession();
    	
    	if (session == null) {
    		logger.info("AppName: "+appName+". DdpBackDocumentProcess.onDemandProcessSchedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] Unable create the DFCClient Session for this Scheduler ID."+ddpExportRule.getExpSchedulerId().getSchId());
    		sendMailForEmptyRecords(ddpExportRule, appName, typeOfService, endDate, startDate, presentDate,"session",dynamicValues);
    		taskUtil.sendMailByDevelopers("For export module, Application Name : "+appName+". Unable to connection the dfc session. So please restart the jboss server.",  "export - dfc session issue.");
    		return isProcessed;
    	}
    	try {
	    	String primaryDocType = "";
	    	boolean isRated = false;
	    	for (DdpRuleDetail ruleDetail : ruleDetails) {
	    		//As the primary document details are fetched using dql query.
				if (ruleDetail.getRdtRelavantType() == 1) {
					primaryDocType += "'" + ruleDetail.getRdtDocType().getDtyDocTypeCode() + "',";
					TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetail);
					 List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
					 isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
				}
	    	}
	    	
	    	if (primaryDocType.endsWith(","))
	    		primaryDocType = primaryDocType.substring(0, primaryDocType.length()-1);
	    	
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		
	    	String strStartDate = dateFormat.format(startDate.getTime());
	    	String strEndDate = dateFormat.format(endDate.getTime());
	    	
			String query = constructOnDemandDyanmicDQLQuery(ddpScheduler, ddpExportRule, appName, typeOfService);
			
			if (typeOfService == 4 && !jobNumbers.isEmpty()) 
				query = query.replaceAll("%%JOBNUMBER%%",commonUtil.joinString(jobNumbers.split(","), "'", "'", ","));
			else if (typeOfService == 5 && !consignmentIDs.isEmpty())
				query = query.replaceAll("%%CONSIGNMENTID%%",commonUtil.joinString(consignmentIDs.split(","), "'", "'", ","));
			else if (typeOfService == 6 && !docRefs.isEmpty())
				query = query.replaceAll("%%DOCREFS%%",commonUtil.joinString(docRefs.split(","), "'", "'", ","));
			
			query = query.replaceAll("%%PRIMARYDOCTYPE%%", primaryDocType);
			query = query.replaceAll("%%startdate%%", strStartDate);
			query = query.replaceAll("%%enddate%%", strEndDate);
			
			if (isRated) {
				query += " and agl_is_rated = 1";
			} else {
				query += " and agl_is_rated = 0";
			}
			
			logger.info("AppName : "+appName+". DdpBackDocumentProcess.onDemandProcessSchedulerJob() - Query for prccess Scheduler JOb  : "+query);
			
			IDfCollection idfCollection = getIDFCollectionDetails(query,session);
			
			if (idfCollection == null) {
				logger.info("AppName : "+appName+".DdpBackDocumentProcess.onDemandProcessSchedulerJob() - No Records found for query "+query);
				//updateDdpSchedulerTime(endDate, presentDate, ddpScheduler, typeOfService, appName);
				sendMailForEmptyRecords(ddpExportRule, appName, typeOfService, endDate, startDate, presentDate,"empty",dynamicValues);
				return isProcessed;
			}
			
			exportList = constructMissingDocs(idfCollection,appName,ddpExportRule.getExpRuleId());
			
			if (exportList.size() == 0) {
				logger.info("AppName : "+appName+". DdpBackDocumentProcess.onDemandProcessSchedulerJob() - DdpMissingExportDocs is empty list ");
				//updateDdpSchedulerTime(endDate, presentDate, ddpScheduler, typeOfService, appName);
				sendMailForEmptyRecords(ddpExportRule, appName, typeOfService, endDate, startDate, presentDate,"empty",dynamicValues);
				return isProcessed;
			}
			
			logger.info("AppName : "+appName+". Export List size of the records : "+exportList.size());
					
			isProcessed = runGeneralSchedulerJob(ddpExportRule, exportList, endDate, typeOfService,appName,session,startDate,presentDate,ruleDetails,dynamicValues);
			
		} catch (Exception ex) {
			logger.error("DdpBackUpDocumentProces.onDemandProcessSchedulerJob() - exception occurried while exporting the documents.", ex);
		} finally {
			if (session != null)
				ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
		}
		return isProcessed;
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
			logger.error("AppName : "+appName+". DdpBackUpDocumentProcess.updateDdpSchedulerTime() - Error ", ex);
		}
	}
	/**
	 * Method used for running the general Scheduler job.
	 * 
	 * @param ddpExportRule
	 * @param exportDocs
	 * @param currentDate
	 * @param typeOfService
	 * @param appName
	 * @param session
	 * @return
	 */
	public boolean runGeneralSchedulerJob(DdpExportRule ddpExportRule,List<DdpExportMissingDocs> exportDocs,Calendar currentDate,int typeOfService,String appName,IDfSession session,
			Calendar startDate,Calendar presentDate,List<DdpRuleDetail> ruleDetails,String dyamicValues) {
		
		boolean isProcessed = false;
		
		
		//Key string as r_objec_id, because it unique value
		Map<String,DdpExportMissingDocs> exportRecords = new HashMap<String, DdpExportMissingDocs>();
		Map<String,List<DdpExportMissingDocs>> subExportRecords = new HashMap<String, List<DdpExportMissingDocs>>();
		Map<String,DdpExportMissingDocs> missingRecords = new HashMap<String, DdpExportMissingDocs>();
		//key as the Document type & ddp rule detail as the value
		Map<String,DdpRuleDetail> ruleDetailMap = new HashMap<String, DdpRuleDetail>();
		Map<Integer,String> sequenceOrder = new HashMap<Integer, String>();
				
		Set<String> primaryDocTypeList = new HashSet<String>();
		Set<String> mandiatoryDocTypes = new HashSet<String>();
		Set<String> optionalDocTypes = new HashSet<String>();
		Set<String> alteastDocTypes = new HashSet<String>();
		String docTypes = "";
		
		
		//Looping for adding the primary & secondary document types
		for (DdpRuleDetail ruleDetail : ruleDetails) {
			
			ruleDetailMap.put(ruleDetail.getRdtDocType().getDtyDocTypeCode(), ruleDetail);
			
			if (ruleDetail.getRdtRelavantType() != null) {
				if (ruleDetail.getRdtRelavantType() == 1)
					primaryDocTypeList.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			}
			
			if (ruleDetail.getRdtForcedType() == 0) {
				optionalDocTypes.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			} else if (ruleDetail.getRdtForcedType() == 1) {
				mandiatoryDocTypes.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			} else if (ruleDetail.getRdtForcedType() == 2) {
				alteastDocTypes.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			}
			//As the primary document details are fetched using dql query.
			if (ruleDetail.getRdtRelavantType() != 1)
				docTypes += "'" + ruleDetail.getRdtDocType().getDtyDocTypeCode() + "',"; 
			
			if (ruleDetail.getRdtDocSequence() != null) 
				sequenceOrder.put(ruleDetail.getRdtDocSequence(), ruleDetail.getRdtDocType().getDtyDocTypeCode());
		}
		//removing the last letter that is comma.
		if (docTypes.endsWith(","))
			docTypes = docTypes.substring(0, docTypes.length()-1);
		
		Map<String,List<String>> subMissingDocs = new HashMap<String, List<String>>();
	
		for (DdpExportMissingDocs ddpExportMissingDoc : exportDocs) {
			
			if (docTypes.trim().length() > 0 && !docTypes.isEmpty()) {
				
				List<DdpExportMissingDocs> backUpDocuments = new ArrayList<DdpExportMissingDocs>();
				fetchBackUpDocuments(backUpDocuments, ruleDetails, ddpExportRule, ddpExportMissingDoc, appName, session);
				List<DdpExportMissingDocs> subExportDocs = isDocExportable(mandiatoryDocTypes, alteastDocTypes, primaryDocTypeList, ddpExportMissingDoc.getMisRObjectId(),subMissingDocs,backUpDocuments);
				if  (subExportDocs == null || subExportDocs.size() == 0) {
					//Case : if all document types are optional then following if condition will be executed. 
					if (primaryDocTypeList.size() == mandiatoryDocTypes.size() && alteastDocTypes.size() == 0)  {
						exportRecords.put(ddpExportMissingDoc.getMisRObjectId(), ddpExportMissingDoc);
						logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.runGeneralSchedulerJob - No optional backup documents found. For the R_OBJECT_ID : "+ddpExportMissingDoc.getMisRObjectId());
					} else {
						logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.runGeneralSchedulerJob - For the R_OBJECT_ID : "+ddpExportMissingDoc.getMisRObjectId()+" no backup documents available." );
						missingRecords.put(ddpExportMissingDoc.getMisRObjectId(), ddpExportMissingDoc);
					}
					continue;
				}
				subExportRecords.put(ddpExportMissingDoc.getMisRObjectId(),subExportDocs);
			}
			exportRecords.put(ddpExportMissingDoc.getMisRObjectId(), ddpExportMissingDoc);
			
		}
		//logger.info("AppName : "+appName+" : DdpBackUpDocumentProcess.runGeneralSchedulerJob- Type of Service "+typeOfService+" : Missing records : "+missingRecords);
		//logger.info("AppName : "+appName+" : DdpBackUpDocumentProcess.runGeneralSchedulerJob- Type of Service "+typeOfService+" : Exported Records : "+exportRecords);
		isProcessed = performTransfer(ddpExportRule, currentDate, typeOfService, appName, session, ruleDetailMap, exportRecords, missingRecords, subExportRecords, docTypes,subMissingDocs,startDate,sequenceOrder,presentDate,dyamicValues);
		return isProcessed;
		
	}
	
	/**
	 * Method used for fetchign the backup documents.
	 * 
	 * @param backUpDocuments
	 * @param ruleDetails
	 * @param ddpExportRule
	 * @param ddpExportMissingDoc
	 * @param appName
	 * @param session
	 */
	private void fetchBackUpDocuments(List<DdpExportMissingDocs> backUpDocuments,List<DdpRuleDetail> ruleDetails,DdpExportRule ddpExportRule,
			DdpExportMissingDocs ddpExportMissingDoc,String appName,IDfSession session) {
		
		//Added code for adding the reated & non-rated option
		for (DdpRuleDetail ruleDetail : ruleDetails) {
			
			if (ruleDetail.getRdtRelavantType() != null && ruleDetail.getRdtRelavantType() == 1)
				continue;
			
				String custQuery = applicationProperties.getProperty("export.rule."+appName+"."+ddpExportRule.getExpSchedulerId().getSchBatchingCriteria()+".customQuery");
				custQuery = custQuery.replaceAll("%%DOCTYPES%%", "'"+ruleDetail.getRdtDocType().getDtyDocTypeCode()+"'");
				
				if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("jobNumber")) {
					custQuery = custQuery.replaceAll("%%JOBNUMBER%%", "'"+ddpExportMissingDoc.getMisJobNumber()+"'");
				} else if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("consignmentID")) {
					custQuery = custQuery.replaceAll("%%JOBNUMBER%%", "'"+ddpExportMissingDoc.getMisConsignmentId()+"'");
				} else if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("entryNo")) {
					custQuery = custQuery.replaceAll("%%JOBNUMBER%%", "'"+ddpExportMissingDoc.getMisEntryNo()+"'");
				}
				
				TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetail);
				 List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
				boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
				if (isRated) {
					custQuery += " and agl_is_rated = 1";
				} else {
					custQuery += " and agl_is_rated = 0";
				}
				
				logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.runGeneralSchedulerJob()- Custom Query for backup document rule id :"+ruleDetail.getRdtRuleId().getRulId()+" : Query is  "+custQuery);
				IDfCollection idfCollection = getIDFCollectionDetails(custQuery,session);
				
				if (idfCollection != null)
					backUpDocuments.addAll(constructMissingDocs(idfCollection, appName,ddpExportRule.getExpRuleId()));
		}
		
	}
	
	/**
	 * Method used for performing the transfer to client based on configurations
	 * 
	 * @param ddpExportRule
	 * @param currentDate
	 * @param typeOfService
	 * @param appName
	 * @param session
	 * @param ruleDetailMap
	 * @param exportRecords
	 * @param missingRecords
	 * @param subExportRecords
	 * @param docTypes
	 * @return
	 */
	private boolean performTransfer(DdpExportRule ddpExportRule,Calendar currentDate,int typeOfService,String appName,IDfSession session,
			Map<String,DdpRuleDetail> ruleDetailMap,Map<String,DdpExportMissingDocs> exportRecords,Map<String,DdpExportMissingDocs> missingRecords,
			Map<String,List<DdpExportMissingDocs>> subExportRecords,String docTypes,Map<String,List<String>> subMissingDocs,Calendar startDate,
			Map<Integer,String> sequenceOrder,Calendar presentDate,String dynamicValues) {
		
		logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.performTransfer() - invoked successfully. ");
		boolean isTransfered = false;
		File tmpFile = null;
		//DdpSFTPClient sftpClient = null;
		//ChannelSftp sftpChannel = null;
		DdpCompressionSetup compressionSetup =  ddpExportRule.getExpCompressionId();
		Set<String> subMissingRecords = new HashSet<String>();
		List<DdpExportSuccessReport> exportedReports = new ArrayList<DdpExportSuccessReport>();
		
		//All or latest version need download
		Map<String, String> versionMap = getExportVersionSetupDetails(ruleDetailMap);
		try{
			if (compressionSetup.getCtsCompressionLevel() != null) {
				
				String tempFilePath = applicationProperties.getProperty("ddp.export.folder");
				// creating the folder in the local for downloading purpose.
			  	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
				String tempFolderPath = tempFilePath+"/temp_"+appName+"_"+typeOfService+"_"+ddpExportRule.getExpRuleId() +"_" + dateFormat.format(new Date());
				tmpFile = FileUtils.createFolder(tempFolderPath);
		
				String sourceFolder = tempFolderPath + "/"+appName+"_" +dateFormat.format(presentDate.getTime());
				File sourceFolderFile = FileUtils.createFolder(sourceFolder);
				File endSourceFolderFile = sourceFolderFile;
				
				DdpDocnameConv namingConvention = ddpExportRule.getExpDocnameConvId();
				DdpCommunicationSetup commSetup = ddpExportRule.getExpCommunicationId();
				//boolean isFTPType =	commSetup.getCmsCommunicationProtocol().equalsIgnoreCase("FTP")? true : false;
				
				StringBuffer mailBody = new StringBuffer();
			
				DdpTransferObject ddpTransferObject = null;
			//	DdpSFTPClient ddpSFTPClient = null;
				List<DdpTransferObject> transferObjects = ddpTransferFactory.constructTransferObject(commSetup);
				
				
				for (DdpTransferObject transferObject : transferObjects) {
					if (transferObject.isConnected()) {
						ddpTransferObject = transferObject;
//						if (ddpTransferObject.getTypeOfConnection().equals("sftp"))
//							ddpSFTPClient = (DdpSFTPClient) ddpTransferObject.getDdpTransferClient();
						break;
					}
				}
				
				if (ddpTransferObject == null) {
					sendMailForConnectionIssue(ddpExportRule, appName, typeOfService, currentDate, startDate, presentDate,transferObjects,dynamicValues);
					return isTransfered;
				}
				
				//looping for downloading files from the ftp location
				for (String rObjectId : exportRecords.keySet()) {
					
					DdpExportMissingDocs exportDocs = exportRecords.get(rObjectId);
					List<DdpExportMissingDocs> subDocsList = subExportRecords.get(rObjectId);
					List<String> fileNameList = new ArrayList<String>();
					//commonUtil.readFileNamesFromLocation(sourceFolder, ddpTransferObject.isFTPType(), ddpTransferObject.getFtpClient(), ddpTransferObject.getDestLocation(), ddpTransferObject.getSmbFile(), ddpTransferObject.getChannelSftp(), fileNameList,ddpSFTPClient,ddpTransferObject.getFtpDetails());
					commonUtil.readFileNamesFromLocation(sourceFolder, ddpTransferObject, fileNameList);
					//Based on the primary document.
					Map<String, String> loadNumberMap = commonUtil.getLoadNumberMapDetails(namingConvention, exportDocs, appName, applicationProperties);
					//merge
					if (compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("merge")) {
						
						List<String> rObjectList =	getRobjectIDSequenceOrder(exportDocs, subDocsList, sequenceOrder);
						String fileName = getDocumentFileName(exportDocs.getMisRobjectName(), namingConvention, exportDocs,exportDocs.getMisEntryType(),fileNameList,loadNumberMap);
						
						if (fileName == null || fileName.length() == 0)
							fileName = exportDocs.getMisRobjectName();
						
						String mergeID = commonUtil.performMergeOperation(rObjectList, fileName, applicationProperties.getProperty("ddp.vdLocation"), applicationProperties.getProperty("ddp.objectType"), session,appName);
						if (mergeID == null) {
							logger.info("AppName : "+appName+" DdpBackUpDocumentProcess.performTransfer() - Unable to merge the documents");
							missingRecords.put(rObjectId, exportDocs);
							subMissingRecords.add(rObjectId);
							continue;
						}						
						boolean isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(mergeID, sourceFolder, fileName, session);
						if (!isDownload)logger.info("AppName : "+appName+" DdpBackUpDocumentProcess.performTransfer() - Unable to download the documents robjectid is "+mergeID); 
						else {
								SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
								mailBody.append(format.format(new Date())+",");
								format.applyLocalizedPattern("HH:mm:ss");
								Long fileSize = FileUtils.findSizeofFileInKB(sourceFolder+"/"+fileName);
								//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+exportDocs.getMisCheckDigit()+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
								mailBody.append(format.format(new Date())+",receive,success,"+fileSize+",00:00:01,"+exportDocs.getMisConsignmentId()+","+exportDocs.getMisJobNumber()+","+fileName+"\n");
								if (typeOfService == 1 || typeOfService == 3) {
									exportedReports.add(commonUtil.constructExportReportDomainObject(exportDocs, fileSize, fileName, typeOfService));
								}
						}
						
					}  else {
						
						boolean isDownload = false;
						isDownload = performDownloadOperation(exportDocs.getMisRobjectName(), rObjectId, namingConvention, exportDocs, session, versionMap,exportDocs.getMisEntryType(),mailBody,fileNameList,sourceFolder,loadNumberMap,typeOfService,exportedReports);
						 
						if (!isDownload) {
							missingRecords.put(rObjectId, exportDocs);
							subMissingRecords.add(rObjectId);
							continue;
						}
						
						if (subDocsList != null && subDocsList.size() > 0) {
							for (DdpExportMissingDocs miss : subDocsList) {
								if (miss.getMisId() == null)
									miss.setMisId(exportDocs.getMisId());
								isDownload = performDownloadOperation(miss.getMisRobjectName(), miss.getMisRObjectId(), namingConvention, miss, session, versionMap,exportDocs.getMisEntryType(),mailBody,fileNameList,sourceFolder,loadNumberMap,typeOfService,exportedReports);
								  if (!isDownload) {
										missingRecords.put(rObjectId, exportDocs);
										subMissingRecords.add(rObjectId);
										break;
								 }
							}
						}
					}
					
				}
				
				//Removing the missing records from export records if any failure case.
				for (String subMissDoc : subMissingRecords) {
					exportRecords.remove(subMissDoc);
				}
			
				if (exportRecords.size() > 0 && compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("exportAsZip")) {
					
					String zipPath = tempFolderPath +"/ZIP_"+appName;
					File zipPathFile = FileUtils.createFolder(zipPath);
					FileUtils.zipFiles(zipPath+"/"+appName+"_"+typeOfService+"_" +dateFormat.format(currentDate.getTime())+".zip", sourceFolder);
					endSourceFolderFile = zipPathFile;
					
				}
				 
				//Error occuries while transferring files then.
				boolean isProcessed = false;
				List<DdpTransferObject> transferList = new ArrayList<DdpTransferObject>();
				
				if (exportRecords.size() > 0) {
				
					for (DdpTransferObject transferObject : transferObjects) {
						
						if (transferObject.isConnected()) {
							
							boolean isTransferd = ddpTransferFactory.transferFilesUsingProtocol(transferObject, endSourceFolderFile,null,appName,startDate,currentDate,"ruleByQuery."+typeOfService);
							
							if (isTransferd) {
								isProcessed = true;
								transferList.add(transferObject);
							}
							
							if (!isTransferd) {
								List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
								list.add(transferObject);
								sendMailForConnectionIssue(ddpExportRule, appName, typeOfService, currentDate, startDate, presentDate,list,dynamicValues);
							}
						} else {
							List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
							list.add(transferObject);
							sendMailForConnectionIssue(ddpExportRule, appName, typeOfService, currentDate, startDate, presentDate,list,dynamicValues);
						}
					}
					
					logger.info("AppName : "+appName+". DdpBackUpDocumentProcess.performTransfer() - Transfered files status : "+isProcessed);
				}
				
				 
				 //dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
				 if ((typeOfService == 1 || typeOfService == 3) && isProcessed) {
					 changeStatusForMissingDocs(exportRecords, presentDate);
					 createExportSuccessReprots(exportedReports);
				 }
				 
				 Date endTime = new Date();
				 if (exportRecords.size() > 0)
					 sendMailForSuccessExport( ddpExportRule, appName, typeOfService, currentDate, startDate, mailBody, tempFolderPath,presentDate,endTime,transferList,dynamicValues);
				 if (missingRecords.size() > 0)
					 sendMailForMissingDocuments(missingRecords,ddpExportRule,tempFolderPath, appName, typeOfService, currentDate, subMissingDocs,startDate,presentDate,endTime,dynamicValues);
			} else {
				logger.info("AppName : "+appName+" DdpBackUpDocumentProcess.performTransfer() -Unbale to export the details due to the compression level is null");
			}
			
			
		} catch(Exception ex) {
			logger.error("AppName : "+appName+" DdpBackUpDocumentProcess.performTransfer() - unable to perform export ",ex);
		} finally {
			
			if (tmpFile != null)
				SchedulerJobUtil.deleteFolder(tmpFile);			
		}
		return isTransfered;
	}
	
	
	
	/**
	 * Method used for the Objectid list in the order form.
	 * 
	 * @param exportDocs
	 * @param subDocsList
	 * @param sequenceOrder
	 * @return
	 */
	private List<String> getRobjectIDSequenceOrder(DdpExportMissingDocs exportDocs,List<DdpExportMissingDocs> subDocsList,Map<Integer,String> sequenceOrder) {
		
		List<String> rObjectList = new LinkedList<String>();
		Map<String,String> map = new HashMap<String, String>();
		map.put(exportDocs.getMisDocType(), exportDocs.getMisRObjectId());
		
		if (subDocsList  != null) {
			for (DdpExportMissingDocs docs : subDocsList)
				map.put(docs.getMisDocType(), docs.getMisRObjectId());
		}
		
		List<Integer> keys = new ArrayList<Integer>(sequenceOrder.keySet());
		Collections.sort(keys);
		for (Integer key : keys) {
			String rObjectID =  map.get(sequenceOrder.get(key));
			if (rObjectID != null)
				rObjectList.add(rObjectID);
		}
		return rObjectList;
	}
	
	/**
	 * Method used for changing the Status of missing documents.
	 * 
	 * @param exportedRecords
	 * @param currentDate
	 */
	private void changeStatusForMissingDocs(Map<String, DdpExportMissingDocs> exportedRecords,Calendar currentDate) {
		
		for(DdpExportMissingDocs docs : exportedRecords.values()) {
			//System.out.println("Documents id : "+docs.getMisId());
			docs.setMisStatus(1);
			docs.setMisLastProcessedDate(currentDate);
			ddpExportMissingDocsService.updateDdpExportMissingDocs(docs);
		}
	}
	
	/**
	 * Method used for sending the mail to success exports.
	 * 
	 * @param exportedRecords
	 * @param exportRule
	 * @param appName
	 * @param typeOfService
	 * @param currentDate
	 * @param startDate
	 * @param tableBody
	 * @param hostName
	 * @param destLoc
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
			String serviceName = applicationProperties.getProperty("ruleByQuery."+typeOfService);
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = applicationProperties.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = applicationProperties.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = applicationProperties.getProperty("export.rule."+appName+".mail.success.subject");
			String body = applicationProperties.getProperty("export.rule."+appName+".mail.success.body");
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
	 * Method used for sending the mail for missing documents.
	 * 
	 * @param missingRecords
	 * @param exportRule
	 * @param tmpFolder
	 * @param appName
	 * @param typeOfService
	 * @param currentDate
	 * @param subMissingDocs
	 */
	private void sendMailForMissingDocuments(Map<String,DdpExportMissingDocs> missingRecords,DdpExportRule exportRule,String tmpFolder,String appName,int typeOfService,Calendar currentDate,
			Map<String,List<String>> subMissingDocs,Calendar startDate,Calendar presentDate,Date endDate,String dynamicValues) {
		
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
			
			for (String rObjectID : missingRecords.keySet()) {
				
				
				DdpExportMissingDocs export = missingRecords.get(rObjectID);
				List<String> docTypes = subMissingDocs.get(rObjectID);
				if (docTypes == null || docTypes.size() == 0) {
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
				} else {
					for (String docType : docTypes) {
						csvFile.append(export.getMisEntryType());
						csvFile.append(",");
						csvFile.append(export.getMisMasterJob());
						csvFile.append(",");
						csvFile.append(export.getMisJobNumber());
						csvFile.append(",");
						csvFile.append(export.getMisConsignmentId());
						csvFile.append(",");
						csvFile.append(docType);
						csvFile.append("\n");
					}
				}
				
			}
			csvFile.flush();
			csvFile.close();
			
			String smtpAddress = applicationProperties.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotSuccessEmailAddress(); 
			String ccAddress = notification.getNotFailureEmailAddress();
			String fromAddress = applicationProperties.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = applicationProperties.getProperty("export.rule."+appName+".mail.failure.subject");
			String body = applicationProperties.getProperty("export.rule."+appName+".mail.failure.body");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = applicationProperties.getProperty("ruleByQuery."+typeOfService);
			
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%ENDTIME%%", dateFormat.format(endDate));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endDate));
			
			if (toAddress == null || toAddress.length()== 0)
				toAddress = applicationProperties.getProperty("mail.toAddress");
			
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
	 * Method used for sending the mail to success exports.
	 * 
	 * @param exportedRecords
	 * @param exportRule
	 * @param appName
	 * @param typeOfService
	 * @param endDate
	 * @param startDate
	 * @param tableBody
	 * @param hostName
	 * @param destLoc
	 */
	private void sendMailForEmptyRecords(DdpExportRule exportRule,String appName,int typeOfService,Calendar endDate,Calendar startDate,Calendar presentDate,String content,String dynamicValues) {
		//TODO
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		
		try {
			
			DdpCommunicationSetup commSetup = exportRule.getExpCommunicationId();
			boolean isFTPType =	commSetup.getCmsCommunicationProtocol().equalsIgnoreCase("FTP");
			boolean isSMTPType = commSetup.getCmsCommunicationProtocol().equalsIgnoreCase("smtp");
			
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
			} else if (isSMTPType) {
				
				DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId());
				destinationLocation = "SMTP";
				hostName = ddpCommEmail.getCemEmailTo();
				
			} else {
				DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId());
				destinationLocation = uncDetails.getCunUncPath();
				hostName = commonUtil.getHostName(uncDetails.getCunUncPath());
			}
			
			if (commSetup.getCmsCommunicationProtocol2() != null && !commSetup.getCmsCommunicationProtocol2().isEmpty()) {
			
				isFTPType =	commSetup.getCmsCommunicationProtocol2().equalsIgnoreCase("FTP")? true : false;
				isSMTPType = commSetup.getCmsCommunicationProtocol2().equalsIgnoreCase("smtp");
				
				if (isFTPType) {
					
					DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2()); 
						//need to split with ftp:// so it 6.
						destinationLocation2 = commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation());				
						hostName2 = commonUtil.getHostName(ftpDetails.getCftFtpLocation());
				} else if (isSMTPType) {
					
					DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2());
					destinationLocation2 = "SMTP";
					hostName2 = ddpCommEmail.getCemEmailTo();
					
				} else {
					DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId2());
					destinationLocation2 = uncDetails.getCunUncPath();
					hostName2 = commonUtil.getHostName(uncDetails.getCunUncPath());
				}
			}
			
			if (commSetup.getCmsCommunicationProtocol3() != null && !commSetup.getCmsCommunicationProtocol3().isEmpty()) {
				
				isFTPType =	commSetup.getCmsCommunicationProtocol3().equalsIgnoreCase("FTP")? true : false;
				isSMTPType = commSetup.getCmsCommunicationProtocol3().equalsIgnoreCase("smtp");
				
				if (isFTPType) {
					
					DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3()); 
						//need to split with ftp:// so it 6.
						destinationLocation3 = commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation());				
						hostName3 = commonUtil.getHostName(ftpDetails.getCftFtpLocation());
				} else if (isSMTPType) {
					
					DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3());
					destinationLocation3 = "SMTP";
					hostName3 = ddpCommEmail.getCemEmailTo();
					
				} else {
					DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(commSetup.getCmsProtocolSettingsId3());
					destinationLocation3 = uncDetails.getCunUncPath();
					hostName3 = commonUtil.getHostName(uncDetails.getCunUncPath());
				}
			}
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = applicationProperties.getProperty("ruleByQuery."+typeOfService);			
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = applicationProperties.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = applicationProperties.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = applicationProperties.getProperty("export.rule."+appName+".mail."+content+".subject");
			String body = applicationProperties.getProperty("export.rule."+appName+".mail."+content+".body");
			//System.out.println("tableBODY : "+tableBody);
			//body = body.replace("%%TABLEDETAILS%%", tableBody);
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(endDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%DESTIONPATH%%", destinationLocation);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			body = body.replace("%%ENDTIME%%", dateFormat.format(new Date()));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), new Date())+"");
			
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
	 * Method used for sending the mail to connection issues like ftp/unc connections.
	 * 
	 * @param exportedRecords
	 * @param exportRule
	 * @param appName
	 * @param typeOfService
	 * @param endDate
	 * @param startDate
	 * @param tableBody
	 * @param hostName
	 * @param destLoc
	 */
	private void sendMailForConnectionIssue(DdpExportRule exportRule,String appName,int typeOfService,Calendar endDate,Calendar startDate,Calendar presentDate,
			List<DdpTransferObject> ddpTransferObjects,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		
		try {
			
			
			String destinationLocation = "";
			String hostName = "";
			DdpTransferObject transferObject = ddpTransferObjects.get(0);
		
			destinationLocation = transferObject.getDestLocation();				
			hostName = transferObject.getHostName();
				
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = applicationProperties.getProperty("ruleByQuery."+typeOfService);
			
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = applicationProperties.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = applicationProperties.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = applicationProperties.getProperty("export.rule."+appName+".mail.transferissue.subject");
			String body = applicationProperties.getProperty("export.rule."+appName+".mail.transferissue.body");
			//System.out.println("tableBODY : "+tableBody);
			//body = body.replace("%%TABLEDETAILS%%", tableBody);
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(endDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%ENDTIME%%", dateFormat.format(new Date()));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), new Date())+"");
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%DESTIONPATH%%", destinationLocation);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			//body = body.replace("%%COUNT%%", FileUtils.countCharacter(tableBody.toString(), '\n')+"");
			String transferBody = "";
			if (ddpTransferObjects.size() > 1) {
				
				for (int i = 1; i < ddpTransferObjects.size(); i++) {
					
					transferObject = ddpTransferObjects.get(i);
					
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
			logger.error("AppName : "+appName+" DdpBackUpDocumentProcess.sendMailForEmptyRecords() - Unable to send the success mail", ex);
		}
		
	}
	
	/**
	 * Method used for performing the download operation.
	 * 
	 * @param objectName
	 * @param rObjectId
	 * @param namingConvention
	 * @param exportDocs
	 * @param sourceFolder
	 * @param isFTPType
	 * @param ftpClient
	 * @param destinationLocation
	 * @param smbFile
	 * @param session
	 * @param versionMap
	 * @param invoiceNumber
	 * @return
	 */
	private boolean performDownloadOperation (String objectName,String rObjectId,DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,
			IDfSession session,Map<String, String> versionMap,String invoiceNumber,StringBuffer mailBody,List<String> fileNameList,String sourceFolder,
			Map<String, String> loadNumberMap,int typeOfService,List<DdpExportSuccessReport> exportedReports) {
		
		String fileName = getDocumentFileName(objectName, namingConvention, exportDocs, invoiceNumber,fileNameList,loadNumberMap);
		if (fileName == null)	 
			fileName = objectName ;
			
		// need to check all version also
		//DdpRuleDetail detail = ruleDetailMap.get(exportDocs.getMisDocType());
		 boolean isDownload = false;
		// boolean isFirstRec = true;
		 
		/* if (mailBody != null && mailBody.length() != 0)
			 isFirstRec = false;*/
		 
		if (versionMap.containsKey(exportDocs.getMisDocType()) && versionMap.get(exportDocs.getMisDocType()).equalsIgnoreCase("All")) {
			isDownload = downloadAllVersion(objectName, rObjectId, namingConvention, exportDocs, session,invoiceNumber,mailBody,fileNameList,sourceFolder,loadNumberMap,typeOfService,exportedReports);
		
		} else {
			isDownload  = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(rObjectId, sourceFolder, fileName, session);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
			if (isDownload) {
				//mailBody.append("<tr><td>"+dateFormat.format(new Date())+"</td>");
				mailBody.append(dateFormat.format(new Date())+",");
				dateFormat.applyLocalizedPattern("HH:mm:ss");
				//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+exportDocs.getMisCheckDigit()+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
				mailBody.append(dateFormat.format(new Date())+",receive,success,"+exportDocs.getMisCheckDigit()+",00:00:01,"+exportDocs.getMisConsignmentId()+","+exportDocs.getMisJobNumber()+","+fileName+"\n");
				if (typeOfService == 1 || typeOfService == 3) {
					exportedReports.add(commonUtil.constructExportReportDomainObject(exportDocs, Long.parseLong(exportDocs.getMisCheckDigit()), fileName, typeOfService));
				}
			}
			
		}
			
		
		return isDownload;
	}
	
	/**
	 * Method used for downloading the all version form the DFC.
	 * 
	 * @param objectName
	 * @param rObjectID
	 * @param namingConvention
	 * @param exportDocs
	 * @param sourceFolder
	 * @param isFTPType
	 * @param ftpClient
	 * @param destinationLocation
	 * @param smbFile
	 * @param session
	 * @param invoiceNumber
	 * @return
	 */
	private boolean downloadAllVersion(String objectName,String rObjectID,DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,
				IDfSession session,String invoiceNumber,StringBuffer mailBody,List<String> fileNameList,String sourceFolder,
				Map<String, String> loadNumberMap,int typeOfService,List<DdpExportSuccessReport> exportedReports) {
		
		logger.info("DdpBackUpDocumentProcess -downloadAllVersion() is invoked for object id : "+rObjectID);
		boolean isAllVersionDown = false;
		//boolean isFirstRec = false;
		DdpDFCClient client =  new DdpDFCClient();
		Hashtable<String, String> dmsMetadata = client.begin(session,rObjectID);
		
   	 	logger.debug("DdpBacDocumentProcess.downloadAllVersion() : Chornicle id : "+dmsMetadata.get("i_chronicle_id"));
   	 	try {
   	 		//and agl_creation_date between Date('"+startDate+"','dd/mm/yyyy HH:mi:ss') and Date('"+endDate+"','dd/mm/yyyy HH:mi:ss')"
		    String query = "select object_name, r_object_id,  r_version_label,r_content_size from agl_control_document (all) where i_chronicle_id= '"+dmsMetadata.get("i_chronicle_id")+"'";
		    IDfQuery dfQuery = new DfQuery();
			dfQuery.setDQL(query);
			IDfCollection dfCollection = dfQuery.execute(session,
					IDfQuery.DF_EXEC_QUERY);
			
			while (dfCollection.next()) {
				boolean isExtVersionReq = false;
				String fileName = getDocumentFileName(dfCollection.getString("object_name"), namingConvention, exportDocs,invoiceNumber,fileNameList,loadNumberMap);
				if (fileName == null) {
					isExtVersionReq = true;
					fileName = objectName ;
				}
				client.exportDocumentToLocal( dfCollection.getString("r_version_label"), fileName, dfCollection.getString("r_object_id"),sourceFolder, session,isExtVersionReq);
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
				//	if (isFirstRec) {
				//	mailBody.append("<tr><td>"+dateFormat.format(new Date())+"</td>");
					/*} else {
						mailBody += "<tr><td></td>";
					}*/
					//dateFormat.applyLocalizedPattern("HH:mm:ss");
					//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+dfCollection.getString("r_content_size")+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
				mailBody.append(dateFormat.format(new Date())+",");
				dateFormat.applyLocalizedPattern("HH:mm:ss");
				//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+exportDocs.getMisCheckDigit()+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
				mailBody.append(dateFormat.format(new Date())+",receive,success,"+exportDocs.getMisCheckDigit()+",00:00:01,"+exportDocs.getMisConsignmentId()+","+exportDocs.getMisJobNumber()+","+fileName+"\n");
				fileNameList.add(fileName);
				
				if (typeOfService == 1 || typeOfService == 3) {
					exportedReports.add(commonUtil.constructExportReportDomainObject(exportDocs, Long.parseLong(exportDocs.getMisCheckDigit()), fileName, typeOfService));
				}
			}
			isAllVersionDown = true;
   	 	} catch(Exception ex) {
   	 		logger.error("DdpBackUpDocumentProcess - downloadAllVersion() : Error occurried while downloading all version", ex);
   	 	}
   	 	return isAllVersionDown;
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
	private String getDocumentFileName (String objectName,DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,String invoiceNumber, List<String> fileNameList,Map<String, String> loadNumberMap) {
		
		String fileName = null;
		
		//check the name is already exists in the destination (FTP & Local) locations. if exists then add sequence number.
		 if  (namingConvention != null) 
			 fileName = commonUtil.getDocumentName(objectName, namingConvention, exportDocs, invoiceNumber,fileNameList,loadNumberMap);
		 else 
			 fileName = commonUtil.getFileNameWithCopy(objectName, fileNameList, null, null);
		 
		 return fileName;
	}
		
    		
	/**
	 * Method used for checking the all mandatory & alteast on document type is available.
	 * 
	 * @param mandatoryDocTypes
	 * @param atleastDocTypes
	 * @param primaryDocTypeList
	 * @param iDfCollection
	 * @return
	 */
	private List<DdpExportMissingDocs> isDocExportable(Set<String> mandatoryDocTypes, Set<String> atleastDocTypes,Set<String> primaryDocTypeList ,String rObjectID,Map<String,List<String>> subMissingDocs,List<DdpExportMissingDocs> backUpDocuments ) {
		
		if (backUpDocuments == null || backUpDocuments.size() == 0) {
			return null;
		}
		
		boolean isAllDocsAvailable = false;
		Set<String> docTypeList = new HashSet<String>();
		List<String> missType = new ArrayList<String>();
		
		
		
		//By default the primary docType details are fetched from the system.
		docTypeList.addAll(primaryDocTypeList);
		
		try {
			for (DdpExportMissingDocs missingDocs : backUpDocuments) {
				docTypeList.add(missingDocs.getMisDocType());
			}
			
		} catch (Exception ex) {
			logger.error("DdpBackUpDocumentProcess.isDocExportable() - while iterating DFCCollection",ex.getMessage());
			//System.out.println("DdpBackUpDocumentProcess.isDocExportable() -  Madatory documents are not able ");	
			backUpDocuments = null;
			//return subExportDocuments;
		}
		
		//Checking all the mandatory documents.
		for (String mandatoryDocs : mandatoryDocTypes) {
			if (!docTypeList.contains(mandatoryDocs)) {
				//System.out.println("DdpBackUpDocumentProcess.isDocExportable() -  Madatory documents are not able " );
				backUpDocuments = null;
				missType.add(mandatoryDocs);
				//return subExportDocuments;
			}
		}
		
		if (atleastDocTypes.size() > 0) {
			for (String atleastDocs : atleastDocTypes) {
				if (docTypeList.contains(atleastDocs)) {
					isAllDocsAvailable = true;
					break;
				}
			}
			
			if (!isAllDocsAvailable) {
				backUpDocuments = null;
				missType.addAll(atleastDocTypes);
			}
		}
		
		if (backUpDocuments == null) {
			subMissingDocs.put(rObjectID, missType);
		}
		
		return backUpDocuments;
	}
	
	
	/**
	 * Method used to get the Matched RuleDetails using scheduler id.
	 * 
	 * @param intSchId
	 * @return
	 */
	public List<DdpRuleDetail> getMatchedSchdIDAndRuleDetailForExport(Integer intSchId)
    {
		logger.debug("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() method invoked.");
                   
		List<DdpRuleDetail> ruleDetails = null;
                    
		try
        {
			ruleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_SCH_RULE_DET_EXPORT, new Object[] {intSchId}, new RowMapper<DdpRuleDetail>() {
            
				public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpRuleDetail rdtId = ddpRuleDetailService.findDdpRuleDetail(rs.getInt("RDT_ID"));
					return rdtId;
				}
			});
		}
        catch(Exception ex)
        {
        	logger.error("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() - Exception while retrieving Matched Schduler ID and Rule Detail ID for Export rules - Error message [{}].",ex.getMessage());
        	ex.printStackTrace();
        }
        
		
        System.out.println("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() executed successfully.");
        return ruleDetails;
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
					logger.error("DdpBackDocumentProcess.constructMissingDocs() - Unable to parse string to date",ex.getMessage());
				}
				missingDocs.add(exportDocs);
			}
		} catch (Exception ex) {
			missingDocs = new ArrayList<DdpExportMissingDocs>();
			logger.error("DdpBackDocumentProcess.constructMissingDocs() - Unable to construct the Missing docs", ex.getMessage());
		}
		return missingDocs;
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
			logger.error("DdpBackDocumentProcess.getIDFCollectionDetails(String query,IDfSession session) - unable to get details of DFCCollections", ex.getMessage());
			taskUtil.sendMailByDevelopers("Unable to execute this query : "+query+"<br/>"+ex, "DdpBackupDocumentProcess.getIDFCollection() not able to fetch.");
			ex.printStackTrace();
		}
		return dfCollection;
	}
	
	
	/**
	 * 
	 * @param ruleDetailMap
	 * @return
	 */
	private Map<String,String> getExportVersionSetupDetails(Map<String,DdpRuleDetail> ruleDetailMap) {
		
		Map<String,String> exportOptions = new HashMap<String, String>();
		try {
			
			for (String docTypeName : ruleDetailMap.keySet()) {
				DdpRuleDetail rule = ruleDetailMap.get(docTypeName);
				List<String> optionNames = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MACTHED_VERSION_SETUP_ID, new Object[] {rule.getRdtId()},  new RowMapper<String>() {
					
					public String mapRow(ResultSet rs, int rowNum) throws SQLException {
						
						
						return rs.getString("EVS_OPTION");
			           }
				});		
				
				if (optionNames.size() > 0) {
					exportOptions.put(docTypeName, optionNames.get(0));
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return exportOptions;
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
	 * Method used for constructing the DQL query.
	 * 
	 * @param ddpScheduler
	 * @param exportRule
	 * @param appName
	 * @return
	 */
	private String constructDyanmicDQLQuery(DdpScheduler ddpScheduler,DdpExportRule exportRule,String appName) {
		
		String query = "";
		
		if (ddpScheduler.getSchQuerySource() == null || ddpScheduler.getSchQuerySource().intValue() == 0) {
			
			query = applicationProperties.getProperty("export.rule."+appName+".customQuery");
			
		} else if (ddpScheduler.getSchQuerySource().intValue() == 1) {
			
			List<DdpExportQueryUi> queryUis = commonUtil.getExportQueryUiByRuleID(exportRule.getExpRuleId());
			query = commonUtil.constructQueryWithExportQueryUIs(queryUis,"export.rule."+appName+".customQuery.byUI","export.rule.customQuery.byUI");
			
		} else {
			
			List<DdpExportQuery> queryList = commonUtil.getExportQueryByRuleID(exportRule.getExpRuleId());
			if (queryList != null && queryList.size() > 0)
				query = commonUtil.constructQueryFromTXT(queryList.get(0).getExqQuery(), appName);
			
		}
		
		if (!query.toLowerCase().contains("%%PRIMARYDOCTYPE%%".toLowerCase())) 
			query += " and (agl_control_doc_type in (%%PRIMARYDOCTYPE%%))";
		
		if (!query.toLowerCase().contains("'%%startdate%%".toLowerCase()) && !query.toLowerCase().contains("'%%enddate%%".toLowerCase()))
			query += " and ( agl_creation_date between Date('%%startdate%%','dd/mm/yyyy HH:mi:ss') and Date('%%enddate%%','dd/mm/yyyy HH:mi:ss'))";
		
		return query;
	}
	
	
	/**
	 * Method used for constructing the DQL query.
	 * 
	 * @param ddpScheduler
	 * @param exportRule
	 * @param appName
	 * @return
	 */
	private String constructOnDemandDyanmicDQLQuery(DdpScheduler ddpScheduler,DdpExportRule exportRule,String appName,Integer typeOfService) {
		
		String query = "";
		
		if (ddpScheduler.getSchQuerySource() == null || ddpScheduler.getSchQuerySource().intValue() == 0) {
			
			query = applicationProperties.getProperty("export.rule."+appName+".customQuery."+applicationProperties.getProperty(typeOfService+""));
			
		} else if (ddpScheduler.getSchQuerySource().intValue() == 1) {
			
			List<DdpExportQueryUi> queryUis = commonUtil.getExportQueryUiByRuleID(exportRule.getExpRuleId());
			query = commonUtil.constructQueryWithExportQueryUIs(queryUis,"export.rule."+appName+".customQuery."+applicationProperties.getProperty(typeOfService+"")+".byUI","export.rule.customQuery."+applicationProperties.getProperty(typeOfService+"")+".byUI");
			
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
    
}
