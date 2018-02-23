package com.agility.ddp.core.quartz;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Resource;
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
import com.agility.ddp.core.logger.AuditLog;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.DdpDFCClient;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommEmailService;
import com.agility.ddp.data.domain.DdpCompressionSetup;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpEmailTriggerSetup;
import com.agility.ddp.data.domain.DdpEmailTriggerSetupService;
import com.agility.ddp.data.domain.DdpExportVersionSetup;
import com.agility.ddp.data.domain.DdpMultiAedRule;
import com.agility.ddp.data.domain.DdpMultiAedRuleService;
import com.agility.ddp.data.domain.DdpMultiAedSuccessReport;
import com.agility.ddp.data.domain.DdpMultiAedSuccessReportService;
import com.agility.ddp.data.domain.DdpMultiEmails;
import com.agility.ddp.data.domain.DdpRateSetup;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpRuleService;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;

/**
 * @author DGuntha
 *
 */
@Configuration
//@PropertySource({"file:///E:/DDPConfig/custom.properties","file:///E:/DDPConfig/mail.properties","file:///E:/DDPConfig/multiaedmail.properties","file:///E:/DDPConfig/ddp.properties"})
public class DdpMultiAedRuleJob extends QuartzJobBean  {

	private static final Logger logger = LoggerFactory.getLogger(DdpMultiAedRuleJob.class);
	private static final String ATTACHMENT = "attachment";
	private static final String ZIP = "zip_folder";
	private static final String MULTI_AED = "multi_aed_";
	
	 @Autowired
	 private ApplicationProperties env;
	
	 @Autowired
	 @Resource(name = "jdbcTemplate")
	 private JdbcTemplate jdbcTemplate;
	 
	 @Autowired
	 private DdpRuleDetailService ddpRuleDetailService;
	 
	 @Autowired
	 private DdpRuleService ddpRuleService;
	 
	 @Autowired
	 private DdpCategorizedDocsService ddpCategorizedDocsService;
	 
	 @Autowired
	 private TaskUtil taskUtil;
	 
	 @Autowired
	 @Resource(name = "controlJdbcTemplate")
	 private JdbcTemplate controlJdbcTemplate;
		
	 @Autowired
	 private DdpCommEmailService ddpCommEmailService;
	 
	// @Value( "${ddp.export.folder}" )
	// private String tempFilePath;
	 
	@Autowired
	private	DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpMultiAedRuleService ddpMultiAedRuleService;
	 
	@Autowired
	private DdpEmailTriggerSetupService ddpEmailTriggerSetupService;

	@Autowired
	private DdpMultiAedSuccessReportService ddpMultiAedSuccessReportService;
	
	@Autowired
    private ApplicationProperties applicationProperties;
	
	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		
		logger.info("MultiAEDRuleJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("MultiAEDRuleJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("MultiAEDRuleJob.executeInternal() executed successfully.");

	}
	
	/**
	 * Method used for the MultiAed Rule checking.
	 * 
	 * @param categorizedDocs
	 */
	@AuditLog(dbAuditBefore=true, message="MultiAEDRuleJob.callWorkFlow(categorizedDocs) Method Invoked.")
	public void callWorkFlow(Object[] categorizedDocs) {
		
		logger.info("DdpMultiAedRuleJob.callWorkFlow(categorizedDocs) Method Invoked.");
		Scheduler scheduler  = null;
		DdpCategorizedDocs ddpCategorizedDoc = (DdpCategorizedDocs) categorizedDocs[0];
		if (categorizedDocs.length > 1)
			scheduler = (Scheduler)categorizedDocs[1];
		
		if (ddpCategorizedDoc.getCatRulId() != null && ddpCategorizedDoc.getCatStatus() == 0) {
			
			List<DdpRuleDetail> ddpRuleDetails = getAllRuleDetails(ddpCategorizedDoc.getCatRulId().getRulId());
			
			if (ddpRuleDetails == null || ddpRuleDetails.size() == 0) {
				logger.info("DdpMultiAedRuleJob.callWorkFlow(categorizedDocs) - Empty rule detail for the rule id : "+ddpCategorizedDoc.getCatRulId().getRulId());
				return ;
			}
			DdpEmailTriggerSetup ddpEmailTriggerSetup = getDdpEmailTriggerSetup(ddpCategorizedDoc.getCatRulId().getRulId());
			try {
				commonUtil.addConsolidatedAEDThread("ConsolidateAED-TriggerID-callWorkFlow :"+ddpEmailTriggerSetup.getEtrId());
				executeJob(ddpRuleDetails,ddpEmailTriggerSetup,null,null,1,ddpCategorizedDoc);
			} catch (Exception ex) {
				logger.error("DdpMultiAedRuleJob.callWorkFlow() - Unable to executJob ", ex);
			} finally {
				if (scheduler != null)
					try {
						scheduler.shutdown();
					} catch (SchedulerException e) {
						logger.error("DdpMultiAedRuleJob.callWorkFlow() - Unable to colse the scheduler ", e);
					}
				if (ddpEmailTriggerSetup != null)
					commonUtil.removeConsolidatedAEDThread("ConsolidateAED-TriggerID-callWorkFlow :"+ddpEmailTriggerSetup.getEtrId());
			}
		}
		
		logger.info("DdpMultiAedRuleJob.callWorkFlow(categorizedDocs) completed successfully.");
	}

	/**
	 * Method used for calling the work flow with Batch of records.
	 * 
	 * @param categorizedDocs
	 */
	@AuditLog(dbAuditBefore=true, message="MultiAEDRuleJob.callWorkFlowWithBulkRecords(categorizedDocs) Method Invoked.")
	public void callWorkFlowWithBulkRecords(Object[] categorizedDocs) {
		
		logger.info("DdpMultiAedRuleJob.callWorkFlow(categorizedDocs) Method Invoked.");
		Scheduler scheduler  = null;
		List<DdpCategorizedDocs> ddpCategorizedDocList = (List<DdpCategorizedDocs>) categorizedDocs[0];
		if (categorizedDocs.length > 1)
			scheduler = (Scheduler)categorizedDocs[1];
		
		
		logger.info("MultiAEDRuleJob.callWorkFlowWithBulkRecords() - list categroized docs : "+ddpCategorizedDocList.stream().map(DdpCategorizedDocs::getCatId).collect(Collectors.toList()));
		try {
			for (DdpCategorizedDocs ddpCategorizedDoc : ddpCategorizedDocList) {
			
				if (ddpCategorizedDoc.getCatRulId() != null && ddpCategorizedDoc.getCatStatus() == 0) {
					
					List<DdpRuleDetail> ddpRuleDetails = getAllRuleDetails(ddpCategorizedDoc.getCatRulId().getRulId());
					
					if (ddpRuleDetails == null || ddpRuleDetails.size() == 0) {
						logger.info("DdpMultiAedRuleJob.callWorkFlow(categorizedDocs) - Empty rule detail for the rule id : "+ddpCategorizedDoc.getCatRulId().getRulId());
						return ;
					}
					DdpEmailTriggerSetup ddpEmailTriggerSetup = getDdpEmailTriggerSetup(ddpCategorizedDoc.getCatRulId().getRulId());
					try {
						commonUtil.addConsolidatedAEDThread("ConsolidateAED-TriggerID-callWorkFlow :"+ddpEmailTriggerSetup.getEtrId()+"-"+ddpCategorizedDoc.getCatId());
						executeJob(ddpRuleDetails,ddpEmailTriggerSetup,null,null,1,ddpCategorizedDoc);
					} catch (Exception ex) {
						logger.error("DdpMultiAedRuleJob.callWorkFlow() - Unable to executJob ", ex);
					} finally {
						if (ddpEmailTriggerSetup != null)
							commonUtil.removeConsolidatedAEDThread("ConsolidateAED-TriggerID-callWorkFlow :"+ddpEmailTriggerSetup.getEtrId()+"-"+ddpCategorizedDoc.getCatId());
					}
				}
			}
		} catch (Exception ex) {
			
		} finally {
			if (scheduler != null)
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpMultiAedRuleJob.callWorkFlow() - Unable to colse the scheduler ", e);
				}
		}
		logger.info("DdpMultiAedRuleJob.callWorkFlow(categorizedDocs) completed successfully.");
	}
	
	/**
	 * Method used for initiating the scheduler job for the Multi AED rule.
	 * 
	 * @param ddpEmailTiggerSetup
	 */
	@AuditLog(dbAuditBefore=true, message="MultiAEDRuleJob.initiateSchedulerJob(Object[] ddpEmailTiggerSetup) Method Invoked.")
	public void initiateSchedulerJob(Object[] ddpEmailTiggerSetup)  {
		
		Calendar startCalendar = GregorianCalendar.getInstance();
		Calendar endCalendar = GregorianCalendar.getInstance();
		
		DdpEmailTriggerSetup ddpTiggerSetup = (DdpEmailTriggerSetup)ddpEmailTiggerSetup[0];
		try {
			commonUtil.addConsolidatedAEDThread("ConsolidateAED-TriggerID-initiateSchedulerJob :"+ddpTiggerSetup.getEtrId());
			logger.info("DdpMultiAedRuleJob.initiateSchedulerJob() - Running Scheduler Email Tigger ID is "+ddpTiggerSetup.getEtrId());
			//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
			String strFeq = SchedulerJobUtil.getSchFreq(ddpTiggerSetup.getEtrCronExpression());
			   
			//Call below method to get date range - This needs to be implemented
			Calendar queryStartDate = SchedulerJobUtil.getQueryStartDate(strFeq, startCalendar,ddpTiggerSetup.getEtrCronExpression());
			Calendar queryEndDate = SchedulerJobUtil.getQueryEndDate(strFeq, endCalendar,ddpTiggerSetup.getEtrCronExpression());
			
			//Checking the rule is deleted or not. If the rule deleted, in rule detail table values will be empty.		
			if (ddpTiggerSetup != null) {
				List<DdpRuleDetail> ddpRuleDetails = getAllRuleDetails(ddpTiggerSetup.getEtrRuleId().getRulId(),GregorianCalendar.getInstance());
				
				if (ddpRuleDetails == null || ddpRuleDetails.size() == 0) {
					logger.info("DdpMultiAedRuleJob.initiateSchedulerJob(Object[] ddpEmailTiggerSetup) - Empty rule detail for the rule id : "+ddpTiggerSetup.getEtrRuleId().getRulId());
					return ;
				}
				
				executeJob(ddpRuleDetails,ddpTiggerSetup,queryStartDate,queryEndDate,1,null);
			}
		} catch (Exception ex) {
			logger.error("DdpMultiAedRuleJob.initiateSchedulerJob() - Unable to peform consolidated AED for trigger id "+ddpTiggerSetup.getEtrId(), ex);
		} finally {
			commonUtil.removeConsolidatedAEDThread("ConsolidateAED-TriggerID-initiateSchedulerJob :"+ddpTiggerSetup.getEtrId());
		}
	}
	
	/**
	 * Method used for executing the job.
	 * 
	 * @param ddpCategorizedDocs
	 * @return
	 */
	public boolean executeJob(List<DdpRuleDetail> ddpRuleDetails,DdpEmailTriggerSetup ddpEmailTriggerSetup,
			Calendar queryStartDate,Calendar queryEndDate,int typeOfService,DdpCategorizedDocs ddpCategorizedDoc) {
		
		
		logger.info("DdpMultiAedRuleJob.executeJob(categorizedDocs) Method Invoked. with type of service : "+(typeOfService == 1?"General Service":"Reprocess Service"));
		boolean isMailTiggered = false;	
		 isMailTiggered = executeConsolidatedJob(ddpRuleDetails, ddpEmailTriggerSetup, queryStartDate, queryEndDate, typeOfService,ddpCategorizedDoc);
		//boolean isAllMandiatoryDocsAvail = false;
//			//Key: Job Number & Value: document type
//			Map<String,String> documentTypeMap = new HashMap<String, String>();
//			//Job Number & branch code
//			Map<String,String> branchMap = new HashMap<String, String>(); 
//			Map<String,DdpCategorizedDocs> ddpCatDocsMap = new HashMap<String,DdpCategorizedDocs>();
//			//Map<Integer, DdpRuleDetail> sortedDdpRuleDetailMap = new TreeMap<Integer, DdpRuleDetail>();
//			Map<String,TreeMap<Integer, DdpRuleDetail>> branchRuleDetails = new HashMap<String, TreeMap<Integer,DdpRuleDetail>>();
//			Map<String,Integer> retryCount = new HashMap<String, Integer>();
//			List<DdpCategorizedDocs> existingList = new ArrayList<DdpCategorizedDocs>();
//			DdpCommEmail ddpCommEmails = null;
//			//Cat ID,
//			String commProtocol = ddpRuleDetails.get(0).getRdtCommunicationId().getCmsCommunicationProtocol();
//		    
//			if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
//				ddpCommEmails = ddpCommEmailService.findDdpCommEmail(Integer.parseInt(ddpRuleDetails.get(0).getRdtCommunicationId().getCmsProtocolSettingsId()));
//			}
//				
//		//	if (Constant.TRIGGER_NAME_SPECIFIC_DOCS.equals(ddpEmailTriggerSetup.getEtrTriggerName()) && ddpEmailTriggerSetup.getEtrDocTypes() != null && !ddpEmailTriggerSetup.getEtrDocTypes().isEmpty()) 
//			//	docType = Arrays.asList(ddpEmailTriggerSetup.getEtrDocTypes().split(","));
//		//	else if (Constant.TRIGGER_NAME_ALL_OR_MINIMUM.equalsIgnoreCase(ddpEmailTriggerSetup.getEtrTriggerName()) && ddpEmailTriggerSetup.getEtrDocSelection() != null && "1".equals(ddpEmailTriggerSetup.getEtrDocSelection()))
//			//	isAllDocsRequired = true;
//			
//			//List of the documents for AED process.
//			for (DdpRuleDetail ddpRuleDetail : ddpRuleDetails) {
//				
//				if (ddpRuleDetail.getRdtDocSequence() == null) {
//					logger.info("DdpMultiAedRule.executeJob() Rule detail sequence is null for rule detail id : "+ddpRuleDetail.getRdtId());
//					return isMailTiggered ;
//				}
//				
//				//Arranging the detail based on the Branch wise.
//				if (!branchRuleDetails.containsKey(ddpRuleDetail.getRdtBranch().getBrnBranchCode())) {
//					TreeMap<Integer, DdpRuleDetail> sortedDdpRuleDetailMap = new TreeMap<Integer, DdpRuleDetail>();
//					sortedDdpRuleDetailMap.put(ddpRuleDetail.getRdtDocSequence(), ddpRuleDetail);
//					branchRuleDetails.put(ddpRuleDetail.getRdtBranch().getBrnBranchCode(), sortedDdpRuleDetailMap);
//				} else {
//					TreeMap<Integer, DdpRuleDetail> sortedDdpRuleDetailMap = branchRuleDetails.get(ddpRuleDetail.getRdtBranch().getBrnBranchCode());
//					sortedDdpRuleDetailMap.put(ddpRuleDetail.getRdtDocSequence(), ddpRuleDetail);
//					branchRuleDetails.put(ddpRuleDetail.getRdtBranch().getBrnBranchCode(), sortedDdpRuleDetailMap);
//				}
//				
//				//forcedTypeMap.put(ddpRuleDetail.getRdtDocType().getDtyDocTypeCode(), ddpRuleDetail.getRdtForcedType());
//				//Based on the Relevant Type is 1 then fetch all the documents. i.e., on the Primary documents are fetched.
//				if (ddpRuleDetail.getRdtRelavantType() != null && ddpRuleDetail.getRdtRelavantType() == 1) {
//					
//					List<DdpCategorizedDocs> categorizedDocs = getMatchedCategorizedDocs(ddpRuleDetail.getRdtRuleId().getRulId(),ddpRuleDetail.getRdtId(),queryStartDate,queryEndDate,typeOfService);
//					for (DdpCategorizedDocs categorizedDoc : categorizedDocs) {
//						List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(categorizedDoc.getCatDtxId().getDtxId());
//						
//						if (ddpDmsDocsDetails != null && ddpDmsDocsDetails.size() > 0) {
//							//TODO : Based on the triggered type need to include put the key value.
//							//Sending only the latest document to user.
//							if (!ddpCatDocsMap.containsKey(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef())) {
//								
//								ddpCatDocsMap.put(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef(),categorizedDoc);
//								retryCount.put(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef(), categorizedDoc.getCatRetries());
//								branchMap.put(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef(), ddpRuleDetail.getRdtBranch().getBrnBranchCode());
//								documentTypeMap.put(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef(), ddpDmsDocsDetails.get(0).getDddControlDocType());
//							}else {
//								existingList.add(categorizedDoc);
//							}
//						}
//					}
//				}
//				
//			}
//			
//			if (ddpCatDocsMap.size()  > 0) {
//				//if multiple primary documents available if different job numbers or consignment id.
//				for (String uniqueNumber : ddpCatDocsMap.keySet()) {
//					
//					String uniqueRefNumber = uniqueNumber.substring(0, uniqueNumber.indexOf("-"));
//							
//					DdpCategorizedDocs categorizedDocs = ddpCatDocsMap.get(uniqueNumber);
//					TreeMap<Integer, DdpRuleDetail> sortedDdpRuleDetailMap = branchRuleDetails.get(branchMap.get(uniqueNumber));
//					
//					//TODO : Based on the triggered type of the job number/consignment id need to fetch the ddp categorized docs details. 
//					List<DdpCategorizedDocs> list = getDdpCategorizedDocsBasedOnJobNumber(uniqueRefNumber, categorizedDocs.getCatRulId().getRulId(),documentTypeMap.get(uniqueNumber));
//					list.add(categorizedDocs);
//					
//					Integer retryNumber = retryCount.get(uniqueNumber);
//					boolean isStatusChangeRequired = isStatusChangeRequiredToCat(retryNumber, ddpEmailTriggerSetup);
//					List<DdpRuleDetail> missingRuleDetails = new ArrayList<DdpRuleDetail>();
//					//Integer as the Categorized id.
//					List<DdpCategorizedDocs> ddpCategorizedDcosList  =  checkAllDocumentsAvailable(list, missingRuleDetails, sortedDdpRuleDetailMap);
//					if (missingRuleDetails.size() > 0) {
//						if (typeOfService == 2) {
//							updatCategorizedDocumentsRetryCount(ddpCategorizedDcosList,uniqueNumber,retryNumber,isStatusChangeRequired);
//							sendMailFailureMultiAEDRules(ddpCommEmails, missingRuleDetails, typeOfService, retryNumber, ddpEmailTriggerSetup,  uniqueRefNumber,true);
//						} else if (typeOfService == 1) {
//							sendMailFailureMultiAEDRules(ddpCommEmails, missingRuleDetails, typeOfService, retryNumber, ddpEmailTriggerSetup,  uniqueRefNumber,false);
//						}
//					} else {
//						if ( list.size() > 0)
//							isMailTiggered = performMailTriggerProcess(ddpCategorizedDcosList,ddpRuleDetails.get(0),uniqueRefNumber,typeOfService,retryNumber,isStatusChangeRequired,ddpEmailTriggerSetup,ddpCommEmails,documentTypeMap.get(uniqueNumber));
//					}
//				}
//				
//			}
//			
//			//TODO : need to send the failure email's.
//			//if (ddpCategorizedDcosList.size() == 0) 
//				//logger.info("DdpMultiAedRuleJob.executeJob(categorizedDocs) no records found Categorized Docs for Email Trigger Setup ID : "+ddpEmailTriggerSetup.getEtrId());
//			//Setting to status 4, due to duplicate primary records while processing.
//			if (existingList.size() > 0) {
//				for (DdpCategorizedDocs docs : existingList) {
//					docs.setCatStatus(4);
//					ddpCategorizedDocsService.updateDdpCategorizedDocs(docs);
//					logger.info("DdpMultiAedRule.executeJob() - Existing Primary Categoized docs & CAT_ID : "+docs.getCatId()+" Status changed to 4 as duplicate record.");
//				}
//			}
			logger.info("DdpMultiAedRuleJob.executeJob(categorizedDocs) Successfully completed & status of mail Tiggering : "+isMailTiggered+" : Type of Service : "+(typeOfService == 1?"General Service":"Reprocess Service"));
			
		return isMailTiggered;
	}
	
	
	/**
	 * Method used for updating the status of execution code.
	 * 
	 * @param ddpCategorizedDcosList
	 * @param executionCode
	 */
	private void updateStatusOfCategorizedDocuments(List<DdpCategorizedDocs> ddpCategorizedDcosList, int executionCode) {
		
		//updating the status
		for (DdpCategorizedDocs ddpCategorizedDocs : ddpCategorizedDcosList) {
					
			ddpCategorizedDocs.setCatStatus(executionCode);			
			ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDocs);
					 
			/*List<DdpCategorizedDocs> dupCatDocsList = getMatchedCategorizedDocs(ddpCategorizedDocs.getCatRulId().getRulId(),ddpCategorizedDocs.getCatRdtId(),ddpCategorizedDocs.getCatId());
			if (dupCatDocsList != null && dupCatDocsList.size() > 0) {
				for (DdpCategorizedDocs ddpDocs : dupCatDocsList) {
						ddpDocs.setCatStatus(3);			
						ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpDocs);
					}
			}*/

		}
	}
	
	private void updateStatusForConsoidatedAEDRepot(List<DdpMultiAedSuccessReport> list, int executionCode,int typeOfService) {
		
		for (DdpMultiAedSuccessReport report : list) {
			report.setMaedrServiceType(typeOfService+"");
			ddpMultiAedSuccessReportService.saveDdpMultiAedSuccessReport(report);
		}
		
	}
	
//	/**
//	 * Method used for sending the failure mails.
//	 * 
//	 * @param ddpCommEmails
//	 * @param missingDocuments
//	 * @param typeOfService
//	 * @param primaryDocumentCount
//	 * @param emailTriggerSetup
//	 * @param uniqueNumber
//	 */
//	private void sendMailFailureMultiAEDRules(DdpCommEmail ddpCommEmails,List<DdpRuleDetail> missingDocuments,int typeOfService,Integer primaryDocumentCount,DdpEmailTriggerSetup emailTriggerSetup,String uniqueNumber,boolean isCalRequired) {
//		
//		
//		logger.info("DdpMultiAedRuleJob.sendMailFailureMultiAEDRules() method is inovked");
//		String mailSubject = null;
//		String mailBody = null;
//		int retryPrimaryCount = (primaryDocumentCount == null ? 0 :primaryDocumentCount.intValue());
//		if (isCalRequired)
//			retryPrimaryCount = retryPrimaryCount + 1;
//		
//		DdpMultiAedRule ddpMultiAedRule = ddpMultiAedRuleService.findDdpMultiAedRule(missingDocuments.get(0).getRdtRuleId().getRulId());
//		String toAddress = ddpMultiAedRule.getMaedNotificationId().getNotFailureEmailAddress();
//		try {
//			mailSubject = env.getProperty("multiaedmail.missing.subject."+missingDocuments.get(0).getRdtCompany().getComCompanyCode());
//		} catch (Exception ex) {
//			
//		}
//		if(mailSubject == null || mailSubject.isEmpty()) {
//			try {
//				mailSubject = env.getProperty("multiaedmail.missing.subject");
//			} catch (Exception ex) {
//				mailSubject = "Auto Emailing from DDP";
//			}
//		}
//		
//		mailSubject = mailSubject.replaceAll("%%JOBNUMBER%%", uniqueNumber);
//		
//		try {
//			 mailBody = env.getProperty("multiaedmail.missing.body."+missingDocuments.get(0).getRdtCompany().getComCompanyCode());
//			} catch (Exception ex) {
//				
//			}
//		if(mailBody == null || mailBody.isEmpty()) {
//			try {
//				mailBody = env.getProperty("multiaedmail.missing.body");
//			} catch (Exception ex){
//				mailBody = "<P><h2><font color='orange'>Missing Document Details</font></h2>%%ADDITIONALBODY%%<table border='2'><tr><td>Type of Service</td>"
//						+ "<td>%%TYPEOFSERVICE%%</td></tr><tr><td>Number of mandatory missing</td><td>%%MANDCOUNT%%</td></tr><tr><td>Available Retries</td>"
//						+ "<td>%%RETRIES%%</td></tr></table><table><br><br><tr style='background-color:rgb(244,160,65);'><th>Document Type</th>"
//						+ "<th>%%UNIQUENAME%%</th><th>Company</th><th>Client ID</th></tr></table><br/><span>please do not respond to this mail, "
//						+ "as this account is intended for outbound emails only</span><br/><br/></P>"
//						+ "<IMG SRC='http://www.latc.la/advhtml_upload/agi_gil-home-logo.png' ALT='GIL'>";
//			}
//		}
//		
//		mailBody = mailBody.replaceAll("%%TYPEOFSERVICE%%", (typeOfService == 2) ? "ReProces Service" : "General Service");
//		int retryCount = 0;
//		int subRetryCount = 0;
//		if (emailTriggerSetup.getEtrRetries() != null && isCalRequired) {
//			retryCount = emailTriggerSetup.getEtrRetries().intValue();
//			subRetryCount = retryCount - 1;
//		}
//		if (retryPrimaryCount == subRetryCount && isCalRequired)
//			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%",env.getProperty("multiaed.missing.lastbutonecount"));
//		else if (retryPrimaryCount >= subRetryCount && isCalRequired)
//			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%",env.getProperty("multiaed.missing.lastcount"));
//		else 
//			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%","");
//		
//		mailBody = mailBody.replaceAll("%%MANDCOUNT%%", missingDocuments.size()+"");
//		mailBody = mailBody.replaceAll("%%RETRIES%%", ((emailTriggerSetup.getEtrRetries() == null || emailTriggerSetup.getEtrRetries().intValue() == 0 ? 0 :emailTriggerSetup.getEtrRetries().intValue())-retryPrimaryCount)+"");
//		mailBody = mailBody.replaceAll("%%RULEID%%", missingDocuments.get(0).getRdtRuleId().getRulId()+"");
//		
//		//mailBody = mailBody.replaceAll("%%UNIQUENAME%%", emailTriggerSetup.getEtrTriggerName().toUpperCase());
//		String tableDetails = "";
//		for (DdpRuleDetail ddpRuleDetail : missingDocuments) {
//			tableDetails = tableDetails + "<tr><td>"+ddpRuleDetail.getRdtDocType().getDtyDocTypeCode()+"</td>";
//			tableDetails = tableDetails +"<td>"+uniqueNumber+"</td>";
//			tableDetails = tableDetails + "<td>"+ ddpRuleDetail.getRdtCompany().getComCompanyCode()+"</td>";
//			tableDetails = tableDetails + "<td>" + ddpRuleDetail.getRdtPartyId() + "</td></tr>";						
//		}
//		mailBody = mailBody.replaceAll("%%DETAILS%%", tableDetails);
//		String mailFrom=env.getProperty("mail.fromAddress");
//		int attachFlag = 0;
//		String commProtocol = missingDocuments.get(0).getRdtCommunicationId().getCmsCommunicationProtocol();
//		
//		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
//			//This should be get from the configuration file or from table
////			String smtpAddres ="10.20.1.233";
//			String smtpAddres = env.getProperty("mail.smtpAddress");
//			logger.info("RuleJob.communication() : calling sendMail() ");
//			attachFlag = taskUtil.sendMail(smtpAddres, toAddress, null, mailFrom, mailSubject, mailBody, null);
//		}
//		
//	}
//	/**
//	 * Method used for sending the mail using the communication set.
//	 * 
//	 * @param listFileMap
//	 * @param ddpDmsDocsDetail
//	 * @param ddpRuleDetail
//	 * @return
//	 */
//	//@AuditLog
//	private int sendMailUsingCommunicationSetup(DdpCommEmail ddpCommEmails,File tmpFile,DdpDmsDocsDetail ddpDmsDocsDetail,DdpRuleDetail ddpRuleDetail,String mailBodyContent,String uniqueNumber,String triggeringDocType) {
//		
//		logger.info("DdpMultiAedRuleJob.sendMailUsingCommunicationSetup() method is inovked");
//		String	mailSubject =  null;	
//		try {
//			mailSubject  = env.getProperty("multiaedmail.subject."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
//		} catch (Exception ex) {
//			
//		}
//		if (mailSubject == null || mailSubject.isEmpty()) {
//			try {
//				mailSubject = env.getProperty("multiaedmail.subject."+ddpDmsDocsDetail.getDddCompanySource());
//			} catch (Exception ex) {
//				
//			}
//		}
//		if(mailSubject == null || mailSubject.isEmpty()) {
//			try {
//				mailSubject = env.getProperty("multiaedmail.subject");
//			} catch (Exception ex) {
//				mailSubject = "Auto Emailing from DDP";
//			}
//		}
//		
//		mailSubject = mailSubject.replaceAll("%%JOBNUMBER%%", uniqueNumber);
//				
//		String mailBody = null;
//		try {
//			mailBody =	env.getProperty("multiaedmail.body."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
//		} catch(Exception ex) {
//			
//		}
//		if (mailBody == null || mailBody.isEmpty()) {
//			try {
//			 mailBody = env.getProperty("multiaedmail.body."+ddpDmsDocsDetail.getDddCompanySource());
//			} catch (Exception ex) {
//				
//			}
//		}
//		if(mailBody == null || mailBody.isEmpty()) {
//			try {
//				mailBody = env.getProperty("multiaedmail.body");
//			} catch (Exception ex){
//				mailBody = "<P><h2><font color='orange'>Attachment Details</font></h2><table><tr style='background-color:rgb(244,160,65);'>"
//						+ "<th>Attachment</th><th>OurReference</th><th>Consignment ID</th><th>Document Reference</th><th>Document Type</th>"
//						+ "</tr>%%DETAILS%%</table><br/><span>please do not respond to this mail, as this account is intended for outbound emails only</span>"
//						+ "<br/><br/></P><IMG SRC='http://www.latc.la/advhtml_upload/agi_gil-home-logo.png' ALT='GIL'>";
//			}
//		}
//		
//		String documentType = new String(triggeringDocType);
//		try { 
//			String docType = env.getProperty("multiaedmail.DOCTYPE."+triggeringDocType);
//			if (docType != null && docType.length() > 0)
//				documentType = docType;
//		} catch (Exception ex) {
//			
//		}
//		mailBody = mailBody.replaceAll("%%JOBNUMBER%%", uniqueNumber);
//		mailBody = mailBody.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
//		mailBody = mailBody.replaceAll("%%DOCUMENTTYE%%", documentType);
//		mailBody = mailBody.replaceAll("%%RULEID%%", ddpRuleDetail.getRdtRuleId().getRulId()+"");
//		mailBody = mailBody.replaceAll("%%DETAILS%%", mailBodyContent);
//		if (mailBody.contains("%%TRACKURL%%")) {
//			mailBody = mailBody.replaceAll("%%TRACKURL%%",env.getProperty("tracking.url."+env.getProperty("mail.evn")));
//		}
//		
//		//In feature need fetch from control for attribute CCMAILID, add in below condition. 
//		//Below condition is written to avoid control db hitting ever time.
//		if (mailBody.contains("%%CONTROLUSERNAME%%") || mailBody.contains("%%MAILID%%")) {
//			
//			String controlUID=ddpDmsDocsDetail.getDddUserId().trim();
//			String controlQuery = env.getProperty("control.query."+env.getProperty("mail.evn"));
//			List<Map<String,Object>> controlResultSet = controlJdbcTemplate.queryForList(controlQuery, new Object[]{controlUID});
//			
//			if (controlResultSet.size() != 0) {
//				for (Map<String, Object> map : controlResultSet) {
//					String userName = (map.get("UDSDES") == null ? "" : (String)map.get("UDSDES")) ;
//					String controlUserMailID =( map.get("UDEADD") == null ? "" : (String)map.get("UDEADD"));
//					mailBody = mailBody.replaceAll("%%CONTROLUSERNAME%%", userName);
//					mailBody = mailBody.replaceAll("%%MAILID%%", controlUserMailID);
//				}
//			} else {
//				mailBody = mailBody.replaceAll("%%CONTROLUSERNAME%%", "");
//				mailBody = mailBody.replaceAll("%%MAILID%%", "");
//			}
//			
//		}
//				
//		String mailFrom=env.getProperty("mail.fromAddress");
//		int attachFlag = 0;
//		String commProtocol = ddpRuleDetail.getRdtCommunicationId().getCmsCommunicationProtocol();
//		
//		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
//			//This should be get from the configuration file or from table
////			String smtpAddres ="10.20.1.233";
//			String smtpAddres = env.getProperty("mail.smtpAddress");
//			logger.info("RuleJob.communication() : calling sendMail() ");
//			attachFlag = taskUtil.sendMailForMultiAed(ddpCommEmails, tmpFile, smtpAddres, mailSubject, mailBody, mailFrom,  ddpRuleDetail.getRdtRuleType());
//		}
//		
//		logger.info("DdpMultiAedRuleJob.sendMailUsingCommunicationSetup() is Successfully compeleted - AttachFlag : "+attachFlag);
//		
//		return attachFlag;
//	}
	
	/**
	 * Method used for getting the Matched Categorized Docs based on the below parameters.
	 * 
	 * @param ruleID
	 * @param ruleDetailID
	 * @return
	 */
	private List<DdpCategorizedDocs> getMatchedCategorizedDocs(Integer ruleID,Integer ruleDetailID,Calendar queryStartDate, Calendar queryEndDate,int typeOfService) {
		
		List<DdpCategorizedDocs> ddpCatList = new ArrayList<DdpCategorizedDocs>();
		String query = null;
		Object[] objects = null;
		
		if (queryStartDate != null || queryEndDate != null) {
			query = Constant.DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS_WITH_DATES;
			objects = new Object[] {ruleID,ruleDetailID,queryStartDate,queryEndDate};
		} else { 
			if (typeOfService == 3) 
				query = Constant.DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS_REPROCESS;
			else 
				query = Constant.DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS;
			objects = new Object[] {ruleID,ruleDetailID};
		}
			
		try {
			logger.info("DdpMultiAedRuleJob.getMatchedCategorizedDocs() - Executing query : "+query );
			 ddpCatList = this.jdbcTemplate.query(query,objects, new RowMapper<DdpCategorizedDocs>() {
                
                public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum) throws SQLException {
                	DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
                     return ddpCategorizedDocs;
                }
    		});       
			
		}
		catch(Exception ex)
		{
			logger.error("DdpAedRuleChecking.getMatchedCategorizedDocs(Integer ruleID) - Exception while accessing DdpRuleDetail retrive based [{}] records for RULE_ID : "+ruleID,ex);
			ex.printStackTrace();
		}
		
		return ddpCatList;
		
	}
	
	
	/**
	 * Method used for getting all the rule details
	 * 
	 * @param ruleID
	 * @return
	 */
	public List<DdpRuleDetail> getAllRuleDetails(Integer ruleID) {
		
		List<DdpRuleDetail> ddpRuleDetails = null;
		try {
			ddpRuleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_MULTI_AED_RULE_DETAIL_ID,new Object[] {ruleID}, new RowMapper<DdpRuleDetail>() {
                
                public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
                	DdpRuleDetail ddpRuleDetail = ddpRuleDetailService.findDdpRuleDetail(rs.getInt("RDT_ID"));
                     return ddpRuleDetail;
                }
    		});                           
		
		}
		catch(Exception ex)
		{
			logger.error("DdpAedRuleChecking.getAllRuleDetails(Integer ruleID) - Exception while accessing DdpRuleDetail retrive based [{}] records for RULE_ID : "+ruleID,ex);
			ex.printStackTrace();
		}
		
		return ddpRuleDetails;
	}
	
	/**
	 * Method used for getting all the rule details
	 * 
	 * @param ruleID
	 * @return
	 */
	private List<DdpRuleDetail> getAllRuleDetails(Integer ruleID, Calendar currentDate) {
		
		List<DdpRuleDetail> ddpRuleDetails = null;
		try {
			ddpRuleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_MULTI_AED_RULE_WITH_DATE_DETAIL_ID,new Object[] {ruleID,currentDate}, new RowMapper<DdpRuleDetail>() {
                
                public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
                	DdpRuleDetail ddpRuleDetail = ddpRuleDetailService.findDdpRuleDetail(rs.getInt("RDT_ID"));
                     return ddpRuleDetail;
                }
    		});                           
		
		}
		catch(Exception ex)
		{
			logger.error("DdpAedRuleChecking.getAllRuleDetails(Integer ruleID) - Exception while accessing DdpRuleDetail retrive based [{}] records for RULE_ID : "+ruleID,ex);
			ex.printStackTrace();
		}
		
		return ddpRuleDetails;
	}
	
	
	/**
	 * Method used for creating the file.
	 * 
	 * @param folderName
	 * @param filename
	 * @param fileLocation
	 * @param objectID
	 * @param session
	 * @return
	 */
	private boolean createFile(String folderName,String filename,String fileLocation,String objectID,IDfSession session) {
		
		boolean isFileCreated = false;
		/*if (checkFileExistsInLocation(filename, fileLocation)) {
			isFileCreated = SchedulerJobUtil.copyFile(new File(fileLocation + "//" + filename), new File(folderName + "//" + filename));
		} else {*/
			isFileCreated =ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(objectID, folderName, filename, session);
		//}
		return isFileCreated;
	}
	
	/**
	 * Method used for checking the file in the existing location or not.
	 * 
	 * @param fileName
	 * @param fileLocation
	 * @return
	 */
	private boolean checkFileExistsInLocation(String fileName,String fileLocation) {
		
		boolean isFileExists = false;
		String fileLoc = fileLocation + "//" + fileName;
		logger.info("DdpMultiAedRuleJob.checkFileExistsInLocation(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : searching for file with name : "+fileLoc);
		File file = new File(fileLoc);
		try {
			isFileExists = file.exists();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return isFileExists;
	}
	
	/**
	 * Method used for getting details of email trigger setup.
	 * 
	 * @param ruleID
	 * @return
	 */
    public DdpEmailTriggerSetup getDdpEmailTriggerSetup(Integer ruleID) {
    	
    	DdpEmailTriggerSetup triggerSetup = null;
    	try {
    	 List<DdpEmailTriggerSetup>	triggerSetups = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_EMAIL_TIGGER_ID, new Object[]{ruleID},  new RowMapper<DdpEmailTriggerSetup>() {

				@Override
				public DdpEmailTriggerSetup mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					
					DdpEmailTriggerSetup ddpEmailTriggerSetup = new DdpEmailTriggerSetup();
					ddpEmailTriggerSetup.setEtrId(rs.getInt("ETR_ID"));
					ddpEmailTriggerSetup.setEtrTriggerName(rs.getString("ETR_TRIGGER_NAME"));
					ddpEmailTriggerSetup.setEtrDocTypes(rs.getString("ETR_DOC_TYPES"));
					ddpEmailTriggerSetup.setEtrDocSelection(rs.getString("ETR_DOC_SELECTION"));
					ddpEmailTriggerSetup.setEtrRetries(rs.getInt("ETR_RETRIES"));
					ddpEmailTriggerSetup.setEtrInclude(rs.getString("ETR_INCLUDE"));
					
					return ddpEmailTriggerSetup;
    			
    		}
    		});
    	 if (triggerSetups != null && triggerSetups.size() > 0)
    		 triggerSetup = triggerSetups.get(0);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	
    	return triggerSetup;
    }
    
    /**
     * Method used for getting the mailBody.
     * 
     * @param aedReport
     * @return
     */
    private String constructMailBody(DdpMultiAedSuccessReport aedReport) {
    	
    	String mailBody = "<tr><td>"+aedReport.getMaedrObjectName()+"</td>";
    	mailBody += "<td>"+aedReport.getMaedrJobNumber()+"</td>";
    	mailBody += "<td>"+aedReport.getMaedrConsignmentId()+"</td>";
    	mailBody += "<td>"+aedReport.getMaedrDocRef()+"</td>";
    	mailBody += "<td>"+aedReport.getMaedrDocType()+"</td></tr>";
    	
    	return mailBody;
    }
      
    
    /**
     * Method used for getting the Categorized docs.
     * 
     * @param jobNumber
     * @param ruleID
     * @return
     */
    public List<DdpCategorizedDocs> getDdpCategorizedDocsBasedOnJobNumber(String jobNumber,Integer ruleID,String documentType) {
        
    	List<DdpCategorizedDocs> categorizedDocs = null;
    	List<DdpCategorizedDocs> categorizedList = new ArrayList<DdpCategorizedDocs>();
    	
    	try {
    		categorizedDocs = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_JOB_NUMBEER_MULTI_AED, new Object[]{jobNumber,documentType,ruleID}, new RowMapper<DdpCategorizedDocs>() {
    			
    			public DdpCategorizedDocs mapRow(ResultSet rs,int rowNum) throws SQLException {
    				
    				return ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
    			}
    		});
    		
    		Map<Integer,SortedMap<Integer,DdpCategorizedDocs>>  collectionMap = new HashMap<Integer, SortedMap<Integer,DdpCategorizedDocs>>();
    		for (DdpCategorizedDocs catDocs : categorizedDocs) {
    			
    			if (!collectionMap.containsKey(catDocs.getCatRdtId())) {
    				SortedMap<Integer, DdpCategorizedDocs> categorizedMap = new TreeMap<Integer, DdpCategorizedDocs>();
    				categorizedMap.put(catDocs.getCatId(), catDocs);
    				collectionMap.put(catDocs.getCatRdtId(), categorizedMap);
    			} else {
    				SortedMap<Integer, DdpCategorizedDocs> categorizedMap = collectionMap.get(catDocs.getCatRdtId());
    				categorizedMap.put(catDocs.getCatId(), catDocs);
    				collectionMap.put(catDocs.getCatRdtId(), categorizedMap);
    			}
    		}
    		
    		for (Integer rdtID : collectionMap.keySet()) {
					SortedMap<Integer, DdpCategorizedDocs> categorizedMap = collectionMap.get(rdtID);
					categorizedList.add(categorizedMap.get(categorizedMap.lastKey()));
    		}
    		
    	} catch (Exception ex) {
    		logger.error("DdpMultiAedRuleJob.getDdpCategorizedDocsBasedOnJobNumber() - Unable to fetch detail of DDPCategorized docs", ex);
    	}
    	return categorizedList;
    }
    
//    /**
//     * Method used for checking the all mandatory document types are available or not.
//     * 
//     * @param categorizedDocs
//     * @param missingDocumentTypes
//     * @param sortedDdpRuleDetailMap
//     * @return
//     */
//    private List<DdpCategorizedDocs> checkAllDocumentsAvailable(List<DdpCategorizedDocs> categorizedDocs,List<DdpRuleDetail> missingDocumentTypes,Map<Integer, DdpRuleDetail> sortedDdpRuleDetailMap) {
//    	
//    	List<DdpCategorizedDocs> list = new LinkedList<DdpCategorizedDocs>();
//    	boolean isDocumentAvailable = true;
//    	Map<Integer,DdpRuleDetail> documentTypeMap = new HashMap<Integer, DdpRuleDetail>();
//    	Map<Integer,List<DdpCategorizedDocs>> documentTypeDocs = new HashMap<Integer, List<DdpCategorizedDocs>>();
//    	
//    	//Adding the forced type in the order.
//    	for (Integer sequnceNum : sortedDdpRuleDetailMap.keySet()) {
//    		DdpRuleDetail ddpRuleDetail = sortedDdpRuleDetailMap.get(sequnceNum);
//    		documentTypeMap.put(ddpRuleDetail.getRdtId(), ddpRuleDetail);
//    	}
//    	
//    	// iterate the Categorized docs put into the document types as one map.
//    	for (DdpCategorizedDocs docs : categorizedDocs) {
//    		
//    		//TODO: This below code works for all version. For the latest version.Need to change the code in the below block.
//    		Integer ruleDetailID = docs.getCatRdtId();
//    		if (documentTypeDocs.containsKey(ruleDetailID)) {
//    			List<DdpCategorizedDocs> categorizedDocList = documentTypeDocs.get(ruleDetailID);
//    			categorizedDocList.add(docs);
//    			documentTypeDocs.put(ruleDetailID, categorizedDocList);
//    		} else {
//    			List<DdpCategorizedDocs> categorizedDocList = new ArrayList<DdpCategorizedDocs>();
//    			categorizedDocList.add(docs);
//    			documentTypeDocs.put(ruleDetailID, categorizedDocList);
//    		}
//    	}
//    	 
//    	// Based on the RULE_DETAIL_ID then get the document type send
//    	// Then check all the documents are able. if missing add into missing documents 
//      	// Then keep in the ordered list.
//    	if (isDocumentAvailable) {
//    		for (Integer sequnceNum : sortedDdpRuleDetailMap.keySet()) {
//        		DdpRuleDetail ddpRuleDetail = sortedDdpRuleDetailMap.get(sequnceNum);
//        		
//        		Integer forcedType = ddpRuleDetail.getRdtForcedType();
//        		if (forcedType != null && forcedType.intValue() == 1) {
//        			List<DdpCategorizedDocs> catList = documentTypeDocs.get(ddpRuleDetail.getRdtId());
//        			if (catList == null || catList.size() == 0) {
//        				missingDocumentTypes.add(ddpRuleDetail);
//        				isDocumentAvailable = false;
//        			}
//        		}
//        		List<DdpCategorizedDocs> catList = documentTypeDocs.get(ddpRuleDetail.getRdtId());
//        		if (catList != null && catList.size() >0) {
//        			list.addAll(catList);
//        		}
//    		}
//    	}
//    	
//    	/*if (!isDocumentAvailable)
//    		list = null;*/
//    	
//    	return list;
//    }
    
    
    /**
	 * Method used for updating the status of execution code.
	 * 
	 * @param ddpCategorizedDcosList
	 * @param executionCode
	 */
	private void updatCategorizedDocumentsRetryCount(List<DdpCategorizedDocs> ddpCategorizedDcosList,String uniqueNumber,Integer retryCount,boolean isStatusChange) {
		
		//updating the status
		for (DdpCategorizedDocs ddpCategorizedDocs : ddpCategorizedDcosList) {
			
			//retry number is will common to all it's backup document if the primary document is present.
			int count = (retryCount == null? 0 :retryCount.intValue());			
			ddpCategorizedDocs.setCatRetries(count+1);
			
			//Status is changed to 3 due to retry count has been finished.
			//if (ddpEmailTriggerSetup.getEtrRetries() != null && ddpCategorizedDocs.getCatRetries() != null && ddpEmailTriggerSetup.getEtrRetries().intValue() ==  ddpCategorizedDocs.getCatRetries().intValue())
			if (isStatusChange)
				ddpCategorizedDocs.setCatStatus(3);
			
			ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDocs);
					 
		}
		logger.info("DdpMultiAedRuleJob.updateCategorizedDocumentsRetryCount(): Updated for the retry count for the job number/Consignment id : "+uniqueNumber);
	}
	
	/**
	 * Method used for checking the retries is matched with EmailTrigger setup.
	 * 
	 * @param retryNumber
	 * @param ddpEmailTriggerSetup
	 * @return
	 */
	private boolean isStatusChangeRequiredToCat(Integer retryNumber,DdpEmailTriggerSetup ddpEmailTriggerSetup) {
		
		boolean isStatusChangeRequired = false;
		
		if (retryNumber != null && ddpEmailTriggerSetup.getEtrRetries() != null) {
			int retry =  retryNumber.intValue() + 1;
			if (retry == ddpEmailTriggerSetup.getEtrRetries().intValue() || retry >= ddpEmailTriggerSetup.getEtrRetries().intValue())
				isStatusChangeRequired = true;
		} else {
			if (ddpEmailTriggerSetup.getEtrRetries() == null || ddpEmailTriggerSetup.getEtrRetries().intValue() == 0) {
				isStatusChangeRequired = true;
			} else if (retryNumber == null &&  ddpEmailTriggerSetup.getEtrRetries() != null) {
				int retry =  1;
				if (retry == ddpEmailTriggerSetup.getEtrRetries().intValue() || retry >= ddpEmailTriggerSetup.getEtrRetries().intValue())
					isStatusChangeRequired = true;
			}
		}
		
		return isStatusChangeRequired;
	}
	
	/**
	 * Method used for running the reprocess command.
	 */
	public void reprocessJob() {
		
		List<DdpEmailTriggerSetup> list = getListOfDdpEmailTriggerSetupForReprocess();
		
		if (list != null && list.size() > 0) {
			for (DdpEmailTriggerSetup setup : list) {
				
				List<DdpRuleDetail> ddpRuleDetails = getAllRuleDetails(setup.getEtrRuleId().getRulId(),GregorianCalendar.getInstance());
				
				if (ddpRuleDetails == null || ddpRuleDetails.size() == 0) {
					logger.info("DdpMultiAedRuleJob.reporcesJob() - Empty rule detail for the rule id : "+setup.getEtrRuleId().getRulId());
					continue;
				}
				
				 if (ddpRuleDetails.get(0).getRdtStatus() != 0) {
					 
					 logger.info("DdpMultiAedRuleJob.exuecteConsolidatedJob() for rule id : "+ddpRuleDetails.get(0).getRdtRuleId().getRulId()+" : is inactive ");
					 continue;
				 }
				
				 try {
			    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
			    	 Scheduler scheduler = applicationProperties.getQuartzScheduler("MULTI_AED_REPROCESS : "+setup.getEtrRuleId().getRulId()+"_"+(new Date()));
			    	 jdfb.setTargetObject(this);
				     jdfb.setTargetMethod("executeJob");
				     jdfb.setArguments(new Object[]{ddpRuleDetails,setup,null,null,2,scheduler});
				     jdfb.setName("MULTI_AED_REPROCESS : "+setup.getEtrRuleId().getRulId()+"_"+(new Date()));
				     jdfb.afterPropertiesSet();
				     JobDetail jd = (JobDetail)jdfb.getObject();
				     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
				     trigger.setName("MULTI_AED_REPROCESS : "+setup.getEtrRuleId().getRulId()+"_"+(new Date()));
				     trigger.setGroup("MULTI_AED_REPROCESS-Group- : "+setup.getEtrRuleId().getRulId()+"_"+(new Date()));
				     trigger.setStartTime(new Date());
				     trigger.setEndTime(null);
				     trigger.setRepeatCount(0);
				     trigger.setRepeatInterval(4000L);
				     
				     scheduler.scheduleJob(jd, trigger);
				     try
				     {
				    	 scheduler.start();
				    	 Thread.sleep(600L);
				     }catch(Exception ex){
				    	 logger.error("DdpMultiAedRuleJob.reprocess() Error occurried when starting reprocess for rule ID {}", setup.getEtrRuleId().getRulId());
				     }
				     finally {
				    	 //scheduler.shutdown();
					}
		       	} catch (Exception ex) {
		        		logger.error("DdpMultiAedRuleJob.reprocess() Error occurried while running the reprocess service.", ex);
		        }
			}
		}
	}
	
	public boolean executeJob(List<DdpRuleDetail> ddpRuleDetails,DdpEmailTriggerSetup ddpEmailTriggerSetup,
			Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Scheduler scheduler,DdpCategorizedDocs ddpCategorizedDoc) {
		
		logger.info("DdpMultiAedRuleJob.executeJob(categorizedDocs) Method Invoked. with type of service : "+(typeOfService == 1?"General Service":"Reprocess Service"));
		boolean isMailTiggered = false;	
		 isMailTiggered = executeConsolidatedJob(ddpRuleDetails, ddpEmailTriggerSetup, queryStartDate, queryEndDate, typeOfService,ddpCategorizedDoc);
		logger.info("DdpMultiAedRuleJob.executeJob(...,Scheduler scheduler) Successfully completed & status of mail Tiggering : "+isMailTiggered+" : Type of Service : "+(typeOfService == 1?"General Service":"Reprocess Service"));
		try {
			if (scheduler != null)
				scheduler.shutdown();
		} catch (Exception ex) {
			logger.error("DdpMultiAedRuleJob.executeJob(...,Scheduler scheduler) - unable to shutdown the scheduler thread", ex);
		}
		return isMailTiggered;
	}
	
	private List<DdpEmailTriggerSetup> getListOfDdpEmailTriggerSetupForReprocess() {
		
		List<DdpEmailTriggerSetup> ddpEmailTriggerSetups = null;
		
		try {
    		ddpEmailTriggerSetups = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_REPROCESS_MULTI_AED, new Object[]{}, new RowMapper<DdpEmailTriggerSetup>() {
    			
    			public DdpEmailTriggerSetup mapRow(ResultSet rs,int rowNum) throws SQLException {
    				
    				return ddpEmailTriggerSetupService.findDdpEmailTriggerSetup(rs.getInt("ETR_ID"));
    			}
    		});
    		
    	} catch (Exception ex) {
    		logger.error("DdpMultiAedRuleJob.getListOfDdpEmailTriggerSetupForReprocess() - Unable to fetch detail of Email Trigger setup details.", ex);
    	}
		return ddpEmailTriggerSetups;
	}
	
	/**
	 * Method used for getting the filename based on existing copy in the location.
	 * 
	 * @param isZipComp
	 * @param fileName
	 * @param sourceZipFilePath
	 * @param sourceFilePath
	 * @return
	 */
	private String getFileName(boolean isZipComp,String fileName,String sourceZipFilePath,String sourceFilePath) {
		
		if (isZipComp && checkFileExistsInLocation(fileName,sourceZipFilePath)) {
			List <String> fileNames = commonUtil.readListOfFileInLocal(sourceZipFilePath);
			fileName = commonUtil.getFileNameWithCopy(fileName, fileNames, null, null);
		} else if(!isZipComp && checkFileExistsInLocation(fileName,sourceFilePath)) {
			List <String> fileNames = commonUtil.readListOfFileInLocal(sourceFilePath);
			fileName = commonUtil.getFileNameWithCopy(fileName, fileNames, null, null);
		}
		
		if (fileName != null) {
			fileName=fileName.replaceAll("[^a-zA-Z0-9_.-]", ""); 
		}

		return fileName;
		
	}
	
//	/**
//	 * Method used for fetching the all versions of r_object_id.
//	 * 
//	 * @param rObjectID
//	 * @param emailBody
//	 * @param session
//	 * @return
//	 */
//	private List<String> fetchAllVersionObjectIDsFromDMS(String rObjectID,StringBuffer emailBody,IDfSession session) {
//		
//		logger.info("DdpMultiAedRuleJob -fetchAllVersionFromDMS() is invoked for object id : "+rObjectID);
//		DdpDFCClient client =  new DdpDFCClient();
//		Hashtable<String, String> dmsMetadata = client.begin(session,rObjectID);
//		List<String> list = new ArrayList<String>();
//		
//		logger.debug("DdpBacDocumentProcess.downloadAllVersion() : Chornicle id : "+dmsMetadata.get("i_chronicle_id"));
//   	 	try {
//   	 		String query = "select object_name, r_object_id,  r_version_label,r_content_size,agl_job_numbers, agl_consignment_id, agl_doc_ref, agl_control_doc_type,agl_doc_ref from agl_control_document (all) where i_chronicle_id= '"+dmsMetadata.get("i_chronicle_id")+"'";
//
//   	 	  IDfQuery dfQuery = new DfQuery();
//			dfQuery.setDQL(query);
//			IDfCollection dfCollection = dfQuery.execute(session,
//					IDfQuery.DF_EXEC_QUERY);
//			
//			while (dfCollection.next()) {
//				list.add(dfCollection.getString("r_object_id"));
//				emailBody.append(constructMailBody(dfCollection.getString("agl_job_numbers"), dfCollection.getString("agl_consignment_id"), 
//						dfCollection.getString("agl_doc_ref"), dfCollection.getString("agl_control_doc_type"), dfCollection.getString("object_name")));
//			}
//   	 	} catch (Exception ex) {
//   	 		logger.error("DdpMultiAedRuleJob.fetchAllVersionFromDMS() - Unable to fetch all version details for robject id  : "+rObjectID , ex);
//   	 	}
//   	 	return list;
//	}
	
//	/**
//	 * Method used downloading the all version document into location.
//	 * 
//	 * @param robjectID
//	 * @param emailBody
//	 * @param session
//	 * @param isZipComp
//	 * @param sourceZipFilePath
//	 * @param sourceFilePath
//	 * @return
//	 */
//	private boolean downloadAllVersionDocumentsFromDMS(String robjectID,StringBuffer emailBody,IDfSession session, 
//				boolean isZipComp,String sourceZipFilePath,String sourceFilePath) {
//		
//		boolean isDownload = false;
//		
//		DdpDFCClient client =  new DdpDFCClient();
//		Hashtable<String, String> dmsMetadata = client.begin(session,robjectID);
//				
//		logger.debug("DdpBacDocumentProcess.downloadAllVersion() : Chornicle id : "+dmsMetadata.get("i_chronicle_id"));
//   	 	try {
//   	 		String query = "select object_name, r_object_id,  r_version_label,r_content_size,agl_job_numbers, agl_consignment_id, agl_doc_ref, agl_control_doc_type,agl_doc_ref from agl_control_document (all) where i_chronicle_id= '"+dmsMetadata.get("i_chronicle_id")+"'";
//
//   	 	  IDfQuery dfQuery = new DfQuery();
//			dfQuery.setDQL(query);
//			IDfCollection dfCollection = dfQuery.execute(session,
//					IDfQuery.DF_EXEC_QUERY);
//			
//			while (dfCollection.next()) {
//				
//				String fileName = getFileName(isZipComp, dfCollection.getString("object_name"), sourceZipFilePath, sourceFilePath);
//				if (isZipComp)
//					isDownload = createFile(sourceZipFilePath, fileName, null, dfCollection.getString("r_object_id"), session);
//				else
//					isDownload = createFile(sourceFilePath, fileName, null,dfCollection.getString("r_object_id"), session);
//				
//				if (!isDownload)
//					return isDownload;
//				
//				emailBody.append(constructMailBody(dfCollection.getString("agl_job_numbers"), dfCollection.getString("agl_consignment_id"), 
//						dfCollection.getString("agl_doc_ref"), dfCollection.getString("agl_control_doc_type"), fileName));
//			}
//   	 	} catch (Exception ex) {
//   	 		logger.error("DdpMultiAedRuleJob.downloadAllVersionDocumentsFromDMS() - Unable to fetch all version details for robject id  : "+robjectID , ex);
//   	 	}
//		
//		return isDownload;
//		
//	}
	
	
	/**
	 * 
	 * @param ruleDetailMap
	 * @return
	 */
	private String getVersionSetupDetailsBasedRdtID(Integer rdtID) {
		
		String version = "latest";
	
		try {		

				List<String> optionNames = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MACTHED_VERSION_SETUP_ID, new Object[] {rdtID},  new RowMapper<String>() {
					
					public String mapRow(ResultSet rs, int rowNum) throws SQLException {
						
						
						return rs.getString("EVS_OPTION");
			           }
				});		
				
				if (optionNames.size() > 0)
					version = optionNames.get(0);
		
		} catch (Exception ex) {
			logger.error("DdpMultAEdRuleJob.getVersionSetupDetailsBasedRdtID() - Unable to idetify the version number : "+rdtID, ex);
		}
		return version;
	}
	
	
	/**
	 * Method used for executing the job.
	 * 
	 * @param ddpCategorizedDocs
	 * @return
	 */
	public boolean executeConsolidatedJob(List<DdpRuleDetail> ddpRuleDetails,DdpEmailTriggerSetup ddpEmailTriggerSetup,
			Calendar queryStartDate,Calendar queryEndDate,int typeOfService,DdpCategorizedDocs ddpCategorizedDoc) {
		
		
		logger.info("DdpMultiAedRuleJob.executeConsolidatedJob(categorizedDocs) Method Invoked. with type of service : "+(typeOfService == 1?"General Service":"Reprocess Service"));
		boolean isMailTiggered = false;	
		DdpCommEmail ddpCommEmails = null;
		//For fetching the unique branches
		Map<String, List<String>> uniqueBraches = new HashMap<String, List<String>>();
		Map<String, List<DdpRuleDetail>> uniqueDocType = new HashMap<String, List<DdpRuleDetail>>();
		Map<String,DdpCategorizedDocs> triggeringDocMap = new HashMap<String,DdpCategorizedDocs>();
		List<DdpCategorizedDocs> existingList = new ArrayList<DdpCategorizedDocs>();
		
		String commProtocol = ddpRuleDetails.get(0).getRdtCommunicationId().getCmsCommunicationProtocol();
	    
		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
			ddpCommEmails = ddpCommEmailService.findDdpCommEmail(Integer.parseInt(ddpRuleDetails.get(0).getRdtCommunicationId().getCmsProtocolSettingsId()));
		}
		
		if (ddpRuleDetails.get(0).getRdtStatus() != 0) {
			 
			 logger.info("DdpMultiAedRuleJob.exuecteConsolidatedJob() for rule id : "+ddpRuleDetails.get(0).getRdtRuleId().getRulId()+" : is inactive ");
			 return false;
		 }
		
		
		//To avoid time creation of session for each iteration of loop
		IDfSession session = ddpDFCClientComponent.beginSession();
			    
		if (session == null) {
			logger.info("DdpMultiAedRuleJob.executeConsolidatedJob() - Unable create the DFCClient Session for this Consolidated AED rule ID.",ddpRuleDetails.get(0).getRdtRuleId().getRulId());
			taskUtil.sendMailByDevelopers("Please restart the server. Unable to get IDFSession Consolidated AED rule ID : " +ddpRuleDetails.get(0).getRdtRuleId().getRulId(), "DdpMultiAedRuleJob.executeConsolidatedJob() unable to IDFSession.");
			return isMailTiggered;
		}
		 
		File checkDupFile = null;
		
		try {
			//Based on Document type adding setting the set of branches
			commonUtil.filterDocTypesBasedOnRuleDetails(ddpRuleDetails, uniqueBraches, uniqueDocType);
			findTriggeringDocumentType(ddpRuleDetails, triggeringDocMap, existingList, queryStartDate, queryEndDate, typeOfService,ddpEmailTriggerSetup);
			
			
			//checking rule is running already.
			for (String triggeringKey : triggeringDocMap.keySet()) {
				String tempFilePath = env.getProperty("ddp.export.folder");
				String uniqueRefNumber = (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("consignmentID")  ? triggeringKey : triggeringKey.substring(0, triggeringKey.indexOf("-")));
				synchronized (this) {
					String checkDuplicatePath = tempFilePath + "/" + MULTI_AED + ddpRuleDetails.get(0).getRdtRuleId().getRulId()+"-"+uniqueRefNumber.replaceAll("[^a-zA-Z0-9_.]", "");
					File lcheckDupFile = new File(checkDuplicatePath);
						
					if (lcheckDupFile.exists()) {
						logger.info("DdpMultiAedRuleJob.performMailTriggerProcess() - Duplicate rule record is executing at same time for Rule ID : "+ddpRuleDetails.get(0).getRdtRuleId().getRulId());
						return isMailTiggered;
					}
					checkDupFile = FileUtils.createFolder(checkDuplicatePath);
					break;
				}
			}
			
			//Duplicate document changing status to 4
			if (typeOfService == 1 || typeOfService == 3) {
				for (DdpCategorizedDocs doc : existingList) {
					doc.setCatStatus(4);
					ddpCategorizedDocsService.updateDdpCategorizedDocs(doc);
				}
			}
			
			for (String triggeringKey : triggeringDocMap.keySet()) {
				
//				String uniqueRefNumber = (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("consignmentID")  ? triggeringKey : triggeringKey.substring(0, triggeringKey.indexOf("-")));
				String uniqueRefNumber =  triggeringKey.substring(0, triggeringKey.indexOf("-"));
				
				DdpCategorizedDocs triggeringCatDocs = triggeringDocMap.get(triggeringKey);
				List<DdpMultiAedSuccessReport> aedBackUpList = new ArrayList<DdpMultiAedSuccessReport>();
				List<DdpMultiAedSuccessReport> triggringList = new ArrayList<DdpMultiAedSuccessReport>();
				Map<String,List<DdpRuleDetail>> missingMandatoryList = new HashMap<String, List<DdpRuleDetail>>();
				Map<String,List<String>> missingDocsMap = new HashMap<String,List<String>>();
				try {
					for (String documentType : uniqueDocType.keySet()) {
			    		
			    		List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(documentType);
			    		List<String> branchSet = uniqueBraches.get(documentType);
						
						if (ruleDetailSet.get(0).getRdtDocSequence() == null) {
							logger.info("DdpMultiAedRule.executeJob() Rule detail sequence is null for rule detail id : "+ruleDetailSet.get(0).getRdtId());
							return isMailTiggered ;
						}
						
						//forcedTypeMap.put(ddpRuleDetail.getRdtDocType().getDtyDocTypeCode(), ddpRuleDetail.getRdtForcedType());
						//Based on the Relevant Type is 1 then fetch all the documents. i.e., on the Primary documents are fetched.
						if (ruleDetailSet.get(0).getRdtRelavantType() != null && ruleDetailSet.get(0).getRdtRelavantType() == 1) {
							continue;
						} else {
							if (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("consignmentID")) {
								performConsolidationProcessForCosignmentID(uniqueRefNumber, ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, typeOfService, missingMandatoryList, aedBackUpList, session, ddpEmailTriggerSetup,triggeringCatDocs);
							} else if (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("multipleJobs")) {
								performConsolidationProcessForMultiJobNumbers(triggeringCatDocs, ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, typeOfService, missingMandatoryList, aedBackUpList, session,missingDocsMap);
							} else {
								//Single job number check.
								performConsolidationProcessForSingleJobNumber(uniqueRefNumber, ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, typeOfService, missingMandatoryList, aedBackUpList, session);
		
							}
						}
					}
					
					List<DdpCategorizedDocs> catList = new ArrayList<DdpCategorizedDocs>();
					catList.add(triggeringCatDocs);
					
					//If the mandatory documents are missing then sending the mail to configured users
					if (missingMandatoryList.size() > 0) {
						if (typeOfService == 1 || typeOfService == 3) {
							
							if (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("multipleJobs")) {	
								if (typeOfService == 1 && ddpCategorizedDoc != null ) {
									//Trigger missing document only for current CAT_ID document.
									if ( triggeringCatDocs.getCatId().intValue() ==  ddpCategorizedDoc.getCatId().intValue())									
										sendMailFailureMultipleJobNumbersAEDRules(ddpCommEmails, missingDocsMap, 1, ddpEmailTriggerSetup, uniqueRefNumber, triggeringCatDocs, ddpRuleDetails.get(0));
								} else {
									sendMailFailureMultipleJobNumbersAEDRules(ddpCommEmails, missingDocsMap, 1, ddpEmailTriggerSetup, uniqueRefNumber, triggeringCatDocs, ddpRuleDetails.get(0));
								}
							} else {
								//Cosignment ID & Job Number.
								if (typeOfService == 1 && ddpCategorizedDoc != null ) {
									//Trigger missing document only for current CAT_ID document.
									if ( triggeringCatDocs.getCatId().intValue() ==  ddpCategorizedDoc.getCatId().intValue())
										sendMailFailureMultiAEDRules(ddpCommEmails, missingMandatoryList, 1, ddpEmailTriggerSetup, uniqueRefNumber, triggeringCatDocs);
								} else {
									sendMailFailureMultiAEDRules(ddpCommEmails, missingMandatoryList, 1, ddpEmailTriggerSetup, uniqueRefNumber, triggeringCatDocs);
								}
							}
						} else if (typeOfService == 2 && (ddpEmailTriggerSetup.getEtrRetries() != null  && ddpEmailTriggerSetup.getEtrRetries().intValue() != 0)) {
							//if the Trigger setup retries is null or 0 then not sending any mail. Until the documents comes to DMS it waits. 
							boolean isStatusChangeRequired = isStatusChangeRequiredToCat(triggeringCatDocs.getCatRetries(), ddpEmailTriggerSetup);
							updatCategorizedDocumentsRetryCount(catList,uniqueRefNumber,triggeringCatDocs.getCatRetries(),isStatusChangeRequired);
							if (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("multipleJobs")) {
								sendMailFailureMultipleJobNumbersAEDRules(ddpCommEmails, missingDocsMap, typeOfService, ddpEmailTriggerSetup, uniqueRefNumber, triggeringCatDocs, ddpRuleDetails.get(0));
							} else {
								sendMailFailureMultiAEDRules(ddpCommEmails, missingMandatoryList,typeOfService, ddpEmailTriggerSetup, uniqueRefNumber, triggeringCatDocs);
							}
						}
					} else {
						
						if (ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase("consignmentID")) {
							findTriggeringDocumentsBasedConsignmentID(uniqueRefNumber, triggeringCatDocs, queryStartDate, queryEndDate, typeOfService, session, triggringList, ddpEmailTriggerSetup, uniqueDocType, uniqueBraches);
						} else {
							constructTriggeringDocument(triggeringCatDocs, session, typeOfService, triggringList);
						}
						performMailTriggerProcess(triggringList, aedBackUpList, catList, ddpRuleDetails, uniqueRefNumber, typeOfService, ddpEmailTriggerSetup, ddpCommEmails, session);
					}
				} catch (Exception ex) {
					logger.error("DdpMultiAedRuleJob.executeConsolidatedJob() - Unable file details for Job number "+triggeringKey,ex);
				}
			
			}
		} catch (Exception ex) {
			logger.error("DdpMultiAedRuleJob.executeConsolidatedJob() - Unable to execute the Consolidated AED process : "+ddpRuleDetails.get(0).getRdtRuleId().getRulId(), ex);
		} finally {
			
			if (checkDupFile != null)
			SchedulerJobUtil.deleteFolder(checkDupFile);
			if (session != null) 
				ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
		}
		return isMailTiggered;
	}
	
	/**
	 * Grouping the triggering documents.
	 * 
	 * @param triggeringCatDocs
	 * @param session
	 * @param typeOfService
	 * @param triggringList
	 */
	private void constructTriggeringDocument(DdpCategorizedDocs triggeringCatDocs,IDfSession session,int typeOfService,List<DdpMultiAedSuccessReport> triggringList) {
		
		String version = getVersionSetupDetailsBasedRdtID(triggeringCatDocs.getCatRdtId());
		if (version.equalsIgnoreCase("All")) {
			triggringList.addAll(constructAllVersionConsolidatedAED(triggeringCatDocs, session, typeOfService));
		} else {
			triggringList.add(constructLatestConsolidatedAED(triggeringCatDocs, typeOfService));
		}
	}
	
	/**
	 * Method used for finding the triggering documents
	 * 
	 * @param consignmentID
	 * @param catDocs
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param session
	 * @param triggringList
	 * @param triggerSetup
	 * @param uniqueDocType
	 * @param uniqueBraches
	 */
	private void findTriggeringDocumentsBasedConsignmentID (String consignmentID,DdpCategorizedDocs catDocs,Calendar queryStartDate,Calendar queryEndDate,
			int typeOfService,IDfSession session,List<DdpMultiAedSuccessReport> triggringList,DdpEmailTriggerSetup triggerSetup,
			Map<String, List<DdpRuleDetail>> uniqueDocType,Map<String, List<String>> uniqueBraches ) {
		
		 List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(catDocs.getCatDtxId().getDtxId());
		 String documentType = ddpDmsDocsDetails.get(0).getDddControlDocType();
		// List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(documentType);
 		//List<String> branchSet = uniqueBraches.get(documentType);
 		try {
 			//As per Kerrin requirement, triggering document should only from origin only.
			if (triggerSetup.getEtrInclude().equalsIgnoreCase("all")) {
				
//				String dqlQuery = constructDQLQueryForConsignmentIDALL(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID);
//				IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
//				if (iDfCollection != null) {
//					triggringList.addAll(constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService));
//					iDfCollection.close();
//				}
				constructTriggeringDocument(catDocs, session, typeOfService, triggringList);
			} else if (triggerSetup.getEtrInclude().equalsIgnoreCase("origin")) {
				constructTriggeringDocument(catDocs, session, typeOfService, triggringList);
			} else {
				constructTriggeringDocument(catDocs, session, typeOfService, triggringList);
//				String dqlQuery = constructDQLQueryForConsignmentIDDestination(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID,ddpDmsDocsDetails.get(0).getDddJobNumber());
//				IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
//				if (iDfCollection != null) {
//					triggringList.addAll(constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService));
//					iDfCollection.close();
//				}
			}
 		} catch (Exception ex) {
 			logger.error("DdpMultiAedRuleJob.findTriggeringDocumentsBasedConsignmentID() - Unable to find the document for cat id : "+catDocs.getCatDtxId().getDtxId(), ex);
 		}
	}
	
	/**
	 * Method used for perform consolidation process for consignment id.
	 * 
	 * @param consignmentID
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param missingMandatoryList
	 * @param aedBackUpList
	 * @param session
	 * @param triggerSetup
	 * @param catDocs
	 */
	private void performConsolidationProcessForCosignmentID(String consignmentID,List<DdpRuleDetail> ruleDetailSet, List<String> branchSet,
			String documentType ,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Map<String,List<DdpRuleDetail>> missingMandatoryList,
			List<DdpMultiAedSuccessReport> aedBackUpList,IDfSession session,DdpEmailTriggerSetup triggerSetup,DdpCategorizedDocs catDocs) {
		
		if (triggerSetup.getEtrInclude() != null) {
			if (triggerSetup.getEtrInclude().equalsIgnoreCase("all")) {
				perfromConsolidationForConsignmentIDAll(consignmentID, ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, typeOfService, missingMandatoryList, aedBackUpList, session,catDocs);
			}else if (triggerSetup.getEtrInclude().equalsIgnoreCase("origin")) {
				perfromConsolidationForConsignmentIDOrigin(consignmentID, ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, typeOfService, missingMandatoryList, aedBackUpList, session);
			} else {
				List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(catDocs.getCatDtxId().getDtxId());
				perfromConsolidationForConsignmentIDDestination(consignmentID, ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, typeOfService, missingMandatoryList, aedBackUpList, session, ddpDmsDocsDetails.get(0).getDddJobNumber());
			}
			
		} else {
			logger.info("DdpMultiAedRuleJob.performConsolidationProcessForCosignmentID() - In trigger setup ETR inculde is null for ETR_ID : "+triggerSetup.getEtrId());
			if ( ruleDetailSet.get(0).getRdtForcedType() == 1)
				missingMandatoryList.put(documentType,ruleDetailSet);
		}
		
	}
	
	/**
	 * Method used for perform the consolidationg for consignment id for origin.
	 * 
	 * @param consignmentID
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param missingMandatoryList
	 * @param aedBackUpList
	 * @param session
	 */
	private void perfromConsolidationForConsignmentIDOrigin(String consignmentID,List<DdpRuleDetail> ruleDetailSet, List<String> branchSet,
			String documentType ,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Map<String,List<DdpRuleDetail>> missingMandatoryList,
			List<DdpMultiAedSuccessReport> aedBackUpList,IDfSession session) {
		
			try {
				String dqlQuery = constructDQLQueryForConsignmentIDOrigin(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID);
				IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
				List<DdpMultiAedSuccessReport> consolidatedAEDList = null;
				if (iDfCollection != null) {
					consolidatedAEDList = constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService);
					iDfCollection.close();
				}
					//only mandatory document adding into list.
				if ((consolidatedAEDList == null || consolidatedAEDList.size() == 0) && ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
				else if (consolidatedAEDList != null && consolidatedAEDList.size() > 0)
					aedBackUpList.addAll(consolidatedAEDList);
			
			} catch (Exception ex) {
				logger.error("DdpMultiAedRuleJob.performConsolidationProcessForSingleJobNumber() - Unable to find the docuemnts for Consignment ID : "+consignmentID+" : document type : "+documentType+" : rule id : "+ruleDetailSet.get(0).getRdtRuleId().getRulId(), ex);
				if ( ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
			}
	}
	
	/**
	 * Method used for performing consolidated for consignment id for destination.
	 * 
	 * @param consignmentID
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param missingMandatoryList
	 * @param aedBackUpList
	 * @param session
	 * @param jobNumber
	 */
	private void perfromConsolidationForConsignmentIDDestination(String consignmentID,List<DdpRuleDetail> ruleDetailSet, List<String> branchSet,
			String documentType ,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Map<String,List<DdpRuleDetail>> missingMandatoryList,
			List<DdpMultiAedSuccessReport> aedBackUpList,IDfSession session,String jobNumber) {
		
			try {
				String dqlQuery = constructDQLQueryForConsignmentIDDestination(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID, jobNumber);
				IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
				List<DdpMultiAedSuccessReport> consolidatedAEDList = null;
				if (iDfCollection != null) {
					consolidatedAEDList = constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService);
					iDfCollection.close();
				}
					//only mandatory document adding into list.
				if ((consolidatedAEDList == null || consolidatedAEDList.size() == 0) && ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
				else if (consolidatedAEDList != null && consolidatedAEDList.size() > 0)
					aedBackUpList.addAll(consolidatedAEDList);
			
			} catch (Exception ex) {
				logger.error("DdpMultiAedRuleJob.performConsolidationProcessForSingleJobNumber() - Unable to find the docuemnts for Consignment ID : "+consignmentID+" : document type : "+documentType+" : rule id : "+ruleDetailSet.get(0).getRdtRuleId().getRulId(), ex);
				if ( ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
			}
	}
	
	/**
	 * Method used for perform consolidation for Consignment id as include all option.
	 * 
	 * 
	 * @param consignmentID
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param missingMandatoryList
	 * @param aedBackUpList
	 * @param session
	 */
	private void perfromConsolidationForConsignmentIDAll(String consignmentID,List<DdpRuleDetail> ruleDetailSet, List<String> branchSet,
			String documentType ,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Map<String,List<DdpRuleDetail>> missingMandatoryList,
			List<DdpMultiAedSuccessReport> aedBackUpList,IDfSession session,DdpCategorizedDocs catDocs) {
		
			try {
				String dqlQuery = null;
				//Exclude the origin mean we need to include the destination.
				if (ruleDetailSet.get(0).getRdtInclude() != null && ruleDetailSet.get(0).getRdtInclude().equalsIgnoreCase("origin")) {
					List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(catDocs.getCatDtxId().getDtxId());
					dqlQuery = constructDQLQueryForConsignmentIDDestination(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID, ddpDmsDocsDetails.get(0).getDddJobNumber());
				} else if (ruleDetailSet.get(0).getRdtInclude() != null && ruleDetailSet.get(0).getRdtInclude().equalsIgnoreCase("destination")) {
					//Include only the origin document exclude the destination documents.
					dqlQuery = constructDQLQueryForConsignmentIDOrigin(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID);
				} else {
					dqlQuery =	constructDQLQueryForConsignmentIDALL(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, consignmentID);
				}
				IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
				List<DdpMultiAedSuccessReport> consolidatedAEDList = null;
				if (iDfCollection != null) {
					consolidatedAEDList = constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService);
					iDfCollection.close();
				}
					//only mandatory document adding into list.
				if ((consolidatedAEDList == null || consolidatedAEDList.size() == 0) && ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
				else if (consolidatedAEDList != null && consolidatedAEDList.size() > 0)
					aedBackUpList.addAll(consolidatedAEDList);
			
			} catch (Exception ex) {
				logger.error("DdpMultiAedRuleJob.performConsolidationProcessForSingleJobNumber() - Unable to find the docuemnts for Consignment ID : "+consignmentID+" : document type : "+documentType+" : rule id : "+ruleDetailSet.get(0).getRdtRuleId().getRulId(), ex);
				if ( ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
			}
	}
	
	/**
	 * Method used for perform consolidation process based on single job number.
	 * 
	 * @param uniqueRefNumber
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param missingMandatoryList
	 * @param aedBackUpList
	 * @param session
	 */
	private void performConsolidationProcessForSingleJobNumber(String uniqueRefNumber,List<DdpRuleDetail> ruleDetailSet, List<String> branchSet,
			String documentType ,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Map<String,List<DdpRuleDetail>> missingMandatoryList,
			List<DdpMultiAedSuccessReport> aedBackUpList,IDfSession session) {
		
		try {
			String dqlQuery = constructDQLQueryForConsolidatedAED(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, uniqueRefNumber);
			IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
			List<DdpMultiAedSuccessReport> consolidatedAEDList = null;
			if (iDfCollection != null) {
				consolidatedAEDList = constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService);
				iDfCollection.close();
			}
				//only mandatory document adding into list.
			if ((consolidatedAEDList == null || consolidatedAEDList.size() == 0) && ruleDetailSet.get(0).getRdtForcedType() == 1)
				missingMandatoryList.put(documentType,ruleDetailSet);
			else if (consolidatedAEDList != null && consolidatedAEDList.size() > 0)
				aedBackUpList.addAll(consolidatedAEDList);
			
		} catch (Exception ex) {
			logger.error("DdpMultiAedRuleJob.performConsolidationProcessForSingleJobNumber() - Unable to find the docuemnts for job number : "+uniqueRefNumber+" : document type : "+documentType+" : rule id : "+ruleDetailSet.get(0).getRdtRuleId().getRulId(), ex);
			if ( ruleDetailSet.get(0).getRdtForcedType() == 1)
				missingMandatoryList.put(documentType,ruleDetailSet);
		}
	}
		
	/**
	 * Method used to perform Consolidation process for multiple Job numbers.
	 * 
	 * @param triggeringCatDocs
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 * @param missingMandatoryList
	 * @param aedBackUpList
	 * @param session
	 */
	private void performConsolidationProcessForMultiJobNumbers(DdpCategorizedDocs triggeringCatDocs,List<DdpRuleDetail> ruleDetailSet, List<String> branchSet,
			String documentType ,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Map<String,List<DdpRuleDetail>> missingMandatoryList,
			List<DdpMultiAedSuccessReport> aedBackUpList,IDfSession session,Map<String,List<String>> missingDocsMap) {
		
		logger.info("DdpMultiAedRuleJob.performConsolidationProcessForMultiJobNumbers() - Invoked for cat id : "+triggeringCatDocs.getCatId());
		List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(triggeringCatDocs.getCatDtxId().getDtxId());
		
		if (ddpDmsDocsDetails != null && ddpDmsDocsDetails.size() > 0) {
			
			List<String> jobNumbersList = new ArrayList<String>();
			findMultipleJobNumbersUsingRObjectID(jobNumbersList, ddpDmsDocsDetails.get(0).getDddRObjectId(), session);
			
			if (jobNumbersList.size() > 0) {
				
				try {
					//Building the DQL Query.
					String dqlQuery = constructDQLQueryForMultipleJobNumbers(ruleDetailSet, branchSet, documentType, queryStartDate, queryEndDate, jobNumbersList);
					IDfCollection iDfCollection = getIDFCollectionDetails(dqlQuery, session);
					List<DdpMultiAedSuccessReport> consolidatedAEDList = null;
					
					if (iDfCollection != null) {
						consolidatedAEDList = constructConsolidatedAEDObject(iDfCollection, ruleDetailSet.get(0).getRdtRuleId().getRulId(), null, typeOfService);
						iDfCollection.close();
	    			}
					
					if ((consolidatedAEDList == null || consolidatedAEDList.size() == 0) && ruleDetailSet.get(0).getRdtForcedType() == 1) {
	    				missingMandatoryList.put(documentType,ruleDetailSet);
					} else if (consolidatedAEDList != null && consolidatedAEDList.size() > 0) {
						//If it is mandatory document only then check all job numbers are available in the list.
						if (ruleDetailSet.get(0).getRdtForcedType() == 1) {
							
							//Checking all mandatory documents of job numbers.
		    				if (checkAllMandatoryDocsAvailable(consolidatedAEDList, jobNumbersList,missingDocsMap,documentType))
		    					aedBackUpList.addAll(consolidatedAEDList);
		    				else 
		    					missingMandatoryList.put(documentType,ruleDetailSet);
						} else {
							aedBackUpList.addAll(consolidatedAEDList);
						}
	    			}
				} catch (Exception ex) {
					logger.error("DdpMultiAedRuleJob.performConsolidationProcessForMultiJobNumbers()  : Unable to get backup documents for document type : "+documentType+" : for trrigerring cat id : "+triggeringCatDocs.getCatId(), ex);
					if (ruleDetailSet.get(0).getRdtForcedType() == 1)
						missingMandatoryList.put(documentType,ruleDetailSet);
				}
				
			} else {
				if (ruleDetailSet.get(0).getRdtForcedType() == 1)
					missingMandatoryList.put(documentType,ruleDetailSet);
			}
			
		} else {
			if (ruleDetailSet.get(0).getRdtForcedType() == 1)
				missingMandatoryList.put(documentType,ruleDetailSet);
		}
		
	}
	
	/**
	 * Method used for checking all job numbers are available or not.
	 * 
	 * @param consolidatedAEDList
	 * @param jobNumbersList
	 * @return
	 */
	private boolean checkAllMandatoryDocsAvailable(List<DdpMultiAedSuccessReport> consolidatedAEDList,List<String> jobNumbersList,Map<String,List<String>> missingDocsMap,String docType) {
		
		boolean isAllJobNumbersAvail = true;
		Map<String,List<DdpMultiAedSuccessReport>> uniqueJobNumberMap = new HashMap<String,List<DdpMultiAedSuccessReport>>();
		for (DdpMultiAedSuccessReport multiAED : consolidatedAEDList) {
			
			if (!uniqueJobNumberMap.containsKey(multiAED.getMaedrJobNumber())) {
				List<DdpMultiAedSuccessReport> list = new ArrayList<DdpMultiAedSuccessReport>();
				list.add(multiAED);
				uniqueJobNumberMap.put(multiAED.getMaedrJobNumber(), list);
			} else {
				List<DdpMultiAedSuccessReport> list = uniqueJobNumberMap.get(multiAED.getMaedrJobNumber());
				list.add(multiAED);
				uniqueJobNumberMap.put(multiAED.getMaedrJobNumber(), list);
			}
		}
		
		for (String jobNumber : jobNumbersList) {
			if (uniqueJobNumberMap.get(jobNumber) == null) {
				isAllJobNumbersAvail = false;
				//For mailing purpose
				if (!missingDocsMap.containsKey(docType)) {
					List<String> jobnumbers = new ArrayList<String>();
					jobnumbers.add(jobNumber);
					missingDocsMap.put(docType, jobnumbers);
				} else {
					List<String> jobnumbers = missingDocsMap.get(docType) ;
					jobnumbers.add(jobNumber);
					missingDocsMap.put(docType, jobnumbers);
				}
				logger.info("DdpMultiAedRuleJob.checkAllMandatoryDocsAvailable() : For Multiple Number process this Job Number not available : "+jobNumber+" in DMS.");
			}
		}
		return isAllJobNumbersAvail;
	}
	
	/**
	 * Method used for finding the multiple job numbers for RObjectID.
	 * 
	 * @param jobNumbersList
	 * @param rObjectID
	 * @param session
	 */
	private void findMultipleJobNumbersUsingRObjectID(List<String> jobNumbersList, String rObjectID,IDfSession session) {
		
		try {
			String triggeringQuery = env.getProperty("multiaedrule.multipleJobNumbers.triggeringQuery");
			triggeringQuery = triggeringQuery.replaceAll("%%R_OBJECT_ID%%", rObjectID);
			logger.info("DdpMultiAedRuleJob.findMultipleJobNumbersUsingRObjectID() - Query is for r_object_id :  "+rObjectID+" : Query  ; "+triggeringQuery); 
			IDfCollection iDfCollection = getIDFCollectionDetails(triggeringQuery, session);
			if (iDfCollection != null) {
				while(iDfCollection.next()) { 
					jobNumbersList.add(iDfCollection.getString("agl_job_numbers"));
				}
			}
			logger.info("DdpMultiAedRuleJob.findMultipleJobNumbersUsingRObjectID() - Total size found : "+jobNumbersList);
		} catch (Exception ex) {
			logger.error("DdpMultiAedRuleJob.findMultipleJobNumbersUsingRObjectID() - Unbalet to find the job numbers for r_object_id : "+rObjectID, ex);
		}
	}
	
	/**
	 * Method used for finding the Triggering document details and into collection.
	 * 
	 * @param ddpRuleDetails
	 * @param triggeringDocMap
	 * @param existingList
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService
	 */
	private void findTriggeringDocumentType(List<DdpRuleDetail> ddpRuleDetails,Map<String,DdpCategorizedDocs> triggeringDocMap,
			List<DdpCategorizedDocs> existingList,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,DdpEmailTriggerSetup triggerSetup) {
		
		for (DdpRuleDetail ddpRuleDetail : ddpRuleDetails) {
			
			if (ddpRuleDetail.getRdtRelavantType() != null && ddpRuleDetail.getRdtRelavantType() == 1) {
				
				List<DdpCategorizedDocs> categorizedDocs = getMatchedCategorizedDocs(ddpRuleDetail.getRdtRuleId().getRulId(),ddpRuleDetail.getRdtId(),queryStartDate,queryEndDate,typeOfService);
				for (DdpCategorizedDocs categorizedDoc : categorizedDocs) {
					List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(categorizedDoc.getCatDtxId().getDtxId());
					
					if (ddpDmsDocsDetails != null && ddpDmsDocsDetails.size() > 0) {
						
						//Excluding the document based configured in properties file.
						String excludeDocs = env.getProperty("multiaed.exclude.documents."+ddpDmsDocsDetails.get(0).getDddCompanySource().trim());
						if (excludeDocs != null && !excludeDocs.isEmpty()) {
							boolean isDocExclude = false;
							String[] execludeDocuments = excludeDocs.split(",");
							for(String execludeDoc : execludeDocuments) {
								if (execludeDoc.equalsIgnoreCase(ddpDmsDocsDetails.get(0).getDddContentType().toLowerCase())) {
									//Excluding the documents so the changed the status.
									categorizedDoc.setCatStatus(5);
									ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDoc);
									isDocExclude = true;
									break;
								}
							}
							if (isDocExclude)
								continue;
						}
						//TODO : Based on the triggered type need to include put the key value.
						//Sending only the latest document to user.
						if (triggerSetup.getEtrTriggerName() != null && triggerSetup.getEtrTriggerName().equalsIgnoreCase("consignmentID")) {
							// As per request from kerrin to avoid duplicate consignment ids.
							if (!triggeringDocMap.containsKey(ddpDmsDocsDetails.get(0).getDddConsignmentId()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef())) {
								triggeringDocMap.put(ddpDmsDocsDetails.get(0).getDddConsignmentId()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef(),categorizedDoc);
							}else {
								existingList.add(categorizedDoc);
							}
							
						} else  {
						
							if (!triggeringDocMap.containsKey(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef())) {
								triggeringDocMap.put(ddpDmsDocsDetails.get(0).getDddJobNumber()+"-"+ddpDmsDocsDetails.get(0).getDddDocRef(),categorizedDoc);
							}else {
								existingList.add(categorizedDoc);
							}
						}
					}
				}
			} 
		}
	}
	
	/**
	 * Method used for constructing DQLQuery for ConsolidatedAED based document type, job number
	 * 
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param jobNumber
	 * @return
	 */
	private String constructDQLQueryForConsolidatedAED(List<DdpRuleDetail> ruleDetailSet,List<String> branchSet,String documentType,Calendar queryStartDate,Calendar queryEndDate,
				String jobNumber) {
		
		TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ruleDetailSet.get(0));
		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
		DdpExportVersionSetup ddpExportVersionSetup = null;
		if (ddpExportVersionSetups != null && ddpExportVersionSetups.size() > 0)
			ddpExportVersionSetup =	ddpExportVersionSetups.get(0);
		
		//Checking the RATED & NON-RATED option
		TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetailSet.get(0));
       	List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
		boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
			
		//getting option
		String option = (ddpExportVersionSetup == null ? "latest" : ddpExportVersionSetup.getEvsOption());
		String typeOfServiceValue = "jobNumber";
		String dqlQuery = (option.equalsIgnoreCase("All") ? env.getProperty("multi.aedrule."+typeOfServiceValue+".all") : env.getProperty("multi.aedrule."+typeOfServiceValue+".latest")); 
		
		String dynamicLine = "";	
		
		if (!ruleDetailSet.get(0).getRdtCompany().getComCompanyCode().equalsIgnoreCase("GIL")) {
			
			if (branchSet.size() > 1) {
				dynamicLine = "and agl_branch_source in ("+commonUtil.joinString(branchSet, "'", "'", ",") +")";
			} else {
				if (!branchSet.get(0).equalsIgnoreCase("All"))
					dynamicLine = "and agl_branch_source in ('"+branchSet.get(0) +"')";
			}
			
			dynamicLine += "and agl_company_source in ('" + ruleDetailSet.get(0).getRdtCompany().getComCompanyCode() + "')";
		}
		
		
		String excludeDocs = env.getProperty("multiaed.exclude.documents."+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode());
		if (excludeDocs != null && !excludeDocs.isEmpty()) {
			dynamicLine +=  "and a_content_type not in (" + commonUtil.joinString(excludeDocs.split(","), "'", "'", ",")  + ")";
		}
		
//		dynamicLine += "and " + env.getProperty(ruleDetailSet.get(0).getRdtPartyCode().getPtyPartyCode()) + " in ('"
//				+ ruleDetailSet.get(0).getRdtPartyId() + "')";
		
		
		
		if (queryStartDate != null && queryEndDate != null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
			dynamicLine += "and (r_creation_date between Date('"+dateFormat.format(queryStartDate.getTime())+"','dd/mm/yyyy HH:mi:ss') and Date('"+dateFormat.format(queryEndDate.getTime())+"','dd/mm/yyyy HH:mi:ss'))";
		}
				
		if (isRated) {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%", "agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 1");
		} else {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%","agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 0" );
		}
		
		dqlQuery = dqlQuery.replaceAll("%%DYNANMICQUERY%%", dynamicLine);
		dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%","'"+jobNumber+"'");
		
		logger.info("DdpMultiAedRuleJob.constructDQLQueryForConsolidatedAED - DQL query : "+dqlQuery);
		
		return dqlQuery;
	}
	
	
	/**
	 * Method used for constructing the DQL query for Multiple Job Numbers.
	 * 
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param jobNumbers
	 * @return
	 */
	private String constructDQLQueryForMultipleJobNumbers(List<DdpRuleDetail> ruleDetailSet,List<String> branchSet,String documentType,Calendar queryStartDate,Calendar queryEndDate,
			List<String> jobNumbers) {
	
		TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ruleDetailSet.get(0));
		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
		DdpExportVersionSetup ddpExportVersionSetup = null;
		if (ddpExportVersionSetups != null && ddpExportVersionSetups.size() > 0)
			ddpExportVersionSetup =	ddpExportVersionSetups.get(0);
		
		//Checking the RATED & NON-RATED option
		TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetailSet.get(0));
	   	List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
		boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
			
		//getting option
		String option = (ddpExportVersionSetup == null ? "latest" : ddpExportVersionSetup.getEvsOption());
		String typeOfServiceValue = "jobNumber";
		String dqlQuery = (option.equalsIgnoreCase("All") ? env.getProperty("multi.aedrule.multi."+typeOfServiceValue+".all") : env.getProperty("multi.aedrule.multi."+typeOfServiceValue+".latest")); 
		
		String dynamicLine = "";	
		//As per requirement 9.1 requirement 6 from Pedro company & branch should not be checked.
	//	if (branchSet.size() > 1) {
	//		dynamicLine = "and agl_branch_source in ("+commonUtil.joinString(branchSet, "'", "'", ",") +")";
	//	} else {
	//		if (!branchSet.get(0).equalsIgnoreCase("All"))
	//			dynamicLine = "and agl_branch_source in ('"+branchSet.get(0) +"')";
	//	}
		
		String excludeDocs = env.getProperty("multiaed.exclude.documents."+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode());
		if (excludeDocs != null && !excludeDocs.isEmpty()) {
			dynamicLine +=  "and a_content_type not in (" + commonUtil.joinString(excludeDocs.split(","), "'", "'", ",")  + ")";
		}
		//Based on Pedro request ignoring the Partycode checking.
//		dynamicLine += "and " + env.getProperty(ruleDetailSet.get(0).getRdtPartyCode().getPtyPartyCode()) + " in ('"
//				+ ruleDetailSet.get(0).getRdtPartyId() + "')";
		//dynamicLine += "and agl_company_source in ('" + ruleDetailSet.get(0).getRdtCompany().getComCompanyCode() + "')";
		
		
		if (queryStartDate != null && queryEndDate != null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
			dynamicLine += "and (r_creation_date between Date('"+dateFormat.format(queryStartDate.getTime())+"','dd/mm/yyyy HH:mi:ss') and Date('"+dateFormat.format(queryEndDate.getTime())+"','dd/mm/yyyy HH:mi:ss'))";
		}
				
		if (isRated) {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%", "agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 1");
		} else {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%","agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 0" );
		}
		
		dqlQuery = dqlQuery.replaceAll("%%DYNANMICQUERY%%", dynamicLine);
		dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%",commonUtil.joinString(jobNumbers, "'", "'", ","));
		
		logger.info("DdpMultiAedRuleJob.constructDQLQueryForMultipleJobNumbers - DQL query : "+dqlQuery);
		
		return dqlQuery;
	}

	/**
	 * Method used for constructing the DQL Query for Consignment ID where include type is all.
	 * 
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param consignmentID
	 * @return
	 */
	private String constructDQLQueryForConsignmentIDALL(List<DdpRuleDetail> ruleDetailSet,List<String> branchSet,String documentType,Calendar queryStartDate,Calendar queryEndDate,
			String consignmentID) {
	
	TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ruleDetailSet.get(0));
		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
		DdpExportVersionSetup ddpExportVersionSetup = null;
		if (ddpExportVersionSetups != null && ddpExportVersionSetups.size() > 0)
			ddpExportVersionSetup =	ddpExportVersionSetups.get(0);
		
		//Checking the RATED & NON-RATED option
		TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetailSet.get(0));
	   	List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
		boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
			
		//getting option
		String option = (ddpExportVersionSetup == null ? "latest" : ddpExportVersionSetup.getEvsOption());
		String typeOfServiceValue = "consignmentID";
		String dqlQuery = (option.equalsIgnoreCase("All") ? env.getProperty("multi.aedrule."+typeOfServiceValue+".all") : env.getProperty("multi.aedrule."+typeOfServiceValue+".latest")); 
		
		String dynamicLine = "";	
		//As per requirement 9.1 (requirement 7) no need to check company & branch should not be checked.
			//	if (branchSet.size() > 1) {
			//		dynamicLine = "and agl_branch_source in ("+commonUtil.joinString(branchSet, "'", "'", ",") +")";
			//	} else {
			//		if (!branchSet.get(0).equalsIgnoreCase("All"))
			//			dynamicLine = "and agl_branch_source in ('"+branchSet.get(0) +"')";
			//	}
		//dynamicLine += "and agl_company_source in ('" + ruleDetailSet.get(0).getRdtCompany().getComCompanyCode() + "')";
		
		String excludeDocs = env.getProperty("multiaed.exclude.documents."+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode());
		if (excludeDocs != null && !excludeDocs.isEmpty()) {
			dynamicLine +=  "and a_content_type not in (" + commonUtil.joinString(excludeDocs.split(","), "'", "'", ",")  + ")";
		}
		
//		dynamicLine += "and " + env.getProperty(ruleDetailSet.get(0).getRdtPartyCode().getPtyPartyCode()) + " in ('"
//				+ ruleDetailSet.get(0).getRdtPartyId() + "')";
		
		if (queryStartDate != null && queryEndDate != null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
			dynamicLine += "and (r_creation_date between Date('"+dateFormat.format(queryStartDate.getTime())+"','dd/mm/yyyy HH:mi:ss') and Date('"+dateFormat.format(queryEndDate.getTime())+"','dd/mm/yyyy HH:mi:ss'))";
		}
				
		if (isRated) {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%", "agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 1");
		} else {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%","agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 0" );
		}
		
		dqlQuery = dqlQuery.replaceAll("%%DYNANMICQUERY%%", dynamicLine);
		dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%","'"+consignmentID+"'");
		
		logger.info("DdpMultiAedRuleJob.constructDQLQueryForConsignmentIDALL() - DQL query : "+dqlQuery);
		
		return dqlQuery;
	}
	
	/**
	 * Method used for constructing the query for Consignment ID based on include type is destination.
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param consignmentID
	 * @param jobNumber
	 * @return
	 */
	private String constructDQLQueryForConsignmentIDDestination(List<DdpRuleDetail> ruleDetailSet,List<String> branchSet,String documentType,Calendar queryStartDate,Calendar queryEndDate,
			String consignmentID,String jobNumber) {
	
	TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ruleDetailSet.get(0));
		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
		DdpExportVersionSetup ddpExportVersionSetup = null;
		if (ddpExportVersionSetups != null && ddpExportVersionSetups.size() > 0)
			ddpExportVersionSetup =	ddpExportVersionSetups.get(0);
		
		//Checking the RATED & NON-RATED option
		TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetailSet.get(0));
	   	List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
		boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
			
		//getting option
		String option = (ddpExportVersionSetup == null ? "latest" : ddpExportVersionSetup.getEvsOption());
		String typeOfServiceValue = "consignmentID";
		String dqlQuery = (option.equalsIgnoreCase("All") ? env.getProperty("multi.aedrule."+typeOfServiceValue+".all") : env.getProperty("multi.aedrule."+typeOfServiceValue+".latest")); 
		
		String dynamicLine = "";	
		//As per requirement 9.1 (requirement 7) no need to check company & branch should not be checked.
			//	if (branchSet.size() > 1) {
			//		dynamicLine = "and agl_branch_source in ("+commonUtil.joinString(branchSet, "'", "'", ",") +")";
			//	} else {
			//		if (!branchSet.get(0).equalsIgnoreCase("All"))
			//			dynamicLine = "and agl_branch_source in ('"+branchSet.get(0) +"')";
			//	}
		//dynamicLine += "and agl_company_source in ('" + ruleDetailSet.get(0).getRdtCompany().getComCompanyCode() + "')";
		
		String excludeDocs = env.getProperty("multiaed.exclude.documents."+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode());
		if (excludeDocs != null && !excludeDocs.isEmpty()) {
			dynamicLine +=  "and a_content_type not in (" + commonUtil.joinString(excludeDocs.split(","), "'", "'", ",")  + ")";
		}
		
		//As per kerrin comments.So that it is commented
		//Please checkout what is going on. I suspect you are checking client ID and you should not be.
//		dynamicLine += "and " + env.getProperty(ruleDetailSet.get(0).getRdtPartyCode().getPtyPartyCode()) + " in ('"
//				+ ruleDetailSet.get(0).getRdtPartyId() + "')";
		
		if (queryStartDate != null && queryEndDate != null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
			dynamicLine += "and (r_creation_date between Date('"+dateFormat.format(queryStartDate.getTime())+"','dd/mm/yyyy HH:mi:ss') and Date('"+dateFormat.format(queryEndDate.getTime())+"','dd/mm/yyyy HH:mi:ss'))";
		}
				
		if (isRated) {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%", "agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 1");
		} else {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%","agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 0" );
		}
		
		dynamicLine  += "and any agl_job_numbers not in ('"+jobNumber+"')";
		
		dqlQuery = dqlQuery.replaceAll("%%DYNANMICQUERY%%", dynamicLine);
		dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%","'"+consignmentID+"'");
		
		logger.info("DdpMultiAedRuleJob.constructDQLQueryForConsignmentIDALL() - DQL query : "+dqlQuery);
		
		return dqlQuery;
	}
	
	/**
	 * Method used for constructing the DQL query for Consignment ID of include type is Origin.
	 * 
	 * @param ruleDetailSet
	 * @param branchSet
	 * @param documentType
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param consignmentID
	 * @return
	 */
	private String constructDQLQueryForConsignmentIDOrigin(List<DdpRuleDetail> ruleDetailSet,List<String> branchSet,String documentType,Calendar queryStartDate,Calendar queryEndDate,
			String consignmentID) {
	
		TypedQuery<DdpExportVersionSetup> query = DdpExportVersionSetup.findDdpExportVersionSetupsByEvsRdtId(ruleDetailSet.get(0));
		List<DdpExportVersionSetup> ddpExportVersionSetups = query.getResultList();
		DdpExportVersionSetup ddpExportVersionSetup = null;
		if (ddpExportVersionSetups != null && ddpExportVersionSetups.size() > 0)
			ddpExportVersionSetup =	ddpExportVersionSetups.get(0);
		
		//Checking the RATED & NON-RATED option
		TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ruleDetailSet.get(0));
	   	List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
		boolean isRated = (ddpRateSetupSet != null  &&  !ddpRateSetupSet.isEmpty()  && ddpRateSetupSet.get(0).getRtsOption().equalsIgnoreCase("Rated"))? true : false;
			
		//getting option
		String option = (ddpExportVersionSetup == null ? "latest" : ddpExportVersionSetup.getEvsOption());
		String typeOfServiceValue = "consignmentID";
		String dqlQuery = (option.equalsIgnoreCase("All") ? env.getProperty("multi.aedrule."+typeOfServiceValue+".all") : env.getProperty("multi.aedrule."+typeOfServiceValue+".latest")); 
		
		String dynamicLine = "";	
		
		if (!ruleDetailSet.get(0).getRdtCompany().getComCompanyCode().equalsIgnoreCase("GIL")) {
				
			//As per requirement 9.1 (requirement 7) no need to check company & branch should not be checked.
			if (branchSet.size() > 1) {
					dynamicLine = "and agl_branch_source in ("+commonUtil.joinString(branchSet, "'", "'", ",") +")";
			} else {
					if (!branchSet.get(0).equalsIgnoreCase("All"))
						dynamicLine = "and agl_branch_source in ('"+branchSet.get(0) +"')";
			}
			dynamicLine += "and agl_company_source in ('" + ruleDetailSet.get(0).getRdtCompany().getComCompanyCode() + "')";
		}
		
		String excludeDocs = env.getProperty("multiaed.exclude.documents."+ruleDetailSet.get(0).getRdtCompany().getComCompanyCode());
		if (excludeDocs != null && !excludeDocs.isEmpty()) {
			dynamicLine +=  "and a_content_type not in (" + commonUtil.joinString(excludeDocs.split(","), "'", "'", ",")  + ")";
		}
		
//		dynamicLine += "and " + env.getProperty(ruleDetailSet.get(0).getRdtPartyCode().getPtyPartyCode()) + " in ('"
//				+ ruleDetailSet.get(0).getRdtPartyId() + "')";
		
		if (queryStartDate != null && queryEndDate != null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
			dynamicLine += "and (r_creation_date between Date('"+dateFormat.format(queryStartDate.getTime())+"','dd/mm/yyyy HH:mi:ss') and Date('"+dateFormat.format(queryEndDate.getTime())+"','dd/mm/yyyy HH:mi:ss'))";
		}
				
		if (isRated) {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%", "agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 1");
		} else {
			dqlQuery = dqlQuery.replaceAll("%%PRIMARYDOCTYPE%%","agl_control_doc_type in ('"+documentType+"') and agl_is_rated = 0" );
		}
		
		dqlQuery = dqlQuery.replaceAll("%%DYNANMICQUERY%%", dynamicLine);
		dqlQuery = dqlQuery.replaceAll("%%DYNAMICVALUE%%","'"+consignmentID+"'");
		
		logger.info("DdpMultiAedRuleJob.constructDQLQueryForConsignmentIDALL() - DQL query : "+dqlQuery);
		
		return dqlQuery;
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
			logger.error("DdpMultiAedRuleJob.getIDFCollectionDetails(String query,IDfSession session) - unable to get details of DFCCollections", ex.getMessage());
			taskUtil.sendMailByDevelopers("Unable to execute this query : "+query+"<br/>"+ex, "DdpMultiAedRuleJob.getIDFCollection() not able to fetch.");
		}
		
		return dfCollection;
	}
	
	/**
	 * Method used for constructing the Consolidated AED object.
	 * 
	 * @param iDfCollection
	 * @param ruleID
	 * @param consolidateID
	 * @return
	 */
	private List<DdpMultiAedSuccessReport> constructConsolidatedAEDObject(IDfCollection iDfCollection,Integer ruleID,Integer catID,Integer typeOfService) {
		
		List<DdpMultiAedSuccessReport> consolidatedAED = new ArrayList<DdpMultiAedSuccessReport>();
		
		try {
			while(iDfCollection.next()) {
				DdpMultiAedSuccessReport aed = new DdpMultiAedSuccessReport();
				Calendar createdDate = GregorianCalendar.getInstance();
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				
				aed.setMaedrRuleId(ruleID);
				aed.setMaedrCatId(catID);
				aed.setMaedrRObjectId(iDfCollection.getString("r_object_id"));
				//aed.setMaedrc(iDfCollection.getString("agl_customs_entry_no"));
				aed.setMaedrMasterJobNumber(iDfCollection.getString("agl_master_job_number"));
				aed.setMaedrJobNumber(iDfCollection.getString("agl_job_numbers"));
				aed.setMaedrConsignmentId(iDfCollection.getString("agl_consignment_id"));
				//Invoice number for sales document reference
				aed.setMaedrDocRef(iDfCollection.getString("agl_doc_ref"));
				//Content size
				aed.setMaedrFileSize(iDfCollection.getInt("r_content_size"));
				//System.out.println("Inside the construnctMissingDocs : "+exportDocs.getMisEntryType());
				aed.setMaedrDocType(iDfCollection.getString("agl_control_doc_type"));
				//aed.setMaed(iDfCollection.getString("agl_branch_source"));
				//aed.setMisCompany(iDfCollection.getString("agl_company_source"));
				aed.setMaedrObjectName(iDfCollection.getString("object_name"));
				aed.setMaedrConsolidateDate(GregorianCalendar.getInstance());
				aed.setMaedrConsolidateType(typeOfService+"");
				try {
					createdDate.setTime(dateFormat.parse(iDfCollection.getString("r_creation_date")));
					aed.setMaedrDmsCreated(createdDate);
					
				} catch(Exception ex) {
					logger.error("DdpMultiAedRuleJob.constructConsolidatedAEDObject() - Unable to parse string to date",ex.getMessage());
				}
				consolidatedAED.add(aed);
			
			}
		} catch(Exception ex) {
			logger.error("DdpMultiAedRuleJob.constructConsolidatedAEDObject() - Unable to construct the object", ex);
			consolidatedAED = null;
		}
		
		return consolidatedAED;
	}
	
	/**
	 * Method used for constructing the all version documents for Consolidated AED.
	 * 
	 * @param ddpCategorizedDocs
	 * @param session
	 * @param typeOfService
	 * @return
	 */
	private List<DdpMultiAedSuccessReport> constructAllVersionConsolidatedAED(DdpCategorizedDocs ddpCategorizedDocs,IDfSession session,Integer typeOfService) {
		
		List<DdpMultiAedSuccessReport> consolidatedAED = new ArrayList<DdpMultiAedSuccessReport>();
		List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(ddpCategorizedDocs.getCatDtxId().getDtxId());
		DdpDFCClient client =  new DdpDFCClient();
		Hashtable<String, String> dmsMetadata = client.begin(session,ddpDmsDocsDetails.get(0).getDddRObjectId());
		
				
		logger.debug("DdpMultiAedRuleJob.constructAllVersionConsolidatedAED() : Chornicle id : "+dmsMetadata.get("i_chronicle_id"));
   	 	try {
   	 		String query = "select r_object_id, agl_customs_entry_no, agl_master_job_number, agl_job_numbers, agl_consignment_id, agl_doc_ref, agl_control_doc_type, r_creation_date,r_content_size,agl_company_source,agl_branch_source,object_name from agl_control_document (all) where i_chronicle_id= '"+dmsMetadata.get("i_chronicle_id")+"'";

   	 	  IDfQuery dfQuery = new DfQuery();
			dfQuery.setDQL(query);
			IDfCollection dfCollection = dfQuery.execute(session,
					IDfQuery.DF_EXEC_QUERY);
			
			List<DdpMultiAedSuccessReport> list  = constructConsolidatedAEDObject( dfCollection, ddpCategorizedDocs.getCatRulId().getRulId(),ddpCategorizedDocs.getCatId() , typeOfService);
			if (list != null && list.size()  > 0)
				consolidatedAED.addAll(list);
			
			dfCollection.close();	
   	 	} catch (Exception ex) {
   	 		
   	 	}
   	 	if (consolidatedAED.size() < 1)
   	 		consolidatedAED.add(constructLatestConsolidatedAED(ddpCategorizedDocs, typeOfService));
			
   	 	return consolidatedAED;
	}
   	 	
	/**
	 * Method used for constructing the latest versiong of Consolidated AED.
	 * 
	 * @param ddpCategorizedDocs
	 * @param typeOfService
	 * @return
	 */
   	 private DdpMultiAedSuccessReport constructLatestConsolidatedAED (DdpCategorizedDocs ddpCategorizedDocs,Integer typeOfService) {
   		 
   		List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(ddpCategorizedDocs.getCatDtxId().getDtxId());
   		
   		DdpMultiAedSuccessReport aed = new DdpMultiAedSuccessReport();
		//Calendar createdDate = GregorianCalendar.getInstance();
		//SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		
		aed.setMaedrRuleId(ddpCategorizedDocs.getCatRulId().getRulId());
		aed.setMaedrCatId(ddpCategorizedDocs.getCatId());
		aed.setMaedrRObjectId(ddpDmsDocsDetails.get(0).getDddRObjectId());
		//aed.setMaedrc(iDfCollection.getString("agl_customs_entry_no"));
		aed.setMaedrMasterJobNumber(ddpDmsDocsDetails.get(0).getDddMasterJobNumber());
		aed.setMaedrJobNumber(ddpDmsDocsDetails.get(0).getDddJobNumber());
		aed.setMaedrConsignmentId(ddpDmsDocsDetails.get(0).getDddConsignmentId());
		//Invoice number for sales document reference
		aed.setMaedrDocRef(ddpDmsDocsDetails.get(0).getDddDocRef());
		//Content size
		aed.setMaedrFileSize(Integer.parseInt(ddpDmsDocsDetails.get(0).getDddContentSize()));
		//System.out.println("Inside the construnctMissingDocs : "+exportDocs.getMisEntryType());
		aed.setMaedrDocType(ddpDmsDocsDetails.get(0).getDddControlDocType());
		//aed.setMaed(iDfCollection.getString("agl_branch_source"));
		//aed.setMisCompany(iDfCollection.getString("agl_company_source"));
		aed.setMaedrObjectName(ddpDmsDocsDetails.get(0).getDddObjectName());
		aed.setMaedrConsolidateDate(GregorianCalendar.getInstance());
		aed.setMaedrConsolidateType(typeOfService+"");
//		try {
//			//createdDate.setTime(dateFormat.parse(iDfCollection.getString("r_creation_date")));
//			//aed.setMaedrConsolidateDate(ddpDmsDocsDetails.get(0).getDdd);
//			
//		} catch(Exception ex) {
//			logger.error("DdpMultiAedRuleJob.constructConsolidatedAEDObject() - Unable to parse string to date",ex.getMessage());
//		}
		//consolidatedAED.add(aed);
		
		return aed;
   	 }
   	 
   	/**
 	 * Method used for performing the mail Triggering process
 	 * 
 	 * @param ddpCategorizedDcosList
 	 * @param ddpRuleDetail
 	 * @return
 	 */
 	private boolean performMailTriggerProcess(List<DdpMultiAedSuccessReport> aedTriggeringList, List<DdpMultiAedSuccessReport> aedBackupList, List<DdpCategorizedDocs> ddpCategorizedDcosList,List<DdpRuleDetail> ddpRuleDetails,String uniqueNumber,
 			int typeOfService,DdpEmailTriggerSetup ddpEmailTriggerSetup,
 			DdpCommEmail ddpCommEmails,IDfSession session) {
 		
 		logger.info("DdpMultiAedRuleJob.performMailTriggerProcess(List<DdpCategorizedDocs> ddpCategorizedDocsList) is inovked");
 		boolean isMailProcessed = false;
 		String tempFilePath = env.getProperty("ddp.export.folder");
 		
 		
 		boolean isAllFilesDownload  = true;
     	String mailBody = "";
     	int executionCode = -1;
    
     	//To avoid time creation of session for each iteration of loop
     	File tmpFile = null;
     	
     	Date date = new Date() ;
     	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
 		try {
 			
 	    	String tempFolderPath = tempFilePath+"/temp_multiaed_"+ddpRuleDetails.get(0).getRdtRuleId().getRulId() +"_" +uniqueNumber.replaceAll("[^a-zA-Z0-9_.]", "") +"_"+dateFormat.format(date);
 	    	tmpFile = FileUtils.createFolder(tempFolderPath);
 	    				
 			DdpMultiAedRule	rule = ddpRuleDetails.get(0).getRdtRuleId().getDdpMultiAedRule();
 			DdpCompressionSetup compressionSetup =  rule.getMaedCompressionId();
 			Map<Integer,Integer> zipMap = new HashMap<Integer,Integer>();
 			int count = 1;
 			int zipCount = 1;
 			boolean isZipComp = false;
 			String sourceZipFilePath = null;
 	    	File sourceZipFile = null;
 			
 	    	String sourceFilePath = tempFolderPath+ "/" + ATTACHMENT + "_" + count;
 	    	File sourceFile = FileUtils.createFolder(sourceFilePath);
 	    	
 	    	
 	    	if  (compressionSetup.getCtsNoOfFilesAttached() == 1 && compressionSetup.getCtsCompressionLevel() != null && compressionSetup.getCtsCompressionLevel().equalsIgnoreCase(Constant.ZIP_COMPRESSION)) {
 	    		
 				isZipComp = true;
 				sourceZipFilePath  = sourceFilePath + "/" + ZIP + "_" + zipCount;
 				sourceZipFile = FileUtils.createFolder(sourceZipFilePath);
 				zipMap.put(count, zipCount);
 			}
 	    	
 	    	//Merging the file into single pdf.
 	    	if  (compressionSetup.getCtsNoOfFilesAttached() == 1 && compressionSetup.getCtsCompressionLevel() != null && compressionSetup.getCtsCompressionLevel().equalsIgnoreCase(Constant.MAED_MERGE)) {
 	    		
 	    		String fileName = null;
 	    		List<String> rObjectList = new LinkedList<String>();
 	    		SortedMap<Integer,List<DdpMultiAedSuccessReport>> aedFinalList = new TreeMap<Integer, List<DdpMultiAedSuccessReport>>();
 	    		arrangeDocumentInSequenceOrder(aedTriggeringList, aedBackupList, ddpRuleDetails, aedFinalList);
 	    		for (Integer sequenceOrder : aedFinalList.keySet()) {
 	    			List<DdpMultiAedSuccessReport> list = aedFinalList.get(sequenceOrder);
 	    			if (list != null) {
	 	    			for (DdpMultiAedSuccessReport multiAED : list) {
	 	    				rObjectList.add(multiAED.getMaedrRObjectId());
								mailBody += constructMailBody(multiAED);
	 	    			}
 	    			}
 	    		}
 	    		
 				fileName = "consolidated" + "-" + uniqueNumber.replaceAll("[^a-zA-Z0-9_.]", "") +".pdf";
 	    		String mergeID = commonUtil.performMergeOperation(rObjectList, fileName, env.getProperty("ddp.vdLocation"), env.getProperty("ddp.objectType"), session,ddpRuleDetails.get(0).getRdtCompany().getComCompanyCode()+":MULTI_AED-"+uniqueNumber);
 	    		if (mergeID != null) {
 	    			isAllFilesDownload = createFile(sourceFilePath, fileName, null, mergeID, session);
 	    		} else {
 	    			isAllFilesDownload = false;
 	    			logger.info("Unable to merge the document due to the ADTS server not responding for Rule ID : "+ddpRuleDetails.get(0).getRdtRuleId().getRulId());
 	    			//Not able to merge due to ADTS server.
 	    			//executionCode = 3;
 	    			taskUtil.sendMailByDevelopers("DdpMultiAedRuleJob.performMailTriggerProcess(List<DdpCategorizedDocs> ddpCategorizedDocsList).<br> Unable to merge the document due to the ADTS server not responding for Rule ID : "+ddpRuleDetails.get(0).getRdtRuleId().getRulId(), "Consolidated AED merging issue");
 	    		}
 	    		
 	    	} else {
 	    		//Non merged files
 	    		List<DdpMultiAedSuccessReport> aedFinalList = new ArrayList<DdpMultiAedSuccessReport>();
 	    		aedFinalList.addAll(aedTriggeringList);
 	    		aedFinalList.addAll(aedBackupList);
 	    	
 				for (DdpMultiAedSuccessReport aedReport : aedFinalList) {
 					
 					//If the greater than 15 MB then it creates new file & new mail.
 					if (FileUtils.findSizeofFileInMB(sourceFile) >= 15) {
 						
 						FileUtils.createFileWithContent(sourceFilePath+"/mail.txt", mailBody);
 						mailBody = null;
 						count++;
 						zipCount = 1;
 						sourceFilePath = tempFolderPath+ "/" + ATTACHMENT + "_" + count;
 				    	sourceFile = FileUtils.createFolder(sourceFilePath);
 				    	
 				    	if (isZipComp) {
 				    		zipMap.put(count, zipCount);
 							sourceZipFilePath  = sourceFilePath + "/" + ZIP + "_" + zipCount;
 							sourceZipFile = FileUtils.createFolder(sourceZipFilePath);
 				    	}
 					}
 					
 					if (isZipComp && FileUtils.findSizeofFileInMB(sourceZipFile) >= 15) {
 						
 						zipCount++;
 						zipMap.put(count, zipCount);
 						sourceZipFilePath  = sourceFilePath + "/" + ZIP + "_" + zipCount;
 						sourceZipFile = FileUtils.createFolder(sourceZipFilePath);
 					}
 					
 											
 					boolean isCreated = false;		
    				String fileName = getFileName(isZipComp, aedReport.getMaedrObjectName(), sourceZipFilePath, sourceFilePath);
 							
 					if (isZipComp)
 						isCreated = createFile(sourceZipFilePath, fileName, null, aedReport.getMaedrRObjectId(), session);
 					else
 						isCreated = createFile(sourceFilePath, fileName, null, aedReport.getMaedrRObjectId(), session);
 							
 					mailBody += constructMailBody(aedReport);
 		    			
 					if (!isCreated) {
 						isAllFilesDownload = false;
 						executionCode = 2;
 						break;
 					}
 						
 				}
 	    	} 
 			
 			if (isAllFilesDownload) {
 				
 				// Number of attachments is single
 				if (compressionSetup.getCtsNoOfFilesAttached() == 0  || (compressionSetup.getCtsCompressionLevel() != null && compressionSetup.getCtsCompressionLevel().equalsIgnoreCase(Constant.MAED_MERGE))) {
 						
 					for (int i = 1; i <= count; i ++) {
 							
 						sourceFilePath = tempFolderPath+ "/" + ATTACHMENT + "_" + i;
 						String mailContent = "";
 						if (i == count) 
 							mailContent = mailBody;
 						else
 							mailContent = FileUtils.readTextFile(sourceFilePath +"/mail.txt");
 						executionCode = sendMailUsingCommunicationSetup(ddpCommEmails,new File(sourceFilePath), ddpCategorizedDcosList.get(0), ddpRuleDetails.get(0),mailContent,uniqueNumber,aedTriggeringList.get(0).getMaedrDocType(),ddpEmailTriggerSetup);
 					}
 					
 				} else {
 						 						
 						for (int i = 1; i <= count; i ++) {
 							
 							sourceFilePath = tempFolderPath+ "/" + ATTACHMENT + "_" + i;
 							String zipPath =  tempFolderPath + "/" + ZIP + "_" + i;
 							File zipFile = FileUtils.createFolder(zipPath);
 							String mailContent = "";
 							
 							if (i == count) 
 								mailContent = mailBody;
 							else
 								mailContent = FileUtils.readTextFile(sourceFilePath +"/mail.txt");
 							for (int j = 1; j <= zipMap.get(i) ; j ++) {
 								FileUtils.zipFiles(zipPath+"/"+"consolidated-"+uniqueNumber +".zip", sourceFilePath + "/" + ZIP + "_" + j);
 							}
 							executionCode = sendMailUsingCommunicationSetup(ddpCommEmails,zipFile, ddpCategorizedDcosList.get(0), ddpRuleDetails.get(0),mailContent,uniqueNumber,aedTriggeringList.get(0).getMaedrDocType(),ddpEmailTriggerSetup);
 						}
 						
 					}
 				} else {
 				 	taskUtil.sendMailByDevelopers("DdpMultiAedRuleJob.performTriggerProcess() unable to download the documents into local due to any issue. where Job number /Consignment ID : "+uniqueNumber+". <br> list of documents"+ddpCategorizedDcosList, "unable to download documents of Scheduler Mail.");
 				}
 			
 			
 			if (executionCode == 1)
 				isMailProcessed = true;
 			
 			if (executionCode == 1 || executionCode == -1) {
 				updateStatusOfCategorizedDocuments(ddpCategorizedDcosList,executionCode);
 				updateStatusForConsoidatedAEDRepot(aedTriggeringList, executionCode, typeOfService);
 				updateStatusForConsoidatedAEDRepot(aedBackupList, executionCode, typeOfService);
 			}
 			
 		} catch (Exception ex) {
 			logger.error("DdpMultiAedRuleJob.performMailTriggerProcess(List<DdpCategorizedDocs> ddpCategorizedDocsList) unable to send the mails.",ex);
 			taskUtil.sendMailByDevelopers("DdpMultiAedRuleJob.performMailTriggerProcess(List<DdpCategorizedDocs> ddpCategorizedDocsList) unable to send the mails.<br>"+ex, "Scheduler mail is notable send");
 			
 		} finally {
 			
 			if (tmpFile != null)
 				SchedulerJobUtil.deleteFolder(tmpFile);
 		}
 		logger.info("DdpMultiAedRuleJob.performMailTriggerProcess(List<DdpCategorizedDocs> ddpCategorizedDocsList) is successfully compeleted");
 		return isMailProcessed;
 	}
   	
 	/**
 	 * Method used for arranging the documents in Sequence order based on rule details.
 	 * 
 	 * @param aedTriggeringList
 	 * @param aedBackUpList
 	 * @param ddpRuleDetails
 	 * @param aedFinalList
 	 */
 	private void arrangeDocumentInSequenceOrder(List<DdpMultiAedSuccessReport> aedTriggeringList,List<DdpMultiAedSuccessReport> aedBackUpList,
 			List<DdpRuleDetail> ddpRuleDetails,SortedMap<Integer,List<DdpMultiAedSuccessReport>> aedFinalList) {
 		
 		SortedMap<Integer,List<DdpRuleDetail>> seqRuleMap = new TreeMap<Integer, List<DdpRuleDetail>>();
 		SortedMap<String, List<DdpMultiAedSuccessReport>> aedList = new TreeMap<String, List<DdpMultiAedSuccessReport>>();
 		
 		for(DdpRuleDetail ddpRuleDetail : ddpRuleDetails) {
 			
 			if (!seqRuleMap.containsKey(ddpRuleDetail.getRdtDocSequence())) {
 				
 				List<DdpRuleDetail> list = new ArrayList<DdpRuleDetail>();
 				list.add(ddpRuleDetail);
 				seqRuleMap.put(ddpRuleDetail.getRdtDocSequence(), list);
 				
 			} else {
 				List<DdpRuleDetail> list = seqRuleMap.get(ddpRuleDetail.getRdtDocSequence());
 				list.add(ddpRuleDetail);
 				seqRuleMap.put(ddpRuleDetail.getRdtDocSequence(), list);
 			}
 		}
 		
 		for (DdpMultiAedSuccessReport aed : aedTriggeringList) {
 			
 			if (!aedList.containsKey(aed.getMaedrDocType())) {
 				List<DdpMultiAedSuccessReport> list = new ArrayList<DdpMultiAedSuccessReport>();
 				list.add(aed);
 				aedList.put(aed.getMaedrDocType(), list);
 			} else {
 				List<DdpMultiAedSuccessReport> list = aedList.get(aed.getMaedrDocType());
 				list.add(aed);
 				aedList.put(aed.getMaedrDocType(), list);
 			}
 		}
 		
 		for (DdpMultiAedSuccessReport aed : aedBackUpList) {
 			
 			if (!aedList.containsKey(aed.getMaedrDocType())) {
 				List<DdpMultiAedSuccessReport> list = new ArrayList<DdpMultiAedSuccessReport>();
 				list.add(aed);
 				aedList.put(aed.getMaedrDocType(), list);
 			} else {
 				List<DdpMultiAedSuccessReport> list = aedList.get(aed.getMaedrDocType());
 				list.add(aed);
 				aedList.put(aed.getMaedrDocType(), list);
 			}
 		}
 		
 		for (Integer sequence : seqRuleMap.keySet()) {
 			List<DdpRuleDetail> ruleDetailList = seqRuleMap.get(sequence);
 			List<DdpMultiAedSuccessReport>  consAEDList = aedList.get(ruleDetailList.get(0).getRdtDocType().getDtyDocTypeCode());
 			aedFinalList.put(sequence, consAEDList);
 		}
 		
 	}
	
	/**
	 * Method used for sending the failure mails.
	 * 
	 * @param ddpCommEmails
	 * @param missingDocuments
	 * @param typeOfService
	 * @param primaryDocumentCount
	 * @param emailTriggerSetup
	 * @param uniqueNumber
	 */
	private void sendMailFailureMultiAEDRules(DdpCommEmail ddpCommEmails,Map<String,List<DdpRuleDetail>> missingDocuments,int typeOfService,DdpEmailTriggerSetup emailTriggerSetup,String uniqueNumber,DdpCategorizedDocs ddpCategorizedDocs) {
		
		
		logger.info("DdpMultiAedRuleJob.sendMailFailureMultiAEDRules() method is inovked");
		String mailSubject = null;
		String mailBody = null;
		DdpRuleDetail ruleDetail = null;
		for (String documentType :missingDocuments.keySet()) {
			List<DdpRuleDetail> list = missingDocuments.get(documentType);
			ruleDetail = list.get(0);
		}
			
		
		DdpMultiAedRule ddpMultiAedRule = ddpMultiAedRuleService.findDdpMultiAedRule(ruleDetail.getRdtRuleId().getRulId());
		String toAddress = ddpMultiAedRule.getMaedNotificationId().getNotFailureEmailAddress();
		try {
			mailSubject = env.getProperty("multiaedmail.missing.subject."+ruleDetail.getRdtCompany().getComCompanyCode());
		} catch (Exception ex) {
			
		}
		if(mailSubject == null || mailSubject.isEmpty()) {
			try {
				mailSubject = env.getProperty("multiaedmail.missing.subject");
			} catch (Exception ex) {
				mailSubject = "Auto Emailing from DDP";
			}
		}
		
		mailSubject = mailSubject.replaceAll("%%DYNAMICNAME%%", env.getProperty("multiaedmail."+emailTriggerSetup.getEtrTriggerName()));
		mailSubject = mailSubject.replaceAll("%%JOBNUMBER%%", uniqueNumber);
		
		try {
			 mailBody = env.getProperty("multiaedmail.missing.body."+ruleDetail.getRdtCompany().getComCompanyCode());
			} catch (Exception ex) {
				
			}
		if(mailBody == null || mailBody.isEmpty()) {
			try {
				mailBody = env.getProperty("multiaedmail.missing.body");
			} catch (Exception ex){
				mailBody = "<P><h2><font color='orange'>Missing Document Details</font></h2>%%ADDITIONALBODY%%<table border='2'><tr><td>Type of Service</td>"
						+ "<td>%%TYPEOFSERVICE%%</td></tr><tr><td>Number of mandatory missing</td><td>%%MANDCOUNT%%</td></tr><tr><td>Available Retries</td>"
						+ "<td>%%RETRIES%%</td></tr></table><table><br><br><tr style='background-color:rgb(244,160,65);'><th>Document Type</th>"
						+ "<th>%%UNIQUENAME%%</th><th>Company</th><th>Client ID</th></tr></table><br/><span>please do not respond to this mail, "
						+ "as this account is intended for outbound emails only</span><br/><br/></P>"
						+ "<IMG SRC='http://www.latc.la/advhtml_upload/agi_gil-home-logo.png' ALT='GIL'>";
			}
		}
		
		mailBody = mailBody.replaceAll("%%TYPEOFSERVICE%%", (typeOfService == 2) ? "ReProces Service" : "General Service");
		int retriesCount = ((emailTriggerSetup.getEtrRetries() == null || emailTriggerSetup.getEtrRetries().intValue() == 0  || typeOfService == 1)? 0 : emailTriggerSetup.getEtrRetries().intValue());
		int ddpCatRetries = (ddpCategorizedDocs.getCatRetries() == null ? 0 : ddpCategorizedDocs.getCatRetries().intValue());
		
		if ((retriesCount-1) == ddpCatRetries)
			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%",env.getProperty("multiaed.missing.lastbutonecount"));
		else if (retriesCount != 0 && (retriesCount == ddpCatRetries))
			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%",env.getProperty("multiaed.missing.lastcount"));
		else 
			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%","");
		
		mailBody = mailBody.replaceAll("%%MANDCOUNT%%", missingDocuments.size()+"");
		mailBody = mailBody.replaceAll("%%RETRIES%%", ((emailTriggerSetup.getEtrRetries() == null || emailTriggerSetup.getEtrRetries().intValue() == 0 ? 0 : (typeOfService == 1 ?emailTriggerSetup.getEtrRetries().intValue() : emailTriggerSetup.getEtrRetries().intValue()-ddpCatRetries))+""));
		mailBody = mailBody.replaceAll("%%RULEID%%", ruleDetail.getRdtRuleId().getRulId()+"");
		
		//mailBody = mailBody.replaceAll("%%UNIQUENAME%%", emailTriggerSetup.getEtrTriggerName().toUpperCase());
		String tableDetails = "";
		for (String documentType :missingDocuments.keySet()) {
			List<DdpRuleDetail> list = missingDocuments.get(documentType);
			tableDetails = tableDetails + "<tr><td>"+documentType+"</td>";
			tableDetails = tableDetails +"<td>"+uniqueNumber+"</td>";
			tableDetails = tableDetails + "<td>"+ list.get(0).getRdtCompany().getComCompanyCode()+"</td>";
			tableDetails = tableDetails + "<td>" + list.get(0).getRdtPartyId() + "</td></tr>";						
		}
		mailBody = mailBody.replaceAll("%%DETAILS%%", tableDetails);
		String mailFrom=env.getProperty("mail.fromAddress");
		int attachFlag = 0;
		String commProtocol = ruleDetail.getRdtCommunicationId().getCmsCommunicationProtocol();
		
		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
			//This should be get from the configuration file or from table
//			String smtpAddres ="10.20.1.233";
			String smtpAddres = env.getProperty("mail.smtpAddress");
			logger.info("RuleJob.communication() : calling sendMail() ");
			attachFlag = taskUtil.sendMail(smtpAddres, toAddress, null, mailFrom, mailSubject, mailBody, null);
			logger.info("DdpMultiAedRuleJob.sendMailFailureMultiAEDRules() - Status of sending the Failure Email for Consolidated AED for Rule ID : "+ruleDetail.getRdtRuleId().getRulId()+" : Email Send Status : "+attachFlag);
		}
		
	}
	
	/**
	 * Failure mail for Multiple job numbers for AED rule.
	 * 
	 * Method used for 
	 * @param ddpCommEmails
	 * @param missingDocuments
	 * @param typeOfService
	 * @param emailTriggerSetup
	 * @param uniqueNumber
	 * @param ddpCategorizedDocs
	 * @param ruleDetail
	 */
	private void sendMailFailureMultipleJobNumbersAEDRules(DdpCommEmail ddpCommEmails,Map<String,List<String>> missingDocuments,int typeOfService,DdpEmailTriggerSetup emailTriggerSetup,String uniqueNumber,DdpCategorizedDocs ddpCategorizedDocs,DdpRuleDetail ruleDetail) {
		
		
		logger.info("DdpMultiAedRuleJob.sendMailFailureMultiAEDRules() method is inovked");
		String mailSubject = null;
		String mailBody = null;
//		DdpRuleDetail ruleDetail = null;
//		for (String documentType :missingDocuments.keySet()) {
//			List<DdpRuleDetail> list = missingDocuments.get(documentType);
//			ruleDetail = list.get(0);
//		}
			
		
		DdpMultiAedRule ddpMultiAedRule = ddpMultiAedRuleService.findDdpMultiAedRule(ruleDetail.getRdtRuleId().getRulId());
		String toAddress = ddpMultiAedRule.getMaedNotificationId().getNotFailureEmailAddress();
		try {
			mailSubject = env.getProperty("multiaedmail.missing.mulitple.jobnumbers.subject."+ruleDetail.getRdtCompany().getComCompanyCode());
		} catch (Exception ex) {
			
		}
		if(mailSubject == null || mailSubject.isEmpty()) {
			try {
				mailSubject = env.getProperty("multiaedmail.missing.mulitple.jobnumbers.subject");
			} catch (Exception ex) {
				mailSubject = "Auto Emailing from DDP";
			}
		}
		
		mailSubject = mailSubject.replaceAll("%%DYNAMICNAME%%", env.getProperty("multiaedmail."+emailTriggerSetup.getEtrTriggerName()));
		mailSubject = mailSubject.replaceAll("%%JOBNUMBER%%", ruleDetail.getRdtPartyId());
		
		try {
			 mailBody = env.getProperty("multiaedmail.missing.body."+ruleDetail.getRdtCompany().getComCompanyCode());
			} catch (Exception ex) {
				
			}
		if(mailBody == null || mailBody.isEmpty()) {
			try {
				//mailBody = env.getProperty("multiaedmail.missing.body");
				mailBody = env.getProperty("multiaedmail.missing.multiple.jobnumbers.body");
			} catch (Exception ex){
				mailBody = "<P><h2><font color='orange'>Missing Document Details</font></h2>%%ADDITIONALBODY%%<table border='2'><tr><td>Type of Service</td>"
						+ "<td>%%TYPEOFSERVICE%%</td></tr><tr><td>Number of mandatory missing</td><td>%%MANDCOUNT%%</td></tr><tr><td>Available Retries</td>"
						+ "<td>%%RETRIES%%</td></tr></table><table><br><br><tr style='background-color:rgb(244,160,65);'><th>Document Type</th>"
						+ "<th>%%UNIQUENAME%%</th><th>Company</th><th>Client ID</th></tr></table><br/><span>please do not respond to this mail, "
						+ "as this account is intended for outbound emails only</span><br/><br/></P>"
						+ "<IMG SRC='http://www.latc.la/advhtml_upload/agi_gil-home-logo.png' ALT='GIL'>";
			}
		}
		
		mailBody = mailBody.replaceAll("%%TYPEOFSERVICE%%", (typeOfService == 2) ? "ReProces Service" : "General Service");
		int retriesCount = ((emailTriggerSetup.getEtrRetries() == null || emailTriggerSetup.getEtrRetries().intValue() == 0  || typeOfService == 1)? 0 : emailTriggerSetup.getEtrRetries().intValue());
		int ddpCatRetries = (ddpCategorizedDocs.getCatRetries() == null ? 0 : ddpCategorizedDocs.getCatRetries().intValue());
		
		if ((retriesCount-1) == ddpCatRetries)
			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%",env.getProperty("multiaed.missing.lastbutonecount"));
		else if (retriesCount != 0 && (retriesCount == ddpCatRetries))
			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%",env.getProperty("multiaed.missing.lastcount"));
		else 
			mailBody = mailBody.replaceAll("%%ADDITIONALBODY%%","");
		
		mailBody = mailBody.replaceAll("%%MANDCOUNT%%", missingDocuments.size()+"");
		mailBody = mailBody.replaceAll("%%RETRIES%%", ((emailTriggerSetup.getEtrRetries() == null || emailTriggerSetup.getEtrRetries().intValue() == 0 ? 0 : (typeOfService == 1 ?emailTriggerSetup.getEtrRetries().intValue() : emailTriggerSetup.getEtrRetries().intValue()-ddpCatRetries))+""));
		mailBody = mailBody.replaceAll("%%RULEID%%", ruleDetail.getRdtRuleId().getRulId()+"");
		
		String missingJobNumbers = "";
		if (missingDocuments.size() > 0) {
			List<String> list = missingDocuments.get(0);			
			for (String jobNumber : list) {
				missingJobNumbers = missingJobNumbers.concat(","+jobNumber);
			}
		} else {
			missingJobNumbers = (uniqueNumber == null)? "" : uniqueNumber+"";
		}
		
	
		mailBody = mailBody.replaceAll("%%JOBNUM%%", missingJobNumbers);
		
		//mailBody = mailBody.replaceAll("%%UNIQUENAME%%", emailTriggerSetup.getEtrTriggerName().toUpperCase());
		String tableDetails = "";
//		for (String documentType :missingDocuments.keySet()) {
//			List<String> list = missingDocuments.get(documentType);
//			for (String jobNumber : list) {
//				tableDetails = tableDetails + "<tr><td>"+documentType+"</td>";
//				tableDetails = tableDetails +"<td>"+jobNumber+"</td>";
//				tableDetails = tableDetails + "<td>"+ ruleDetail.getRdtCompany().getComCompanyCode()+"</td>";
//				tableDetails = tableDetails + "<td>" + ruleDetail.getRdtPartyId() + "</td></tr>";	
//			}
//		}
		for (String documentType :missingDocuments.keySet()) {
			tableDetails = tableDetails + "<tr><td>"+documentType+"</td>";
			List<String> list = missingDocuments.get(documentType);
			String jobNumbersListstr = "";
			for (String jobNumber : list) {
				jobNumbersListstr = jobNumbersListstr.concat(","+jobNumber);
			}
			tableDetails = tableDetails +"<td>"+jobNumbersListstr+"</td>";
		}
		mailBody = mailBody.replaceAll("%%DETAILS%%", tableDetails);
		String mailFrom=env.getProperty("mail.fromAddress");
		int attachFlag = 0;
		String commProtocol = ruleDetail.getRdtCommunicationId().getCmsCommunicationProtocol();
		
		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
			//This should be get from the configuration file or from table
//			String smtpAddres ="10.20.1.233";
			String smtpAddres = env.getProperty("mail.smtpAddress");
			logger.info("RuleJob.communication() : calling sendMail() ");
			attachFlag = taskUtil.sendMail(smtpAddres, toAddress, null, mailFrom, mailSubject, mailBody, null);
			logger.info("DdpMultiAedRuleJob.sendMailFailureMultiAEDRules() - Status of sending the Failure Email for Consolidated AED for Rule ID : "+ruleDetail.getRdtRuleId().getRulId()+" : Email Send Status : "+attachFlag);
		}
		
	}

	/**
	 * Method used for sending the mail using the communication set.
	 * 
	 * @param listFileMap
	 * @param ddpDmsDocsDetail
	 * @param ddpRuleDetail
	 * @return
	 */
	private int sendMailUsingCommunicationSetup(DdpCommEmail ddpCommEmails,File tmpFile,DdpCategorizedDocs ddpCategorizedDocs,
			DdpRuleDetail ddpRuleDetail,String mailBodyContent,String uniqueNumber,String triggeringDocType,DdpEmailTriggerSetup triggerSetup) {
		
		logger.info("DdpMultiAedRuleJob.sendMailUsingCommunicationSetup() method is inovked");
		List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(ddpCategorizedDocs.getCatDtxId().getDtxId());
		DdpDmsDocsDetail ddpDmsDocsDetail = ddpDmsDocsDetails.get(0);
		
		String	mailSubject =  null;
		try {
			mailSubject  = env.getProperty("multiaedmail.subject."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType()+"."+ddpRuleDetail.getRdtPartyId());
		} catch (Exception ex) {
			
		}
		if (mailSubject == null || mailSubject.isEmpty()) {
			try {
				mailSubject  = env.getProperty("multiaedmail.subject."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
			} catch (Exception ex) {
				
			}
		}
		if (mailSubject == null || mailSubject.isEmpty()) {
			try {
				mailSubject = env.getProperty("multiaedmail.subject."+ddpDmsDocsDetail.getDddCompanySource());
			} catch (Exception ex) {
				
			}
		}
		if(mailSubject == null || mailSubject.isEmpty()) {
			try {
				mailSubject = env.getProperty("multiaedmail.subject");
			} catch (Exception ex) {
				mailSubject = "Auto Emailing from DDP";
			}
		}
		String docRef = ddpDmsDocsDetail.getDddDocRef();
		docRef = (docRef.contains("-R"))? docRef.substring(0, docRef.indexOf("-R")) : docRef;
		
		mailSubject = mailSubject.replaceAll("%%DYNAMICNAME%%", env.getProperty("multiaedmail."+triggerSetup.getEtrTriggerName()));
		mailSubject = mailSubject.replaceAll("%%JOBNUMBER%%", uniqueNumber);
		mailSubject = mailSubject.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
		mailSubject = mailSubject.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
		mailSubject = mailSubject.replaceAll("%%DOCREF%%", docRef);
		mailSubject = mailSubject.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef()); 
		mailSubject = mailSubject.replaceAll("%%JOBNO%%", ddpDmsDocsDetail.getDddJobNumber());
		mailSubject = mailSubject.replaceAll("%%REFERENCE%%", ddpDmsDocsDetail.getDddJobNumber());
		mailSubject = mailSubject.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
		mailSubject = mailSubject.replaceAll("%%COMPANY%%", ddpDmsDocsDetail.getDddCompanySource());
		mailSubject = mailSubject.replaceAll("%%BRANCH%%", ddpDmsDocsDetail.getDddBranchSource());
		mailSubject = mailSubject.replaceAll("%%DEPARTMENT%%", ddpDmsDocsDetail.getDddDeptSource());
				
		String mailBody = null;
		try {
			mailBody =	env.getProperty("multiaedmail.body."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType()+"."+ddpRuleDetail.getRdtPartyId());
		} catch(Exception ex) {
			
		}
		if (mailBody == null || mailBody.isEmpty()) {
			try {
				mailBody =	env.getProperty("multiaedmail.body."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
			} catch(Exception ex) {
				
			}
		}
		if (mailBody == null || mailBody.isEmpty()) {
			try {
			 mailBody = env.getProperty("multiaedmail.body."+ddpDmsDocsDetail.getDddCompanySource());
			} catch (Exception ex) {
				
			}
		}
		if(mailBody == null || mailBody.isEmpty()) {
			try {
				mailBody = env.getProperty("multiaedmail.body");
			} catch (Exception ex){
				mailBody = "<P><h2><font color='orange'>Attachment Details</font></h2><table><tr style='background-color:rgb(244,160,65);'>"
						+ "<th>Attachment</th><th>OurReference</th><th>Consignment ID</th><th>Document Reference</th><th>Document Type</th>"
						+ "</tr>%%DETAILS%%</table><br/><span>please do not respond to this mail, as this account is intended for outbound emails only</span>"
						+ "<br/><br/></P><IMG SRC='http://www.latc.la/advhtml_upload/agi_gil-home-logo.png' ALT='GIL'>";
			}
		}
		
		String documentType = new String(triggeringDocType);
		try { 
			String docType = env.getProperty("multiaedmail.DOCTYPE."+triggeringDocType);
			if (docType != null && docType.length() > 0)
				documentType = docType;
		} catch (Exception ex) {
			
		}
		mailBody = mailBody.replaceAll("%%DYNAMICNAME%%", env.getProperty("multiaedmail."+triggerSetup.getEtrTriggerName()));
		mailBody = mailBody.replaceAll("%%JOBNUMBER%%", uniqueNumber);
		mailBody = mailBody.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
		
		mailBody = mailBody.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
		mailBody = mailBody.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
		mailBody = mailBody.replaceAll("%%DOCREF%%", docRef);
		mailBody = mailBody.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef()); 
		mailBody = mailBody.replaceAll("%%JOBNO%%", ddpDmsDocsDetail.getDddJobNumber());
		mailBody = mailBody.replaceAll("%%REFERENCE%%", ddpDmsDocsDetail.getDddJobNumber());
		mailBody = mailBody.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
		mailBody = mailBody.replaceAll("%%COMPANY%%", ddpDmsDocsDetail.getDddCompanySource());
		mailBody = mailBody.replaceAll("%%BRANCH%%", ddpDmsDocsDetail.getDddBranchSource());
		mailBody = mailBody.replaceAll("%%DEPARTMENT%%", ddpDmsDocsDetail.getDddDeptSource());
		mailSubject = mailSubject.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
		
		mailBody = mailBody.replaceAll("%%DOCUMENTTYE%%", documentType);
		mailBody = mailBody.replaceAll("%%RULEID%%", ddpRuleDetail.getRdtRuleId().getRulId()+"");
		mailBody = mailBody.replaceAll("%%DETAILS%%", mailBodyContent);
		if (mailBody.contains("%%TRACKURL%%")) {
			mailBody = mailBody.replaceAll("%%TRACKURL%%",env.getProperty("tracking.url."+env.getProperty("mail.evn")));
		}
		
		boolean isBranchCCRequire = false;
		String brachCCMailAddress = null;
		String flagbranchCCMailid = env.getProperty("multiaedmail.cc.userMailId."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
		if(flagbranchCCMailid == null || flagbranchCCMailid.equals(""))
			flagbranchCCMailid = env.getProperty("multiaedmail.cc.userMailId."+ddpDmsDocsDetail.getDddCompanySource());
		if(flagbranchCCMailid == null || flagbranchCCMailid.equals(""))
			flagbranchCCMailid = env.getProperty("multiaedmail.cc.userMailId");
		if(flagbranchCCMailid.equalsIgnoreCase("Y") && ddpDmsDocsDetail.getDddGeneratingSystem().equalsIgnoreCase("Control"))
			isBranchCCRequire = true;
		//In feature need fetch from control for attribute CCMAILID, add in below condition. 
		//Below condition is written to avoid control db hitting ever time.
		if (mailBody.contains("%%CONTROLUSERNAME%%") || mailBody.contains("%%MAILID%%") || isBranchCCRequire) {
			
			String controlUID=ddpDmsDocsDetail.getDddUserId().trim();
			String controlQuery = env.getProperty("control.query."+env.getProperty("mail.evn"));
			controlQuery = controlQuery.replaceAll("%%CONTROLUSERID%%", controlUID);
			
//			List<Map<String,Object>> controlResultSet = controlJdbcTemplate.queryForList(controlQuery, new Object[]{controlUID});
			List<Map<String,Object>> controlResultSet = commonUtil.getControlMetaData(controlQuery, 0, 2, 3);
			if (controlResultSet.size() != 0) {
				for (Map<String, Object> map : controlResultSet) {
					String userName = (map.get("UDSDES") == null ? "" : (String)map.get("UDSDES")) ;
					String controlUserMailID =( map.get("UDEADD") == null ? "" : (String)map.get("UDEADD"));
					brachCCMailAddress = controlUserMailID;
					mailBody = mailBody.replaceAll("%%CONTROLUSERNAME%%", userName.trim());
					mailBody = mailBody.replaceAll("%%MAILID%%", controlUserMailID.trim());
				}
			} else {
				mailBody = mailBody.replaceAll("%%CONTROLUSERNAME%%", "");
				mailBody = mailBody.replaceAll("%%MAILID%%", "");
			}
			
		}
				
		String mailFrom=env.getProperty("mail.fromAddress");
		int attachFlag = 0;
		String commProtocol = ddpRuleDetail.getRdtCommunicationId().getCmsCommunicationProtocol();
		
		if(commProtocol.equalsIgnoreCase("MSMTP"))
		{
			String smtpAddres = env.getProperty("mail.smtpAddress");
			List<DdpMultiEmails> ddpMultiEmails = commonUtil.getMultiEmailsByCmsID(ddpRuleDetail.getRdtCommunicationId().getCmsCommunicationId());
			attachFlag = taskUtil.sendMailForMultiAedToMultiple(ddpMultiEmails,ddpDmsDocsDetail, tmpFile, smtpAddres, mailSubject, mailBody, mailFrom, ddpRuleDetail.getRdtRuleType(), brachCCMailAddress);
		}
		
		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
			//This should be get from the configuration file or from table
//			String smtpAddres ="10.20.1.233";
			String smtpAddres = env.getProperty("mail.smtpAddress");
			logger.info("RuleJob.communication() : calling sendMail() ");
			attachFlag = taskUtil.sendMailForMultiAed(ddpCommEmails, tmpFile, smtpAddres, mailSubject, mailBody, mailFrom,  ddpRuleDetail.getRdtRuleType(),brachCCMailAddress);
		}
		
		logger.info("DdpMultiAedRuleJob.sendMailUsingCommunicationSetup() is Successfully compeleted - AttachFlag : "+attachFlag);
		
		return attachFlag;
	}
	
	
}
