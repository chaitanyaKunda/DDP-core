package com.agility.ddp.core.quartz;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.net.ftp.FTPClient;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.components.DdpDFCClientComponent;
import com.agility.ddp.core.components.DdpFTPClient;
import com.agility.ddp.core.components.DdpSFTPClient;
import com.agility.ddp.core.components.DdpTransferFactory;
import com.agility.ddp.core.components.DdpTransferObject;
import com.agility.ddp.core.task.DdpSchedulerJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.NamingConventionUtil;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommFtpService;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.agility.ddp.data.domain.DdpCommUncService;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.agility.ddp.data.domain.DdpCommunicationSetupService;
import com.agility.ddp.data.domain.DdpCompressionSetup;
import com.agility.ddp.data.domain.DdpDocnameConv;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportMissingDocsService;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpExportRuleService;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.agility.ddp.data.domain.DdpExportSuccessReportService;
import com.agility.ddp.data.domain.DdpNotification;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfSession;

@Configuration
//@PropertySource({"file:///E:/DDPConfig/export.properties","file:///E:/DDPConfig/ddp.properties","file:///E:/DDPConfig/custom.properties","file:///E:/DDPConfig/mail.properties"})
public class DdpWDRuleSchedulerJob extends QuartzJobBean implements DdpSchedulerJob
{

	private static final Logger logger = LoggerFactory.getLogger(DdpWDRuleSchedulerJob.class);
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private DdpExportRuleService ddpExportRuleService;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private DdpCommunicationSetupService ddpCommunicationSetupService;
	
	@Autowired
	private DdpCommFtpService ddpCommFtpService;
	
	@Autowired
	private DdpCommUncService ddpCommUncService;
	
//	@Value( "${ddp.export.folder}" )
//    String tempFilePath;

	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private DdpRuleDetailService ddpRuleDetailService;
	
	
	//@Autowired
	//private Environment env;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private JdbcTemplate controlJdbcTemplate;
	
	@Autowired
	private DdpExportMissingDocsService ddpExportMissingDocsService;
	
	@Autowired
	private CommonUtil commonUtil;
	
	 @Autowired
	 private TaskUtil taskUtil;
	 
	 @Autowired
	 private DdpSchedulerService ddpSchedulerService;
	 
	 @Autowired
	 private DdpExportSuccessReportService ddpExportSuccessReportService;
	 
	 @Autowired
	 private DdpTransferFactory ddpTransferFactory;
	
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("DdpWDRuleSchedulerJob.executeInternal() method invoked.");
		String strJobName = context.getJobDetail().getKey().getName();
		String strJobGroup = context.getJobDetail().getKey().getGroup();
		logger.info("DdpWDRuleSchedulerJob.executeInternal() : JobeName - " + strJobName + " and Group - "+strJobGroup+" executing at " + new Date());
		logger.info("DdpWDRuleSchedulerJob.executeInternal() executed successfully.");
	}
	
	/**
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerReport(Object[] ddpSchedulerObj) {
		
		logger.info("DdpWDRuleSchedulerJob.initiateSchedulerReport(Object[] ddpSchedulerObj) method invoked.");
		
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		File generatedReportFolder = null;
		if (ddpScheduler.getSchStatus() == 0 && ddpScheduler.getSchReportFrequency() != null && !ddpScheduler.getSchReportFrequency().isEmpty() && !ddpScheduler.getSchReportFrequency().equalsIgnoreCase("none")) {
			try {
				String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchReportFrequency());
				Calendar startCalendar = Calendar.getInstance();
				Calendar endCalendar = Calendar.getInstance();  
		   		//Call below method to get date range - This needs to be implemented
		   		Calendar startDate = SchedulerJobUtil.getQueryStartDate(strFeq, startCalendar,ddpScheduler.getSchReportFrequency());
		   		Calendar endDate = endCalendar;
		   		generatedReportFolder = commonUtil.generateReports(ddpScheduler, startDate, endDate, 2,Constant.EXECUTION_STATUS_SUCCESS);
			} finally {
				//commonUtil.removeExportSetupThread("RuleByQuery - "+ddpScheduler.getSchRuleCategory()+"-SchedulerReport :"+ddpScheduler.getSchId());
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
	public void initiateSchedulerJob(Object[] ddpSchedulerObj) 
	{
		logger.info("DdpWDRuleShedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) method invoked.");
		
		DdpScheduler ddpScheduler = (DdpScheduler)ddpSchedulerObj[0];
		
				
		logger.debug("############ SCHEDULER RUNNING FOR "+ddpScheduler.getSchId()+"#############");
		
		executeSchedulerJob(ddpScheduler);
		logger.info("DdpWDRuleShedulerJob.initiateSchedulerJob(Object[] ddpSchedulerObj) executed successfully.");
	}
	
	
	/**
	 * Method used for executing the SchedulerJob.
	 * 
	 * @param ddpScheduler
	 * @return
	 */
	private boolean executeSchedulerJob(DdpScheduler ddpScheduler) {

		boolean isSchedulerExuected = false;
		logger.info("DdpWDRuleShedulerJob.executeSchedulerJob(DdpScheduler ddpScheduler) method invoked.");
		System.out.println(" Scheduler ID : "+ddpScheduler.getSchId());
		
		//Check for SCH_STATUS. If active = 0, process further and if inactive = 1 simply return the job without any further execution
		if(ddpScheduler.getSchStatus() != 0 )
		{
			logger.info("DdpRuleSchedulerJob.executeShedulerJob(DdpScheduler ddpScheduler) - SCHEDULER [{}] is NOT ACTIVE.",ddpScheduler.getSchId());
			return isSchedulerExuected;
		}
			
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
			
			
			
			//Fetch matched rule detail id (RDT_ID) from DDP_RULE_DETAIL table for give Scheduler ID (SCH_ID)
        	//List<Integer> rdtIdList = schedulerJob.getMatchedSchdIDAndRuleDetailForExport(ddpScheduler.getSchId());
        	
        	//List<DdpCategorizedDocs> ddpCategorizedDocsList = new ArrayList<DdpCategorizedDocs>();
			//;
        	//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
    		String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchCronExpressions());
        
    		//Call below method to get date range - This needs to be implemented
    		Calendar queryStartDate = SchedulerJobUtil.getQueryStartDate(strFeq, startCalendar,ddpScheduler.getSchCronExpressions());
    		Calendar queryEndDate = endCalendar;
    		
    		//isSchedulerExuected = performSchedulerAction(strFeq,queryStartDate,queryEndDate,ddpScheduler);
        	/*if (ddpCategorizedDocsList == null || ddpCategorizedDocsList.size() == 0) {
        		logger.info("DdpRuleSchedulerJob.executeSchedulerJob(DdpScheduler ddpScheduler) - SCHEDULER [{}] DdpCategorizedDoc are empty for this scheduler id.",ddpScheduler.getSchId());
        		return isSchedulerExuected;
        	}*/
    		
    		performGeneralSchedulerProcess(ddpScheduler, queryStartDate, queryEndDate,1,today,strFeq);
		 } else {
	       	logger.info("DdpWDRuleSchedulerJob.executeShedulerJob(Object[] ddpSchedulerObj) - SCHEDULER [{}] STRAT DATE is NOT reached.",ddpScheduler.getSchId());
			return isSchedulerExuected;
	      }
		
		
		
		logger.info("DdpWDRuleShedulerJob.executeSchedulerJob(DdpScheduler ddpScheduler) executed successfully.");
		return isSchedulerExuected;
	}
	
	
	/**
	 *  
	 * @param ddpScheduler
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService :  1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
	 * @return
	 */
	private boolean performGeneralSchedulerProcess(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,int typeOfService,Calendar presentDate,String strFeq) {
		
		logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Invoked successfully");
		
		boolean isProcessed = false;
		List<DdpExportMissingDocs> exportList = new ArrayList<DdpExportMissingDocs>();
		
		DdpExportRule exportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),"WD");
		if (exportRule.getExpStatus() != 0) {
			logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Export rule status is not inactive for export rule id : "+exportRule.getExpRuleId());
			return isProcessed;
		}
		
		if (typeOfService == 1) {
			
			DdpScheduler scheduler = ddpSchedulerService.findDdpScheduler(ddpScheduler.getSchId());
			
			if (scheduler == null || scheduler.getSchStatus() != 0) {
				logger.info("DdpBackUpDocumentProcess.executeJob(DdpScheduler ddpScheduler,Calendar currentDate,String appName) - DdpExportRule is null for scheduler id : "+ddpScheduler.getSchId()+ ". So Process is stopped.");
				return isProcessed;
			}
			
			boolean isSchLastRunStatus = false;
			if (scheduler.getSchLastSuccessRun() == null) { 
				queryStartDate = exportRule.getExpActivationDate();
				isSchLastRunStatus = true;
			} else 
				queryStartDate = scheduler.getSchLastSuccessRun();
			
			if (scheduler.getSchDelayCount() != null) {
				
				String dateFreq = SchedulerJobUtil.getSchFreq(scheduler.getSchCronExpressions());
				if (dateFreq.equalsIgnoreCase("monthly")) {
					queryEndDate.add(Calendar.MONTH,-scheduler.getSchDelayCount());
					if (isSchLastRunStatus)
						queryStartDate.add(Calendar.MONTH, -scheduler.getSchDelayCount());
				} else if (dateFreq.equalsIgnoreCase("weekly")) {
					queryEndDate.add(Calendar.WEEK_OF_YEAR, -scheduler.getSchDelayCount());
					if (isSchLastRunStatus)
						queryStartDate.add(Calendar.WEEK_OF_YEAR, -scheduler.getSchDelayCount());
				} else if (dateFreq.equalsIgnoreCase("daily")) {
					queryEndDate.add(Calendar.DAY_OF_YEAR, -scheduler.getSchDelayCount());
					if (isSchLastRunStatus)
						queryStartDate.add(Calendar.DAY_OF_YEAR, -scheduler.getSchDelayCount());
				} else if (dateFreq.equalsIgnoreCase("hourly")) {
					queryEndDate.add(Calendar.HOUR_OF_DAY, -scheduler.getSchDelayCount());
					if (isSchLastRunStatus)
						queryStartDate.add(Calendar.HOUR_OF_DAY, -scheduler.getSchDelayCount());
				}
			}
			
		}
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String query = env.getProperty("export.rule.WD.customQuery");
	//	queryStartDate.add(Calendar.MONTH, -2);
		//queryStartDate.add(Calendar.DAY_OF_MONTH, -12);
		query  = query.replaceAll("%%STARTDATE%%", dateFormat.format(queryStartDate.getTime()));
		query = query.replaceAll("%%ENDDATE%%",dateFormat.format(queryEndDate.getTime()));
		
		logger.info("WD Query : "+query);
		try {
			List<Map<String, Object>> rowList = controlJdbcTemplate.queryForList(query);
			
			if (!rowList.isEmpty()) {
				
				for (Map<String, Object> map : rowList) {
					
					DdpExportMissingDocs missingDocs = new DdpExportMissingDocs();
					if (map.get("Entry Type") != null)
						missingDocs.setMisEntryType(map.get("Entry Type").toString().trim());
					//else
						//missingDocs.setMisEntryType();
					if (map.get("Filer Code") != null)
						missingDocs.setMisFilerCode(map.get("Filer Code").toString().trim());
					if (map.get("Entry #") != null)
						missingDocs.setMisEntryNo(map.get("Entry #").toString().trim());
					if (map.get("Entry #Check Digit") != null)
						missingDocs.setMisCheckDigit(map.get("Entry #Check Digit").toString().trim());
					if (map.get("Master") != null)
						missingDocs.setMisMasterJob(map.get("Master").toString().trim());
					if (map.get("Job#") != null)
						missingDocs.setMisJobNumber(map.get("Job#").toString().trim());
					if (map.get("HAWB") != null)
						missingDocs.setMisConsignmentId(map.get("HAWB").toString().trim());
					if (map.get("Company") != null)
						missingDocs.setMisCompany(map.get("Company").toString().trim());
					if (map.get("Branch") != null)
						missingDocs.setMisBranch(map.get("Branch").toString().trim());
					if (map.get("File#") != null)
						missingDocs.setMisRobjectName(map.get("File#").toString().trim());
					//missingDocs.setMisCreatedDate(map.get("Release Date"));
					missingDocs.setMisAppName("WD");
					missingDocs.setMisStatus(0);
					missingDocs.setMisExpRuleId(exportRule.getExpRuleId());
					missingDocs.setMisCreatedDate(new GregorianCalendar());
					
					exportList.add(missingDocs);
					/*for (String key : map.keySet()) {
						System.out.print( key +" : key : "+map.get(key)+"; ");
					}
					System.out.println();*/
				}
			}
		} catch (Exception ex) {
			logger.error("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Custom Query exception", ex);
			sendMailForEmptyRecords(exportRule, "WD", typeOfService, queryEndDate, queryStartDate, presentDate,"customquery",null);
			return isProcessed;
		}
		logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Total size of the export missing document : "+exportList.size());
		
		if (typeOfService == 1) {
			if (exportList.size() > 0) {
			for (DdpExportMissingDocs docs : exportList)
				ddpExportMissingDocsService.saveDdpExportMissingDocs(docs);
			
			}
			
			ddpScheduler.setSchLastRun(presentDate);
			ddpScheduler.setSchLastSuccessRun(queryEndDate);
			ddpSchedulerService.updateDdpScheduler(ddpScheduler);
		}
		
		if (exportList.size() > 0) {
			isProcessed = runGeneralSchedulerJob(exportRule,exportList , queryStartDate, queryEndDate, typeOfService,"WD",presentDate,null);
		} else {
			sendMailForEmptyRecords(exportRule, "WD", typeOfService, queryEndDate, queryStartDate, presentDate,"empty",null);
			logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Empty result for exportList(DdpExportMissingDocs) ");
		}
		return isProcessed;
	}
	
	
	/**
	 *  
	 * @param ddpScheduler
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param typeOfService :  1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
	 * @return
	 */
	private boolean onDemandPerformGeneralSchedulerProcess(DdpScheduler ddpScheduler,Calendar queryStartDate,Calendar queryEndDate,
			int typeOfService,Calendar presentDate,String strFeq,String jobNumbers,String consignmentIDs,String docRefs) {
		
		logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Invoked successfully");
		
		boolean isProcessed = false;
		String dynamicValues = "";
		List<DdpExportMissingDocs> exportList = new ArrayList<DdpExportMissingDocs>();
		
		DdpExportRule exportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),"WD");
		if (exportRule.getExpStatus() != 0) {
			logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Export rule status is not inactive for export rule id : "+exportRule.getExpRuleId());
			return isProcessed;
		}
		
				
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String query = env.getProperty("export.rule.WD.customQuery."+env.getProperty(typeOfService+""));
				
		if (typeOfService == 4 && !jobNumbers.isEmpty()) { 
			query = query.replaceAll("%%JOBNUMBER%%",commonUtil.joinString(jobNumbers.split(","), "'", "'", ","));
			dynamicValues = jobNumbers;
		} else if (typeOfService == 5 && !consignmentIDs.isEmpty()) {
			query = query.replaceAll("%%CONSIGNMENTID%%",commonUtil.joinString(consignmentIDs.split(","), "'", "'", ","));
			dynamicValues = consignmentIDs;
		} else if (typeOfService == 6 && !docRefs.isEmpty()) {
			query = query.replaceAll("%%DOCREFS%%",commonUtil.joinString(docRefs.split(","), "'", "'", ","));
			dynamicValues = docRefs;
		}
		
		query  = query.replaceAll("%%STARTDATE%%", dateFormat.format(queryStartDate.getTime()));
		query = query.replaceAll("%%ENDDATE%%",dateFormat.format(queryEndDate.getTime()));
		
		logger.info("WD Query : "+query);
		try {
			List<Map<String, Object>> rowList = controlJdbcTemplate.queryForList(query);
			
			if (!rowList.isEmpty()) {
				
				for (Map<String, Object> map : rowList) {
					
					DdpExportMissingDocs missingDocs = new DdpExportMissingDocs();
					if (map.get("Entry Type") != null)
						missingDocs.setMisEntryType(map.get("Entry Type").toString().trim());
					//else
						//missingDocs.setMisEntryType();
					if (map.get("Filer Code") != null)
						missingDocs.setMisFilerCode(map.get("Filer Code").toString().trim());
					if (map.get("Entry #") != null)
						missingDocs.setMisEntryNo(map.get("Entry #").toString().trim());
					if (map.get("Entry #Check Digit") != null)
						missingDocs.setMisCheckDigit(map.get("Entry #Check Digit").toString().trim());
					if (map.get("Master") != null)
						missingDocs.setMisMasterJob(map.get("Master").toString().trim());
					if (map.get("Job#") != null)
						missingDocs.setMisJobNumber(map.get("Job#").toString().trim());
					if (map.get("HAWB") != null)
						missingDocs.setMisConsignmentId(map.get("HAWB").toString().trim());
					if (map.get("Company") != null)
						missingDocs.setMisCompany(map.get("Company").toString().trim());
					if (map.get("Branch") != null)
						missingDocs.setMisBranch(map.get("Branch").toString().trim());
					if (map.get("File#") != null)
						missingDocs.setMisRobjectName(map.get("File#").toString().trim());
					//missingDocs.setMisCreatedDate(map.get("Release Date"));
					missingDocs.setMisAppName("WD");
					missingDocs.setMisStatus(0);
					missingDocs.setMisExpRuleId(exportRule.getExpRuleId());
					missingDocs.setMisCreatedDate(new GregorianCalendar());
					
					exportList.add(missingDocs);
					
				}
			}
		} catch (Exception ex) {
			logger.error("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Custom Query exception", ex);
			sendMailForEmptyRecords(exportRule, "WD", typeOfService, queryEndDate, queryStartDate, presentDate,"customquery",dynamicValues);
			return isProcessed;
		}
		logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Total size of the export missing document : "+exportList.size());
				
		if (exportList.size() > 0) {
			isProcessed = runGeneralSchedulerJob(exportRule,exportList , queryStartDate, queryEndDate, typeOfService,"WD",presentDate,dynamicValues);
		} else {
			sendMailForEmptyRecords(exportRule, "WD", typeOfService, queryEndDate, queryStartDate, presentDate,"empty",dynamicValues);
			logger.info("DdpWDRuleShedulerJob.perfomGeneralSchedulerProcess() - Empty result for exportList(DdpExportMissingDocs) ");
		}
		return isProcessed;
	}
	
	/**
	 * Method used for the running the scheduler job.
	 * 
	 * @param ddpExportRule
	 * @param missingDocs
	 * @param fromDate
	 * @param toDate
	 * @param typeOfService
	 * @param appName
	 * @param presentDate
	 * @return
	 */
	private boolean runGeneralSchedulerJob(DdpExportRule ddpExportRule,List<DdpExportMissingDocs> missingDocs,Calendar fromDate,Calendar toDate,
			int typeOfService,String appName,Calendar presentDate,String dynamicValues) {
		
		logger.info("DdpWDRuleSchedulerJob.runGeneralSchedulerJob() - Invoked sccussfully");
		boolean isExecuted = false;
		IDfSession session = null;
		
		//String is the EntryNumber & group of DdpExportMissingDocs
		Map<String, List<DdpExportMissingDocs>> entryNumberMap = new HashMap<String, List<DdpExportMissingDocs>>();
		Map<String,List<DdpExportMissingDocs>> missDocsMap = new HashMap<String, List<DdpExportMissingDocs>>();
		Map<String,String> exportMissDocsMap = new HashMap<String, String>();
		
		Set<String> importFTZDocs = new HashSet<String>();
		Set<String> importRegularDocs = new HashSet<String>();
		Set<String> exportDocs = new HashSet<String>();

		Set<String> mandImportFTZDocs = new HashSet<String>();
	//	Set<String> optionalImportFTZDocs = new HashSet<String>();
		Set<String> mandImportRegularDocs = new HashSet<String>();
		//Set<String> optionalImportRegularDocs = new HashSet<String>();
		Set<String> mandExportDocs = new HashSet<String>();
		//Set<String> optionalExportDocs = new HashSet<String>();
		
		try {
			
			groupEntryNumbers(missingDocs, entryNumberMap);
			
			//System.out.println("Size of the entry number map : "+entryNumberMap.size());
			String importFTZ = env.getProperty("export.rule."+appName+".documentType.import-ftz");
			String importRegular = env.getProperty("export.rule."+appName+".documentType.import-reqular");
			String exports = env.getProperty("export.rule."+appName+".documentType.exports");
			updateDocumentTypesInList(importFTZ, importFTZDocs, mandImportFTZDocs );
			updateDocumentTypesInList(importRegular, importRegularDocs, mandImportRegularDocs);
			updateDocumentTypesInList(exports, exportDocs, mandExportDocs);
			
			//System.out.println("Total size of the document types : importRegularDocs size: "+importRegularDocs.size());
			
			session = ddpDFCClientComponent.beginSession();
			
			if (session == null) {
				sendMailForEmptyRecords(ddpExportRule, "WD", typeOfService, toDate, fromDate, presentDate,"session",dynamicValues);
				logger.info("DdpWDRuleSchedulerJob.runGeneralSchedulerJob() - Session is empty. Not able create session");
				return isExecuted;
			}
			
			for (String entryNumber : entryNumberMap.keySet()) {
				
				List<DdpExportMissingDocs> expMissDocs = entryNumberMap.get(entryNumber);
				String entryType = expMissDocs.get(0).getMisEntryType();
				String documentTypes = null;
				
				if (entryType == null) {
					documentTypes = joinString(exportDocs, "'", "'", ",");
				} else if (entryType.equals("6")) {
					documentTypes = joinString(importFTZDocs, "'", "'", ",");
				} else if (!entryType.equals("6")) {
					documentTypes = joinString(importRegularDocs, "'", "'", ",");
				}
				 
				
				String custQuery = env.getProperty("export.rule."+appName+"."+ddpExportRule.getExpSchedulerId().getSchBatchingCriteria()+".customQuery");
				custQuery = custQuery.replaceAll("%%DOCUMENTTYPE%%", documentTypes);
				
				if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("jobNumber")) {
					custQuery = custQuery.replaceAll("%%NUMBER%%", joinExportDocs(expMissDocs, "'","'", ",",ddpExportRule.getExpSchedulerId().getSchBatchingCriteria()));
				} else if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("consignmentID")) {
					custQuery = custQuery.replaceAll("%%NUMBER%%",joinExportDocs(expMissDocs, "'","'", ",",ddpExportRule.getExpSchedulerId().getSchBatchingCriteria()));
				} else if (ddpExportRule.getExpSchedulerId().getSchBatchingCriteria().equals("entryNo")) {
					custQuery = custQuery.replaceAll("%%NUMBER%%", joinExportDocs(expMissDocs, "'","'", ",",ddpExportRule.getExpSchedulerId().getSchBatchingCriteria()));
				}
				logger.info("WD : Custom query : "+custQuery);
				IDfCollection idfCollection = commonUtil.getIDFCollectionDetails(custQuery, session);
				List<DdpExportMissingDocs> subMisDocs = commonUtil.constructMissingDocs(idfCollection, "WD", ddpExportRule.getExpRuleId());
				String missingDocumentTypes = null;
				if (entryType == null) 
					missingDocumentTypes = checkMissingDocuments(subMisDocs, mandExportDocs,"exports");
				else if (entryType.equals("6"))
					missingDocumentTypes = checkMissingDocuments(subMisDocs, mandImportFTZDocs,"import-ftz");
				else if (!entryType.equals("6")) 
					missingDocumentTypes = checkMissingDocuments(subMisDocs, mandImportRegularDocs,"import-reqular");
				
				if (missingDocumentTypes == null)
					missDocsMap.put(entryNumber, subMisDocs);
				else
					exportMissDocsMap.put(entryNumber, missingDocumentTypes);
				
				//System.out.println("Total records for the entry number : "+entryNumber+" : Size : "+missDocsMap.get(entryNumber));
				
				
			}
			isExecuted = performTransfer(ddpExportRule, fromDate, toDate, typeOfService, appName, entryNumberMap, missDocsMap, exportMissDocsMap, session,presentDate,dynamicValues);
			logger.info("DdpWDRuleSchedulerJob.runGeneralSchedulerJob() - Is Transfered .. : "+isExecuted);
		} catch (Exception ex) {
			logger.error("DdpWDRuleSchedulerJob.runGeneralSchedulerJob() ", ex);
			sendMailForEmptyRecords(ddpExportRule, "WD", typeOfService, toDate, fromDate, presentDate,"customquery",dynamicValues);
		} finally {
			if (session != null)
				ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
		}
				
		return isExecuted;
	}
	
	private String checkMissingDocuments(List<DdpExportMissingDocs> list,Set<String> mandatoryDocs,String type) {
		
		String documentTypes = "";
		List<String> docTypes = new ArrayList<String>();
		
		for (DdpExportMissingDocs docs : list) {
			if (docs.getMisDocType().equalsIgnoreCase("ENTRY")) 
				docTypes.add(docs.getMisDocType()+":"+docs.getMisRobjectName().substring(0, 4));
			else if (type.equalsIgnoreCase("exports") && docs.getMisDocType().equalsIgnoreCase("CUSTOMSRPT")) 
				docTypes.add(docs.getMisDocType()+":"+docs.getMisRobjectName().substring(0, 7));
			else 
				docTypes.add(docs.getMisDocType());
		}
		
		for (String mandatoryDocumentType : mandatoryDocs) {
			
			if (!docTypes.contains(mandatoryDocumentType)) 
				documentTypes = documentTypes + mandatoryDocumentType +",";
		}
		
		return documentTypes.length() > 0 ? documentTypes.substring(0, documentTypes.length()-1) : null;
	}
	
	/**
	 * Method used for transferring the files in to provided locations.
	 * 
	 * @param ddpExportRule
	 * @param fromDate
	 * @param toDate
	 * @param typeOfService
	 * @param appName
	 * @param entryNumberMap
	 * @param missDocsMap
	 * @param exportMissDocsMap
	 * @param session
	 * @param presentDate
	 * @return
	 */
	private boolean performTransfer(DdpExportRule ddpExportRule,Calendar fromDate,Calendar toDate,int typeOfService,String appName,
			Map<String, List<DdpExportMissingDocs>> entryNumberMap,Map<String,List<DdpExportMissingDocs>> missDocsMap,Map<String,String> exportMissDocsMap,
			IDfSession session,Calendar presentDate,String dynamicValues) {
		
		logger.info("DdpWDRuleSchedulerJob.performTransfer() - Invoked scussfully");
		boolean isTransfered = false;
		File tmpFile = null;
		DdpSFTPClient sftpClient = null;
		//ChannelSftp sftpChannel = null;
		DdpCompressionSetup compressionSetup =  ddpExportRule.getExpCompressionId();
	    String tempFilePath = env.getProperty("ddp.export.folder");
	    List<DdpExportSuccessReport> exportedReports = new ArrayList<DdpExportSuccessReport>();
		if (compressionSetup != null) {
			try {
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
				
				List<String> subMissingRecords = new ArrayList<String>();
				DdpTransferObject ddpTransferObject = null;
				DdpSFTPClient ddpSFTPClient = null;
				List<DdpTransferObject> transferObjects = ddpTransferFactory.constructTransferObject(commSetup);
				
				for (DdpTransferObject transferObject : transferObjects) {
					if (transferObject.isConnected()) {
						ddpTransferObject = transferObject;
						if (ddpTransferObject.getTypeOfConnection().equals("sftp"))
							ddpSFTPClient = (DdpSFTPClient) ddpTransferObject.getDdpTransferClient();
						break;
					}
				}
				
				if (ddpTransferObject == null) {
					sendMailForConnectionIssue(ddpExportRule, appName, typeOfService,toDate, fromDate, presentDate,transferObjects,dynamicValues);
					return isTransfered;
				}
				
				for (String entryNumber : missDocsMap.keySet()) {
					
					//List<DdpExportMissingDocs> missingDocs = entryNumberMap.get(entryNumber);
					List<DdpExportMissingDocs> dfcDocs = missDocsMap.get(entryNumber);
					
					List<String> fileNameList = new ArrayList<String>();
					commonUtil.readFileNamesFromLocation(sourceFolder, ddpTransferObject.isFTPType(), ddpTransferObject.getFtpClient(), ddpTransferObject.getDestLocation(), ddpTransferObject.getSmbFile(), ddpTransferObject.getChannelSftp(), fileNameList,sftpClient,ddpTransferObject.getFtpDetails());
					
					
					if (compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("merge")) {
						
						DdpExportMissingDocs exportDocs = dfcDocs.get(0);
						List<String> rObjectList = new ArrayList<String>();
						String entryType = exportDocs.getMisEntryType();
						if (entryType == null) 
							rObjectList = getRObjectIDOfExports(dfcDocs, appName);
						else if (entryType.equals("6"))
							rObjectList = getRObjectIDOfImportsFTZ(dfcDocs, appName);
						else if (!entryType.equals("6")) 
							rObjectList = getRObjectIDOfImportsReqular(dfcDocs, appName);
						Map<String,String> loadNumberMap = commonUtil.getLoadNumberMapDetails(namingConvention, exportDocs, appName, env);
						String fileName =  commonUtil.getDocumentFileName(appName+"_merge.pdf", namingConvention, exportDocs, exportDocs.getMisEntryType(),fileNameList,loadNumberMap);
						String mergeID = commonUtil.performMergeOperation(rObjectList, fileName, env.getProperty("ddp.vdLocation"), env.getProperty("ddp.objectType"), session,appName);
						if (mergeID == null) {
							logger.info("DdpWDRuleSchedulerJob.performTransfer() - Unable to merge the documents");
							exportMissDocsMap.put(entryNumber, exportDocs.getMisDocType());
							 subMissingRecords.add(entryNumber);
							continue;
						}
						boolean isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(mergeID, sourceFolder, fileName, session);
						if (!isDownload)logger.info("DdpWDRuleSchedulerJob.performTransfer() - Unable to download the documents robjectid is "+mergeID);
						else{
							SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
							mailBody.append(format.format(new Date())+",");
							format.applyLocalizedPattern("HH:mm:ss");
							long fileSize = FileUtils.findSizeofFileInKB(sourceFolder+"/"+fileName);
							//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+exportDocs.getMisCheckDigit()+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
							mailBody.append(format.format(new Date())+",receive,success,"+fileSize+",00:00:01,"+fileName+"\n");
							if (typeOfService == 1 || typeOfService == 3) {
								exportedReports.add(commonUtil.constructExportReportDomainObject(exportDocs, fileSize, fileName, typeOfService));
							}
						}
					} else {
						boolean isDownload = false;
						for (DdpExportMissingDocs miss : dfcDocs) {
							Map<String,String> loadNumberMap = commonUtil.getLoadNumberMapDetails(namingConvention, miss, appName, env);
							isDownload = performDownloadOperation(miss.getMisRobjectName(), miss.getMisRObjectId(), namingConvention, miss, session, miss.getMisEntryType(),mailBody,fileNameList,sourceFolder,loadNumberMap,typeOfService,exportedReports);
							  if (!isDownload) {
								//	missingRecords.put(rObjectId, exportDocs);
								  exportMissDocsMap.put(entryNumber, miss.getMisDocType());
								  subMissingRecords.add(entryNumber);
									break;
							 }
						}
						
						
					}
					
				}
				
				for (String entryNum : subMissingRecords) {
					missDocsMap.remove(entryNum);
				}
				
				if (missDocsMap.size() > 0 && compressionSetup.getCtsCompressionLevel().equalsIgnoreCase("exportAsZip")) {
					
					String zipPath = tempFolderPath +"/ZIP_"+appName;
					File zipPathFile = FileUtils.createFolder(zipPath);
					FileUtils.zipFiles(zipPath+"/"+appName+"_"+typeOfService+"_" +dateFormat.format(toDate.getTime())+".zip", sourceFolder);
					endSourceFolderFile = zipPathFile;
					
				}
				
				boolean isFilesProcessed = false;
				List<DdpTransferObject> transferList = new ArrayList<DdpTransferObject>();
				if (missDocsMap.size() > 0) { 
					for (DdpTransferObject transferObject : transferObjects) {
						
						if (transferObject.isConnected()) {
							
							boolean isTransferd = ddpTransferFactory.transferFilesUsingProtocol(transferObject, endSourceFolderFile,null,"WD",fromDate,toDate,"ruleByQuery."+typeOfService);
							 
							if (isTransferd) {
								isFilesProcessed = true;
								transferList.add(transferObject);
							}
							
							if (!isTransferd) {
								List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
								list.add(transferObject);
								sendMailForConnectionIssue(ddpExportRule, appName, typeOfService, toDate, fromDate, presentDate,list,dynamicValues);
							}
						} else {
							List<DdpTransferObject> list = new ArrayList<DdpTransferObject>();
							list.add(transferObject);
							sendMailForConnectionIssue(ddpExportRule, appName, typeOfService,toDate, fromDate, presentDate,list,dynamicValues);
						}
					}
					
					logger.info("DdpWDRuleSchedulerJob.performTransfer() - Status for Transfer : "+isFilesProcessed) ;
				}
				
				 //dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
				 
				 if ((typeOfService == 1 || typeOfService == 3) && isFilesProcessed) {
					 changeStatusForMissingDocs(entryNumberMap,missDocsMap, toDate);
					 createExportSuccessReprots(exportedReports);
				 }
				 
				 Date endTime = new Date();
				 if (missDocsMap.size() > 0)
					 sendMailForSuccessExport( ddpExportRule, appName, typeOfService, fromDate, toDate, mailBody, tempFolderPath,presentDate,endTime,transferList,dynamicValues);
				 if (exportMissDocsMap.size() > 0)
					 sendMailForMissingDocuments(entryNumberMap,exportMissDocsMap,ddpExportRule,tempFolderPath, appName, typeOfService, toDate,fromDate,presentDate,endTime,dynamicValues);
				 
				 isTransfered = true;
				 
			} catch (Exception ex) {
				logger.error("DdpWDRuleSchedulerJob.performTransfer() - Unable to transfer the files.", ex);
			} finally {
				if (tmpFile != null) {
					SchedulerJobUtil.deleteFolder(tmpFile);
				}
				
			}
		}
		
		return isTransfered;
		
	}
	
	private List<String> getRObjectIDOfImportsReqular(List<DdpExportMissingDocs> dfcDocs,String appName) {
	
		Map<String,List<DdpExportMissingDocs>> map = new HashMap<String,List<DdpExportMissingDocs>>();
		List<String> rObjects = new LinkedList<String>();
		for (DdpExportMissingDocs doc : dfcDocs) {
			
			if (doc.getMisDocType().equalsIgnoreCase("ENTRY")) {
				if (map.containsKey(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4))) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4));
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4), list);
				}
			} else {
				if (map.containsKey(doc.getMisDocType())) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType());
					list.add(doc);
					map.put(doc.getMisDocType(), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType(), list);
				}
			}
		}
		
		String importRegular = env.getProperty("export.rule."+appName+".documentType.import-reqular");
		//Sorting the document types
		Map<Integer, String> docTypeMap = new TreeMap<Integer, String>();
		String[] imports = importRegular.split(";");
		for (String regular : imports) {
			String[] args = regular.split("@");
			String[] docTypes = args[0].split("-");
			try {
				int seq = Integer.parseInt(args[1]);
				docTypeMap.put(seq, docTypes[0]);
			} catch (Exception ex) {
				continue;
			}
			
		}
		
		for (Integer key : docTypeMap.keySet()) {
			String documentType = docTypeMap.get(key);
			if (map.containsKey(documentType)) {
				List<DdpExportMissingDocs> list = map.get(documentType);
				for (DdpExportMissingDocs doc : list)
					rObjects.add(doc.getMisRObjectId());
			}
		}
		
		return rObjects;
		
	}
	
	private List<String> getRObjectIDOfImportsFTZ(List<DdpExportMissingDocs> dfcDocs,String appName) {
		
		Map<String,List<DdpExportMissingDocs>> map = new HashMap<String,List<DdpExportMissingDocs>>();
		List<String> rObjects = new LinkedList<String>();
		for (DdpExportMissingDocs doc : dfcDocs) {
			
			if (doc.getMisDocType().equalsIgnoreCase("ENTRY")) {
				if (map.containsKey(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4))) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4));
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4), list);
				}
			} else {
				if (map.containsKey(doc.getMisDocType())) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType());
					list.add(doc);
					map.put(doc.getMisDocType(), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType(), list);
				}
			}
		}
		
		String importRegular = env.getProperty("export.rule."+appName+".documentType.import-ftz");
		//Sorting the document types
		Map<Integer, String> docTypeMap = new TreeMap<Integer, String>();
		String[] imports = importRegular.split(";");
		for (String regular : imports) {
			String[] args = regular.split("@");
			String[] docTypes = args[0].split("-");
			try {
				int seq = Integer.parseInt(args[1]);
				docTypeMap.put(seq, docTypes[0]);
			} catch (Exception ex) {
				continue;
			}
			
		}
		
		for (Integer key : docTypeMap.keySet()) {
			String documentType = docTypeMap.get(key);
			if (map.containsKey(documentType)) {
				List<DdpExportMissingDocs> list = map.get(documentType);
				for (DdpExportMissingDocs doc : list)
					rObjects.add(doc.getMisRObjectId());
			}
		}
		
		return rObjects;
		
	}
	
	/**
	 * Method used for getting the Robject in the order for merging.
	 * 
	 * @param dfcDocs
	 * @param appName
	 * @return
	 */
	private List<String> getRObjectIDOfExports(List<DdpExportMissingDocs> dfcDocs,String appName) {
		
		//Map used form the Document type exactly as property file type.
		Map<String,List<DdpExportMissingDocs>> map = new HashMap<String,List<DdpExportMissingDocs>>();
		List<String> rObjects = new LinkedList<String>();
		
		for (DdpExportMissingDocs doc : dfcDocs) {
			
			if (doc.getMisDocType().equalsIgnoreCase("ENTRY")) {
				if (map.containsKey(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4))) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4));
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 4), list);
				}
			} else if (doc.getMisDocType().equalsIgnoreCase("CUSTOMSRPT") && doc.getMisRobjectName().startsWith("AESXMIT")) {
				if (map.containsKey(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0,7))) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 7));
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 7), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType()+":"+doc.getMisRobjectName().subSequence(0, 7), list);
				}
			} else {
				if (map.containsKey(doc.getMisDocType())) {
					List<DdpExportMissingDocs> list = map.get(doc.getMisDocType());
					list.add(doc);
					map.put(doc.getMisDocType(), list);
				} else {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(doc);
					map.put(doc.getMisDocType(), list);
				}
			}
		}
		
		String importRegular = env.getProperty("export.rule."+appName+".documentType.exports");
		//Sorting the document types
		Map<Integer, String> docTypeMap = new TreeMap<Integer, String>();
		String[] imports = importRegular.split(";");
		for (String regular : imports) {
			String[] args = regular.split("@");
			String[] docTypes = args[0].split("-");
			try {
				int seq = Integer.parseInt(args[1]);
				docTypeMap.put(seq, docTypes[0]);
			} catch (Exception ex) {
				continue;
			}
			
		}
		
		//
		for (Integer key : docTypeMap.keySet()) {
			String documentType = docTypeMap.get(key);
			if (map.containsKey(documentType)) {
				List<DdpExportMissingDocs> list = map.get(documentType);
				for (DdpExportMissingDocs doc : list)
					rObjects.add(doc.getMisRObjectId());
			}
		}
		
		return rObjects;
		
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
	private void sendMailForMissingDocuments(Map<String, List<DdpExportMissingDocs>> entryNumberMap,Map<String,String> exportMissDocsMap,DdpExportRule exportRule,String tmpFolder,
			String appName,int typeOfService,Calendar currentDate,Calendar startDate,Calendar presentDate,Date endTime,String dynamicValues) {
		
		DdpNotification notification = exportRule.getExpNotificationId();
		logger.info("DdpWDRuleSchedulerJob.sendMailForMissingDocuments() - Mail ID : "+notification.getNotSuccessEmailAddress()+" Failure : "+notification.getNotFailureEmailAddress());
		String errorFolder = tmpFolder + "/ERROR_FOLDER";
		File errorFolderFile = FileUtils.createFolder(errorFolder);
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			
			FileWriter csvFile = new FileWriter(errorFolder+"/Missing-"+appName+"_"+typeOfService+"_"+dateFormat.format(currentDate.getTime())+".csv");
			csvFile.append("Entry Type");
			csvFile.append(",");
			csvFile.append("Entry Number");
			csvFile.append(",");
			csvFile.append("FAST/400 Mstr Jb#");
			csvFile.append(",");
			csvFile.append("FAST/400 Job No.");
			csvFile.append(",");
			csvFile.append("Consignment ID");
			csvFile.append(",");
			csvFile.append("Missing Doc Types");
			csvFile.append("\n");
			
			for (String entryNumber : exportMissDocsMap.keySet()) {
				
				
				List<DdpExportMissingDocs> exports = entryNumberMap.get(entryNumber);
				DdpExportMissingDocs export = exports.get(0);
				String missingDocTypes = exportMissDocsMap.get(entryNumber);
				String[] missDocumentTypes = missingDocTypes.split(",");
				
				for (String documentType : missDocumentTypes) {
					csvFile.append(export.getMisEntryType());
					csvFile.append(",");
					csvFile.append(export.getMisEntryNo());
					csvFile.append(",");
					csvFile.append(export.getMisMasterJob());
					csvFile.append(",");
					csvFile.append(export.getMisJobNumber());
					csvFile.append(",");
					csvFile.append(export.getMisConsignmentId());
					csvFile.append(",");
					csvFile.append(documentType);
					csvFile.append("\n");
				}
												
			}
			csvFile.flush();
			csvFile.close();
			
			String serviceName = env.getProperty("ruleByQuery."+typeOfService);
			
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotSuccessEmailAddress(); 
			String ccAddress = notification.getNotFailureEmailAddress();
			String fromAddress = env.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = env.getProperty("export.rule."+appName+".mail.failure.subject");
			String body = env.getProperty("export.rule."+appName+".mail.failure.body");
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			
			body = body.replace("%%SERVICERUN%%", serviceName);
			body = body.replace("%%ENDDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%ENDTIME%%", dateFormat.format(endTime));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), endTime)+"");
			
			if (toAddress == null || toAddress.length()== 0)
				toAddress = env.getProperty("mail.toAddress");
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValues+"<br/>");
			} else {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
			}
						
			taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, errorFolderFile);
			
		} catch (IOException ex) {
			logger.error("DdpWDRuleSchedulerJob.sendMailForMissingDocuments() : Unable to send the mails or create csv file", ex);
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
	 * @param currentDate
	 * @param startDate
	 * @param tableBody
	 * @param hostName
	 * @param destLoc
	 */
	private void sendMailForSuccessExport(DdpExportRule exportRule,String appName,int typeOfService,Calendar currentDate,Calendar startDate,StringBuffer tableBody,String tmpFolder,Calendar presentDate,Date endTime,
			List<DdpTransferObject> ddpTransferObjects,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		String successFolder = tmpFolder + "/SUCCESS_FOLDER";
		File sucessFolderFile = FileUtils.createFolder(successFolder);
		try {
			//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
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
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(currentDate.getTime()));
			body = body.replace("%%ENDDATE%%", dateFormat.format(startDate.getTime()));
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
							+ "<tr><td align='center'><B>Total Records Exported</B></td><td>0</td></tr>";
					
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
			logger.error("DdpWDRuleSchedulerJob.sendMailForSuccessExoprot() - Unable to send the success mail", ex);
		}
	}
	
	/**
	 * Method used for changing the Status of missing documents.
	 * 
	 * @param exportedRecords
	 * @param currentDate
	 */
	private void changeStatusForMissingDocs(Map<String, List<DdpExportMissingDocs>> entryNumberMap,Map<String,List<DdpExportMissingDocs>> missDocsMap,Calendar currentDate) {
		
		for (String key : missDocsMap.keySet()) {
			
			List<DdpExportMissingDocs> exportedRecords = entryNumberMap.get(key);
			for(DdpExportMissingDocs docs : exportedRecords) {
				//System.out.println("Documents id : "+docs.getMisId());
				docs.setMisStatus(1);
				docs.setMisLastProcessedDate(currentDate);
				ddpExportMissingDocsService.updateDdpExportMissingDocs(docs);
			}
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
			IDfSession session,String invoiceNumber,StringBuffer mailBody,List<String> fileNameList,String sourceFolder,Map<String,String> loadNumberMap,int typeOfService,List<DdpExportSuccessReport> exportedReports) {
		
		String fileName = commonUtil.getDocumentFileName(objectName, namingConvention, exportDocs,invoiceNumber,fileNameList,loadNumberMap);
		if (fileName == null)	 
			fileName = objectName ;
			
		// need to check all version also
		//DdpRuleDetail detail = ruleDetailMap.get(exportDocs.getMisDocType());
		 boolean isDownload = false;
		// boolean isFirstRec = true;
		 
		/* if (mailBody != null && mailBody.length() != 0)
			 isFirstRec = false;*/
		 
	/*	if (versionMap.containsKey(exportDocs.getMisDocType()) && versionMap.get(exportDocs.getMisDocType()).equalsIgnoreCase("All")) {
			isDownload = downloadAllVersion(objectName, rObjectId, namingConvention, exportDocs, sourceFolder, isFTPType, ftpClient, destinationLocation, smbFile, session,invoiceNumber,mailBody);
		
		} else {*/
			isDownload  = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(rObjectId, sourceFolder, fileName, session);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
			if (isDownload) {
				//mailBody.append("<tr><td>"+dateFormat.format(new Date())+"</td>");
				//dateFormat.applyLocalizedPattern("HH:mm:ss");
				//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+exportDocs.getMisCheckDigit()+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
				
				mailBody.append(dateFormat.format(new Date())+",");
				dateFormat.applyLocalizedPattern("HH:mm:ss");
				//mailBody.append("<td>"+dateFormat.format(new Date())+"</td><td>receive</td><td>success</td><td>"+exportDocs.getMisCheckDigit()+"</td><td>00:00:01</td><td>"+fileName+"</td></tr>");
				mailBody.append(dateFormat.format(new Date())+",receive,success,"+exportDocs.getMisCheckDigit()+",00:00:01,"+fileName+"\n");
				if (typeOfService == 1 || typeOfService == 3) {
					exportedReports.add(commonUtil.constructExportReportDomainObject(exportDocs,Long.parseLong(exportDocs.getMisCheckDigit()), fileName, typeOfService));
				}
			}
			
		//}
			
		
		return isDownload;
	}
	

	
	
	
	/**
	 * Method used for connecting the FTP location.
	 * 
	 * @param ddpFtpClient
	 * @param ftpDetails
	 * @return
	 */
	private FTPClient connectFTP(DdpFTPClient ddpFtpClient,DdpCommFtp ftpDetails) {
		
		FTPClient ftpClient = null;
		String ftpPath = ftpDetails.getCftFtpLocation();
		String strFTPURL = null; 
		
		if (ftpPath.contains("/")) {
			
			ftpPath = ftpPath.substring(6, ftpPath.length());
			 String[] strArray = ftpPath.split("/");
			 strFTPURL = ftpPath.substring(0, strArray[0].length());
		}
		try {
			
    		ftpClient = ddpFtpClient.connectFTP(strFTPURL, ftpDetails.getCftFtpPort().intValue());
		} catch (Exception ex) {
			logger.error("DdpBackUpDocumentProcess- ConnectFTP() : Unable to connect to FTP location " , ex.getMessage());
		}
    	return ftpClient;
	
	}
	
	/**
	 * 
	 * @param list
	 * @param startTag
	 * @param endTag
	 * @param separator
	 * @return
	 */
	private String joinString(Set<String> list,String startTag,String endTag,String separator) {
		
		StringBuilder result = new StringBuilder();
	    for(String string : list) {
	    	
	    	result.append(startTag);
	    	result.append(string);
	        result.append(endTag);
	        result.append(separator);
	    }
	    String str = result.length() > 0 ? result.substring(0, result.length() - 1): "";
	    
	    return str;
	}
	
	/**
	 * 
	 * @param list
	 * @param startTag
	 * @param endTag
	 * @param separator
	 * @param type
	 * @return
	 */
	private String joinExportDocs(List<DdpExportMissingDocs> list,String startTag,String endTag,String separator,String type) {
	
		StringBuilder result = new StringBuilder();
	    for(DdpExportMissingDocs doc : list) {
	    	
	    	result.append(startTag);
	    	if (type.equalsIgnoreCase("jobNumber")) {
	    		 result.append(doc.getMisJobNumber());
	    	} else if (type.equalsIgnoreCase("consignmentID")) {
	    		result.append(doc.getMisConsignmentId());
	    	} else if (type.equalsIgnoreCase("entryNo")) {
	    		result.append(doc.getMisEntryNo());
	    	}
	        result.append(endTag);
	        result.append(separator);
	    }
	    String str = result.length() > 0 ? result.substring(0, result.length() - 1): "";
	    
	    return str;
	}
	
	/**
	 * 
	 * @param documentNotation
	 * @param docTypeSet
	 * @param mandatoryDocSet
	 */
	private void updateDocumentTypesInList(String documentNotation,Set<String> docTypeSet,Set<String> mandatoryDocSet) {
		
		if (documentNotation != null) {
			
			String[] documentTypes = documentNotation.split(";");
			
			for (String doc : documentTypes) {
				String[] docs = doc.split("-");
				if (docs.length > 0) {
					
					if (docs[0].contains(":")) 
						docTypeSet.add(docs[0].split(":")[0]);
					else
						docTypeSet.add(docs[0]);
					//Mandatory document types
					if (docs[1].startsWith("1"))
						mandatoryDocSet.add(docs[0]);
					//else if (docs[1].startsWith("0"))
						//optionalDocSet.add(docs[0]);
				}
			}
		}
	}
	
	/**
	 * Method used for grouping the missing records.
	 * 
	 * @param missingDocs
	 * @param entryNumberMap
	 */
	private void groupEntryNumbers(List<DdpExportMissingDocs> missingDocs,Map<String, List<DdpExportMissingDocs>> entryNumberMap) {
		
		for (DdpExportMissingDocs docs : missingDocs) {
			
			if (entryNumberMap.containsKey(docs.getMisEntryNo())) {
				List<DdpExportMissingDocs> list = entryNumberMap.get(docs.getMisEntryNo());
				list.add(docs);
				entryNumberMap.put(docs.getMisEntryNo(),list);
			} else {
				if (docs.getMisEntryNo() != null)  {
					List<DdpExportMissingDocs> list = new ArrayList<DdpExportMissingDocs>();
					list.add(docs);
					entryNumberMap.put(docs.getMisEntryNo(),list);
				}
			}
		}
	}
	
	/**
	 * Method used for performing the scheduler action based on the start date & end date.
	 * 
	 * @param strFeq
	 * @param queryStartDate
	 * @param queryEndDate
	 * @param ddpScheduler
	 * @return
	 */
//	private boolean performSchedulerAction(String strFeq,Calendar queryStartDate,Calendar queryEndDate,DdpScheduler ddpScheduler) {
//		
//		logger.debug("DdpWDRuleSchedulerJob.performSchedulerAction() is invoked");
//		boolean isSchedulerExuected = false;
//		boolean isFTPType = false;
//		DdpCommFtp ftpDetails = null;
//		DdpCommUnc uncDetails = null;
//		String documentType = "";
//		    	
//		DdpExportRule exportRule = getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId());
//		
//		if (exportRule == null || exportRule.getExpStatus() == 1) {
//			logger.info("DdpWDRuleSchedulerJob.performSchedulerAction(String strFeq,Calendar queryStartDate,Calendar queryEndDate,DdpScheduler ddpScheduler) - DdpExportRule is null or status is Inactive for Scheduler ID : "+ddpScheduler.getSchId());
//			return isSchedulerExuected;
//		}
//		
//		List<DdpRuleDetail> ruleDetails = getMatchedSchdIDAndRuleDetailForExport(ddpScheduler.getSchId());
//		
//		//adding the document type
//		for (DdpRuleDetail ruleDetail : ruleDetails) {
//			documentType =  documentType.concat("'").concat(ruleDetail.getRdtDocType().getDtyDocTypeCode()).concat("'").concat(",");
//		}
//		if(-1 != documentType.lastIndexOf(","))
//			documentType = documentType.substring(0, documentType.lastIndexOf(","));
//		
//		DdpCommunicationSetup ddpCommunicationSetup = exportRule.getExpCommunicationId();
//		isFTPType =	ddpCommunicationSetup.getCmsCommunicationProtocol().equalsIgnoreCase("FTP")? true : false;
//		
//		if (isFTPType) {
//			ftpDetails = getFTPDetailsBasedOnProtocolID(ddpCommunicationSetup.getCmsProtocolSettingsId()); 
//		} else {
//			uncDetails = getUNCDetailsBasedOnProtocolID(ddpCommunicationSetup.getCmsProtocolSettingsId());
//		}
//		
//		
//		if (ftpDetails == null && uncDetails == null) {
//    		logger.info("DdpWDRuleSchedulerJob.performSchedulerAction() - SCHEDULER [{}] FTP & UNC Details are not available for SchedulerID : ",ddpScheduler.getSchId());
//    		return isSchedulerExuected;
//    	}
//		
//		//To avoid time creation of session for each iteration of loop
//    	IDfSession session = ddpDFCClientComponent.beginSession();
//    	
//    	if (session == null) {
//    		logger.info("DdpWDRuleSchedulerJob.performSchedulerAction() - SCHEDULER [{}] unable to create IDFCSession for SchedulerID : ",ddpScheduler.getSchId());
//    		return isSchedulerExuected;
//    	}
//    	
//		//creating the temporary file with time stamp.
//    	Date date = new Date() ;
//    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
//    	String tempFolderPath = tempFilePath + "/temp_" + ddpScheduler.getSchId() + "_" + dateFormat.format(date);
//    	File tmpFile = new File(tempFolderPath);
//    	tmpFile.mkdir();
//    	
//    	
//		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean("schedulerJob", DdpRuleSchedulerJob.class);
//		// Need to get the details based on the client id from the custom table.
//		
//		//Get all the document Type from rule details based on the scheduler id.
//		// By passing the client ID and the date range get  all the Consignemnt  id, entry number, etc
//		// Query to DQL based on the consignement ID and doc_type get the all object id with latest document
//	
//		Map<String, Integer> documentMap = new HashMap<String, Integer>();
//	    String consigID = env.getProperty("export.rule."+ddpScheduler.getSchRuleCategory()+".consignmentIds");
//	    
//	    if (consigID != null && !consigID.isEmpty()) {
//	    	
//			String[] strConsignmentIds = consigID.split(",");
//			for (String strConsignmentId : strConsignmentIds ) {
//				IDfCollection dfCollection = null;
//				
//				String query = "select r_object_id, object_name, agl_consignment_id, agl_job_numbers, agl_branch_source, agl_company_source, agl_customs_entry_no, agl_control_doc_type from agl_control_document where agl_consignment_id = ('"+strConsignmentId+"') and agl_control_doc_type in ("+documentType+") order by agl_control_doc_type";
//			 	IDfQuery dfQuery = new DfQuery();
//			 	dfQuery.setDQL(query);
//			 	
//			 	try {
//			 		dfCollection = dfQuery.execute(session,IDfQuery.DF_EXEC_QUERY);
//			 		while(dfCollection.next()){
//				 		transferFileIntoLocalFolder(dfCollection, exportRule.getExpRuleId(), tempFolderPath, session, schedulerJob,documentMap);
//				 	}
//				} catch (DfException e) {
//					e.printStackTrace();
//					return isSchedulerExuected;
//				}
//			}
//		 	//Copying all the files in FTP or UNC
//	    	if (isFTPType) {
//	    		isSchedulerExuected = SchedulerJobUtil.performFileTransferUsingFTP(tmpFile,ftpDetails);
//	    	} else {
//	    		isSchedulerExuected = SchedulerJobUtil.peformFileTransferUsingUNC(tmpFile,uncDetails);
//	    	}
//	    }
//    	//delete the temp directory.
//    	SchedulerJobUtil.deleteFolder(new File(tempFolderPath));
//		
//    	        	
//		logger.debug("DdpWDRuleShedulerJob.performSchedulerAction() is successfully executed");
//		return isSchedulerExuected;
//	}
	
	/**
	 * Method used for running the onDemand service when click on the run button.
	 * 
	 * @param exportRuleID
	 * @return
	 */
	public void runOnDemandRuleJob(final DdpScheduler ddpScheduler,final Calendar fromDate,final Calendar toDate) {
		
		
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) method invoked.");
		Calendar presentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			
			//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
			final String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchCronExpressions());
    		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = new StdSchedulerFactory().getScheduler();
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,fromDate,toDate,2,presentDate,strFeq,null,null,null,scheduler});
			     jdfb.setName("OnDemand : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemand-WD : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemand-WD-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpWDRuleSchedulerJob.runOnDemandRuleJob() Error occurried while running the ondemand service", ex);
	        }
			
		}
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleJob(DdpScheduler ddpScheduler) executed successfully.");
		
		
	}
	
	public void runOnDemandService(DdpScheduler ddpScheduler,Calendar fromDate,Calendar toDate,int typeOfService,Calendar presentDate,
			String strFeq,String jobNumbers,String consignmentIDs,String docRefs,Scheduler scheduler) {
		
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleJob() - Thread invoked Successfully.");
		try {
		if (typeOfService == 2)
			performGeneralSchedulerProcess(ddpScheduler, fromDate, toDate, typeOfService,presentDate,strFeq);
		else 
			onDemandPerformGeneralSchedulerProcess(ddpScheduler, fromDate, toDate, typeOfService, presentDate, strFeq, jobNumbers, consignmentIDs, docRefs);
		} catch (Exception ex) {
			logger.error("DdpWDRuleShedulerJob.runOnDemandRuleJob() - Unable to execute",ex);
		} finally {
			try {
				scheduler.shutdown();
			} catch (SchedulerException e) {
				logger.error("DdpWDRuleShedulerJob.runOnDemandRuleJob() - Unable to shutdown scheduler", e);
			}
		}
	}
	
	/**
	 * Method used for getting the export rule based on the scheduler ID.
	 * 
	 * @param schedulerID
	 * @return
	 */
	private  DdpExportRule getExportRuleBasedOnSchedulerID(Integer schedulerID) {
		
		DdpExportRule ddpExportRule = null;
		
		logger.debug("DdpWDRuleShedulerJob.getExportRuleBasedOnSchedulerID(Integer schedulerID) is invoked");
		try
		{
			List<DdpExportRule> exportRules = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MACTED_EXPORT_RULE_ID, new Object[] { schedulerID }, new RowMapper<DdpExportRule>() {
							
				public DdpExportRule mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpExportRule ddpCategorizedDocs = ddpExportRuleService.findDdpExportRule(rs.getInt("EXP_RULE_ID"));
					
					return ddpCategorizedDocs;
		           }
			});		
			
			if (exportRules != null && exportRules.size() > 0)
				ddpExportRule = exportRules.get(0);
			
		}
		catch(Exception ex)
		{
			logger.error("DdpWDRuleShedulerJob.getExportRuleBasedOnSchedulerID(Integer schedulerID) - Exception while retrieving Matched DDP_EXPORT_RULE based on scheduler id [{}].", schedulerID);
			
			ex.printStackTrace();
		}
		logger.debug("DdpWDRuleShedulerJob.getExportRuleBasedOnSchedulerID(Integer schedulerID) is successfully executed");
		
		return ddpExportRule;
	}
	
	
/*	*//**
	 * Method used for getting the communication setup details based on id.
	 * 
	 * @param communicationSetupID
	 * @return
	 *//*
	private DdpCommunicationSetup getCommunicationSetupBasedOnID (Integer communicationSetupID) {
		
		logger.debug("DdpWDRuleSchedulerJob.getCommunicationSetupBasedOnID() is invoked");
		DdpCommunicationSetup ddpCommunicationSetup = null;
		
		try {
			ddpCommunicationSetup =	ddpCommunicationSetupService.findDdpCommunicationSetup(communicationSetupID);
		} catch (Exception ex) {
			logger.error("DdpWDRuleSchedulerJob.getCommunicationSetupBasedOnID() - Exception while retrieving based on communication setup ID : "+communicationSetupID,ex.getMessage());
			ex.printStackTrace();
		}
		logger.debug("DdpWDRuleSchedulerJob.getCommunicationSetupBasedOnID() is successfully executed");
		return ddpCommunicationSetup;
	}*/
	
	/**
	 * 
	 * @param protocolSettingID
	 * @return
	 */
	private DdpCommFtp getFTPDetailsBasedOnProtocolID(String protocolSettingID) {
		
		logger.debug("DdpWDRuleSchedulerJob.getFTPDetailsBasedOnProtocolID(Integer protocolSettingID) is invoked");
		DdpCommFtp ddpCommFtp = null;
		try {
			ddpCommFtp = ddpCommFtpService.findDdpCommFtp(Long.parseLong(protocolSettingID.trim()));
		} catch (Exception ex) {
			logger.error("DdpWDRuleSchedulerJob.getFTPDetailsBasedOnProtocolID() - Exception while retrieving detail of DDP_COMM_FTP Pbased on protocolSettingID : "+protocolSettingID,ex.getMessage());
			ex.printStackTrace();
		}
		logger.debug("DdpWDRuleSchedulerJob.getFTPDetailsBasedOnProtocolID(Integer protocolSettingID) is successfully executed");
		return ddpCommFtp;
	}
	
	/**
	 * 
	 * @param protocolSettingID
	 * @return
	 */
	private DdpCommUnc getUNCDetailsBasedOnProtocolID(String protocolSettingID) {
		
		logger.debug("DdpWDRuleSchedulerJob.getUNCDetailsBasedOnProtocolID(Integer protocolSettingID) is invoked");
		DdpCommUnc ddpCommUnc = null;
		
		try {
			ddpCommUnc = ddpCommUncService.findDdpCommUnc(Long.parseLong(protocolSettingID.trim()));
		} catch (Exception ex) {
			logger.error("DdpWDRuleSchedulerJob.getFTPDetailsBasedOnProtocolID() - Exception while retrieving detail of DDP_COMM_UNC Pbased on protocolSettingID : "+protocolSettingID,ex.getMessage());
			ex.printStackTrace();
		}
		logger.debug("DdpWDRuleSchedulerJob.getUNCDetailsBasedOnProtocolID(Integer protocolSettingID) is successfully executed");
		return ddpCommUnc;
	}
	
	/**
	 * Method used for transferring the files to local folder.
	 * 
	 * @param docs
	 * @param today
	 * @param isAllVersions
	 * @param tempFolderPath
	 */
	private boolean transferFileIntoLocalFolder(IDfCollection dfCollection,Integer ruleID,String tempFolderPath,IDfSession session,DdpRuleSchedulerJob ruleSchedulerJob,Map<String, Integer> documentMap) {
		
		logger.debug("DdpWDRuleSchedulerJob.transferFileIntoLocalFolder() is invoked");
		boolean isFileTransfered = false;
		
	 	
		//checking version name 
		DdpDocnameConv convName  = ruleSchedulerJob.getMatchedDocnameCon(ruleID);
		
	  try {
	   	
	  //check the file local repository else dms repository & copy in to temp folder
	   	String dmsDestDoc  = null;
	   //	File dmsSourceDoc = new File(tempFilePath+"/"+(docs.getCatDtxId().getDtxId()+"_"+dmsDocsDetails.getDddObjectName()));
	  //latest version & Naming Convention filed.
	   	if ( convName != null && convName.getDcvGenNamingConv() != null && !convName.getDcvGenNamingConv().isEmpty()) {	   	
	   		String ext = ".";
	   		if ( dfCollection.getString("object_name") != null && dfCollection.getString("object_name").contains(".")) {
		   		String [] fileNames = dfCollection.getString("object_name").split("\\.");
				 ext = ext +fileNames[1];
	   		} else {
	   			ext = ".pdf";
	   		}
	   		
	   		dmsDestDoc = NamingConventionUtil.getDocName(convName.getDcvGenNamingConv(), dfCollection.getString("agl_job_numbers"), 
	   				dfCollection.getString("agl_consignment_id"), dfCollection.getString("agl_control_doc_type"), dfCollection.getString("agl_company_source"), 
	   				dfCollection.getString("agl_branch_source"), "1",null,dfCollection.getString("r_version_label")).concat(ext);
	   		
	   		if (documentMap.containsKey(dmsDestDoc) && convName.getDcvDupDocNamingConv() != null && !convName.getDcvDupDocNamingConv().isEmpty()) {
	   			Integer copyCont = documentMap.get(dmsDestDoc);
	   			documentMap.put(dmsDestDoc, copyCont+1);
	   			dmsDestDoc =   NamingConventionUtil.getDocName(convName.getDcvDupDocNamingConv(), dfCollection.getString("agl_job_numbers"), 
	   					dfCollection.getString("agl_consignment_id"), dfCollection.getString("agl_control_doc_type"), dfCollection.getString("agl_company_source"),
	   					dfCollection.getString("agl_branch_source"), copyCont+"",null,dfCollection.getString("r_version_label")).concat(ext);
	   		}
	   	} else {	
	   		dmsDestDoc = dfCollection.getString("object_name");
	  	}
	   	
	   	if (!documentMap.containsKey(dmsDestDoc)) {
	   		documentMap.put(dmsDestDoc, 1);
	   	} else {
	   		Integer i = documentMap.get(dmsDestDoc);
	   		documentMap.put(dmsDestDoc, i+1);
	   	}
		
	   	isFileTransfered = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(dfCollection.getString("r_object_id"),tempFolderPath, dmsDestDoc,session);
		   	
		   	/*if (!isFileCopied)
		   		logger.info("DdpRuleSchedulerJob. "+dmsDocsDetails.getDddObjectName()+" file is not copied due to not available in DFC Client ");*/
	  } catch (Exception ex) {
		  logger.error("DdpWDRuleSchedulerJob.transferFileIntoLocalFolder() - Error occurried while transferring the file in to local folder",ex.getMessage());
		  ex.printStackTrace();
	  }
	   	logger.debug("DdpWDRuleSchedulerJob.transferFileIntoLocalFolder() is successfully executed...");
	   	return isFileTransfered;
	}
	
	
	public List<DdpRuleDetail> getMatchedSchdIDAndRuleDetailForExport(Integer intSchId)
    {
		logger.debug("DdpWDRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() method invoked.");
                   
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
        	logger.error("DdpWDRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() - Exception while retrieving Matched Schduler ID and Rule Detail ID for Export rules - Error message [{}].",ex.getMessage());
        	ex.printStackTrace();
        }
        
        logger.info("DdpWDRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() executed successfully.");
        return ruleDetails;
    }

	@Override
	public void reprocesRuleJob(final String appName,final Calendar endDate,final Calendar startDate) {
		
		Runnable r = new Runnable() {
	         public void run() {
	        	
	        	List<DdpExportMissingDocs> exportDocs = commonUtil.fetchMissingDocsBasedOnAppName(appName,startDate,endDate);
	        	if (exportDocs == null || exportDocs.size() == 0) {
	        		logger.info("DdpWDRuleSchedulerJob.reprocesRuleJob() - DdpExportMissingDocs are empty");
	        		return;
	        	}
	        	
	        	DdpExportRule ddpExportRule = commonUtil.getExportRuleById(exportDocs.get(0).getMisExpRuleId());
	        	
	        	if (ddpExportRule == null || ddpExportRule.getExpStatus() != 0) {
	        		logger.info("DdpWDRuleSchedulerJob.reprocesRuleJob() - DdpExportRule is null or status is not equal to zero");
	        		return;
	        	}
	        	Calendar presentDate = GregorianCalendar.getInstance();
	        	runGeneralSchedulerJob(ddpExportRule, exportDocs, startDate, endDate, 3, appName,presentDate,null);
	         }
		};
		

	     	ExecutorService executor = Executors.newCachedThreadPool();
	     	executor.submit(r);
		
	}
	
	
	/**
	 * Method used for sending the mail. if no records are available for exporting.
	 * 
	 * @param exportRule
	 * @param appName
	 * @param typeOfService
	 * @param endDate
	 * @param startDate
	 * @param presentDate
	 */
	private void sendMailForEmptyRecords(DdpExportRule exportRule,String appName,int typeOfService,Calendar endDate,Calendar startDate,Calendar presentDate,String content,String dynamicValues) {
		
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
			body = body.replace("%%CURRENTDATE%%", dateFormat.format(presentDate.getTime()));
			body = body.replace("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replace("%%DESTIONPATH%%", destinationLocation);
			body = body.replace("%%HOSTDETAILS%%", hostName);
			body = body.replace("%%ENDTIME%%", dateFormat.format(new Date()));
			body = body.replace("%%TOTALTIME%%", SchedulerJobUtil.timeDifferenceInDays(presentDate.getTime(), new Date())+"");
			//body = body.replace("%%COUNT%%", FileUtils.countCharacter(tableBody.toString(), '\n')+"");
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
			
			if (typeOfService == 4 || typeOfService == 5 || typeOfService == 6) {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", serviceName+": "+dynamicValues+"<br/>");
			} else {
				body = body.replaceAll("%%DYNAMICONDEMANDVALUE%%", "");
			}
			
			
			body = body.replaceAll("%%TRANSFERLOCATION%%", transferBody);
			
				taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, null);
		} catch (Exception ex) {
			logger.error("DdpWDRuleSchedulerJob.sendMailForEmptyRecords() - Unable to send the success mail", ex);
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
	private void sendMailForConnectionIssue(DdpExportRule exportRule,String appName,int typeOfService,Calendar endDate,Calendar startDate,
			Calendar presentDate,List<DdpTransferObject> ddpTransferObjects,String dynamicValues) {
		
		//1 for general scheduler configured by user, 2 - onDemand service, 3 - Re-processing service.
		
		try {
			
			DdpCommunicationSetup commSetup = exportRule.getExpCommunicationId();
			String destinationLocation = "";
			String hostName = "";
			
			DdpTransferObject transferObject = ddpTransferObjects.get(0);
			
			destinationLocation = transferObject.getDestLocation();				
			hostName = transferObject.getHostName();
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH-mm-ss");
			
			
			dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH:mm:ss");
			String serviceName = env.getProperty("ruleByQuery."+typeOfService);
						
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String toAddress = notification.getNotFailureEmailAddress();
			String ccAddress = notification.getNotSuccessEmailAddress();
			String fromAddress = env.getProperty("export.rule."+appName+".mail.fromAddress");
			String subject = env.getProperty("export.rule."+appName+".mail.transferissue.subject");
			String body = env.getProperty("export.rule."+appName+".mail.transferissue.body");
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

	@Override
	public void runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String jobNumbers) {
		
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler) method invoked.");
		Calendar presentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			
			//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
			final String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchCronExpressions());
    		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = env.getQuartzScheduler("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,fromDate,toDate,2,presentDate,strFeq,null,null,null,scheduler});
			     jdfb.setName("OnDemandJobNumber : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemandJobNumber-WD : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemandJobNumber-WD-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpWDRuleSchedulerJob.runOnDemandRuleBasedJobNumber() Error occurried while running the ondemand service", ex);
	        }
			
		}
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler) executed successfully.");
		
	}

	@Override
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String consignmentIDs) {
		
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler) method invoked.");
		Calendar presentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			
			//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
			final String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchCronExpressions());
    		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = env.getQuartzScheduler("OnDemanConsIDs : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,fromDate,toDate,5,presentDate,strFeq,null,consignmentIDs,null,scheduler});
			     jdfb.setName("OnDemanConsIDs : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemanConsIDs-WD : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemanConsIDs-WD-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpWDRuleSchedulerJob.runOnDemandRuleBasedConsignmentId() Error occurried while running the ondemand service", ex);
	        }
			
		}
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler) executed successfully.");
		
	}

	@Override
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate, String docRefs) {
	
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler) method invoked.");
		Calendar presentDate = GregorianCalendar.getInstance();
		if (ddpScheduler != null) {
			
			//Get the scheduler frequency from DDP_SCHEDULER table SCH_CRON_EXPRESSIONS column
			final String strFeq = SchedulerJobUtil.getSchFreq(ddpScheduler.getSchCronExpressions());
    		
			try {
		    	 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
		    	 Scheduler scheduler = env.getQuartzScheduler("OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
		    	 
		    	 jdfb.setTargetObject(this);
			     jdfb.setTargetMethod("runOnDemandService");
			     jdfb.setArguments(new Object[]{ddpScheduler,fromDate,toDate,6,presentDate,strFeq,null,null,docRefs,scheduler});
			     jdfb.setName("OnDemandDocRefs : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     jdfb.afterPropertiesSet();
			     JobDetail jd = (JobDetail)jdfb.getObject();
			     SimpleTriggerImpl trigger = new SimpleTriggerImpl();
			     trigger.setName("OnDemandDocRefs-WD : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setGroup("OnDemandDocRefs-WD-Group : "+ddpScheduler.getSchId()+"_"+(new Date()));
			     trigger.setStartTime(new Date());
			     trigger.setEndTime(null);
			     trigger.setRepeatCount(0);
			     trigger.setRepeatInterval(4000L);
			        
			     scheduler.scheduleJob(jd, trigger);
			   	 scheduler.start();
	       	} catch (Exception ex) {
	        		logger.error("AppName : "+ddpScheduler.getSchRuleCategory()+". DdpWDRuleSchedulerJob.runOnDemandRuleBasedDocRef() Error occurried while running the ondemand service", ex);
	        }
			
		}
		logger.info("DdpWDRuleShedulerJob.runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler) executed successfully.");
		
	}

	/**
	 * 
	 */
	@Override
	public File runOnDemandRulForReports(DdpScheduler ddpScheduler,
			Calendar fromDate, Calendar toDate,String typeOfStatus) {
		
		File reportGenerated = null;
		try {
//   		 MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
//   		 Scheduler scheduler = env.getQuartzScheduler("OnDemandReports : "+ddpScheduler.getSchId()+"_"+(new Date()));
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
//	     	 
			reportGenerated = this.generateReport(ddpScheduler, fromDate, toDate, null,typeOfStatus);
       	} catch (Exception ex) {
       		logger.error("DdpWDRuleShedulerJob.runOnDemandRulForReports() Error occurried while running the ondemand service", ex);
       	}
		return reportGenerated;
		
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
	 * 
	 * @param ddpScheduler
	 * @param fromDate
	 * @param toDate
	 * @param scheduler
	 */
	public File generateReport(DdpScheduler ddpScheduler,Calendar fromDate, Calendar toDate,Scheduler scheduler,String typeOfStatus) {
		
		File generatedFile = null;
		try {
			generatedFile = commonUtil.generateReports(ddpScheduler, fromDate, toDate,1,typeOfStatus);
		} catch (Exception ex) {
			logger.error("DdpWDRuleShedulerJob.generateReport() - Unable to generate reports", ex);
		} finally {
			if (scheduler != null) {
				try {
					scheduler.shutdown();
				} catch (SchedulerException e) {
					logger.error("DdpWDRuleShedulerJob.generateReport() - Unable to shutdown the scheduler", e);
				}
			}
		}
		return generatedFile;
		
	}

}
