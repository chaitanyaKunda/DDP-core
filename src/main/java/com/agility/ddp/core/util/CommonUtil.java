package com.agility.ddp.core.util;

import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.components.DdpFTPClient;
import com.agility.ddp.core.components.DdpFTPSClient;
import com.agility.ddp.core.components.DdpSFTPClient;
import com.agility.ddp.core.components.DdpTransferFactory;
import com.agility.ddp.core.components.DdpTransferObject;
import com.agility.ddp.core.components.DdpUNCClient;
import com.agility.ddp.core.components.DdpVirtualDocBuilder;
import com.agility.ddp.core.components.DdpVirtualDocTransformer;
import com.agility.ddp.core.entity.DdpRateSetupEntity;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommEmailService;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommFtpService;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.agility.ddp.data.domain.DdpCommUncService;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.agility.ddp.data.domain.DdpCommunicationSetupService;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpDocnameConv;
import com.agility.ddp.data.domain.DdpEmailTriggerSetup;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportMissingDocsService;
import com.agility.ddp.data.domain.DdpExportQuery;
import com.agility.ddp.data.domain.DdpExportQueryService;
import com.agility.ddp.data.domain.DdpExportQueryUi;
import com.agility.ddp.data.domain.DdpExportQueryUiService;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpExportRuleService;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.agility.ddp.data.domain.DdpExportSuccessReportService;
import com.agility.ddp.data.domain.DdpGenSourceSetup;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.agility.ddp.data.domain.DdpJobRefHolderService;
import com.agility.ddp.data.domain.DdpMultiEmails;
import com.agility.ddp.data.domain.DdpNotification;
import com.agility.ddp.data.domain.DdpParty;
import com.agility.ddp.data.domain.DdpPartyService;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpScheduler;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

@Component
public class CommonUtil {

	private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);
	
	@Autowired
	private DdpCommFtpService ddpCommFtpService;
	
	@Autowired
	private DdpCommUncService ddpCommUncService;
	
	@Autowired
	private DdpCommEmailService ddpCommEmailService;
	
	@Autowired
	private DdpExportRuleService ddpExportRuleService;
	 
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private DdpExportMissingDocsService ddpExportMissingDocsService;
	
	@Autowired
	private DdpRuleDetailService ddpRuleDetailService;
	
	@Autowired
	private JdbcTemplate controlJdbcTemplate;
	
	@Autowired
	private DdpExportSuccessReportService ddpExportSuccessReportService;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private TaskUtil taskUtil;
	
	@Autowired
	private DdpJobRefHolderService ddpJobRefHolderService;
	
	@Autowired
	private DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	private DdpTransferFactory ddpTransferFactory;
	
	@Autowired
	private DdpCommunicationSetupService ddpCommunicationSetupService;
	
	private Map<String, Date> consolidatedThreadMap = new ConcurrentHashMap<String, Date>();
	
	private Map<String,Date> exportThreadMap = new ConcurrentHashMap<String, Date>();
	
	@Autowired
	private DdpPartyService partyService;
	
	@Autowired
	public DdpExportQueryUiService ddpExportQueryUiService;
	
	@Autowired
	public DdpExportQueryService ddpExportQueryService;
	
	/**
	 * Method used for getting the FTP details based on the protocol id.
	 * 
	 * @param protocolSettingID
	 * @return DdpCommFtp
	 */
	public DdpCommFtp getFTPDetailsBasedOnProtocolID(String protocolSettingID) {
		
		logger.debug("CommonUtil.getFTPDetailsBasedOnProtocolID(Integer protocolSettingID) is invoked");
		DdpCommFtp ddpCommFtp = null;
		try {
			ddpCommFtp = ddpCommFtpService.findDdpCommFtp(Long.parseLong(protocolSettingID.trim()));
		} catch (Exception ex) {
			logger.error("CommonUtil.getFTPDetailsBasedOnProtocolID() - Exception while retrieving detail of DDP_COMM_FTP Pbased on protocolSettingID : "+protocolSettingID,ex.getMessage());
			ex.printStackTrace();
		}
		logger.debug("CommonUtil.getFTPDetailsBasedOnProtocolID(Integer protocolSettingID) is successfully executed");
		return ddpCommFtp;
	}
	
	/**
	 * Method used for getting the unc details based on the protocol id.
	 * 
	 * @param protocolSettingID
	 * @return DdpCommUnc
	 */
	public DdpCommUnc getUNCDetailsBasedOnProtocolID(String protocolSettingID) {
		
		logger.debug("CommonUtil.getUNCDetailsBasedOnProtocolID(Integer protocolSettingID) is invoked");
		DdpCommUnc ddpCommUnc = null;
		
		try {
			ddpCommUnc = ddpCommUncService.findDdpCommUnc(Long.parseLong(protocolSettingID.trim()));
		} catch (Exception ex) {
			logger.error("CommonUtil.getFTPDetailsBasedOnProtocolID() - Exception while retrieving detail of DDP_COMM_UNC Pbased on protocolSettingID : "+protocolSettingID,ex.getMessage());
			//ex.printStackTrace();
		}
		logger.debug("CommonUtil.getUNCDetailsBasedOnProtocolID(Integer protocolSettingID) is successfully executed");
		return ddpCommUnc;
	}
	
	/**
	 * Method used for getting the email details based on protocol id.
	 * 
	 * @param protocolSettingID
	 * @return
	 */
	public DdpCommEmail getEmailDetailsBasedOnProtocolID(String protocolSettingID) {
		
		DdpCommEmail ddpCommEmail = null;
		
		try {
			ddpCommEmail = ddpCommEmailService.findDdpCommEmail(Integer.parseInt(protocolSettingID.trim()));
		} catch (Exception ex) {
			logger.error("CommonUtil.getEmailDetailsBasedOnProtocolID() - Exception while retrieving detail of DDP_COMM_EMAIL based on protocolSettingID : "+protocolSettingID,ex.getMessage());
		}
		return ddpCommEmail;
	}
	
	/**
	 * Method used for getting the export rule based on the scheduler ID.
	 * 
	 * @param schedulerID
	 * @return
	 */
	public DdpExportRule getExportRuleBasedOnSchedulerID(Integer schedulerID,String appName) {
		
		DdpExportRule ddpExportRule = null;
		
		logger.debug("CommonUtil.getExportRuleBasedOnSchedulerID(Integer schedulerID) is invoked. For appliaction : "+appName);
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
			logger.error("CommonUtil.getExportRuleBasedOnSchedulerID(Integer schedulerID) - Exception while retrieving Matched DDP_EXPORT_RULE based on scheduler id :  "+schedulerID + " For application : "+appName, ex.getMessage());
			
		}
		logger.debug("CommonUtil.getExportRuleBasedOnSchedulerID(Integer schedulerID) is successfully executed. For application : "+appName);
		
		return ddpExportRule;
	}
	
	/**
	 * Method used for getting all the missing document from the DB based on the application name & status is zero.
	 * 
	 * @param appName
	 * @return
	 */
	public List<DdpExportMissingDocs> fetchMissingDocsBasedOnAppName(String appName,Calendar startDate,Calendar endDate) {
		
		List<DdpExportMissingDocs> missingDocs = null;
		logger.debug("CommonUtil.fetchMissingDocsBasedOnAppName(Integer appName) is invoked. For appliaction : "+appName);
		
		try
		{
			missingDocs = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_MISSING_DOCS_APP_STATUS,new Object[]{appName,startDate,endDate},new RowMapper<DdpExportMissingDocs>() {

				@Override
				public DdpExportMissingDocs mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					
					DdpExportMissingDocs ddpExportMissingDocs = ddpExportMissingDocsService.findDdpExportMissingDocs(rs.getInt("MIS_ID"));
					
					return ddpExportMissingDocs;
				}
				
			});
		} catch (Exception ex) {
			
		}
		logger.debug("CommonUtil.fetchMissingDocsBasedOnAppName(Integer appName) is successfully executed. For application : "+appName);
		return missingDocs;
		
	}
	
	/**
	 * Method used for getting the DdpExportRule details.
	 * 
	 * @param id
	 * @return
	 */
	public DdpExportRule getExportRuleById(Integer id) {
		
		DdpExportRule ddpExportRule = null;
		
		try {
			ddpExportRule = ddpExportRuleService.findDdpExportRule(id);
		} catch (Exception ex) {
			
		}
		return ddpExportRule;
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
			logger.error("CommonUtil.getAllRuleDetails(Integer ruleID) - Exception while accessing DdpRuleDetail retrive based [{}] records for RULE_ID : "+ruleID,ex);
			ex.printStackTrace();
		}
		
		return ddpRuleDetails;
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
	 * Method used for getting the detail of IDFCollection
	 * @param query
	 * @param session
	 * 
	 * @return IDFCollection
	 */
    public IDfCollection getIDFCollectionDetails(String query,IDfSession session) {
		
		IDfCollection dfCollection = null;
		try {
			 IDfQuery dfQuery = new DfQuery();
		 	 dfQuery.setDQL(query);
		 	 dfCollection = dfQuery.execute(session,IDfQuery.DF_EXEC_QUERY);
		 		 		 
		} catch (Exception ex) {
			logger.error("CommonUtils.getIDFCollectionDetails(String query,IDfSession session) - unable to get details of DFCCollections \n for the Query : "+query, ex.getMessage());
			ex.printStackTrace();
		}
		return dfCollection;
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
				exportDocs.setMisEntryNo(iDfCollection.getString("agl_customs_entry_no"));
				exportDocs.setMisMasterJob(iDfCollection.getString("agl_master_job_number"));
				exportDocs.setMisJobNumber(iDfCollection.getString("agl_job_numbers"));
				exportDocs.setMisConsignmentId(iDfCollection.getString("agl_consignment_id"));
				//Invoice number for sales document reference
				exportDocs.setMisEntryType(iDfCollection.getString("agl_doc_ref"));
				//Content size
				exportDocs.setMisCheckDigit(iDfCollection.getString("r_content_size"));
				System.out.println("Inside the construnctMissingDocs : "+exportDocs.getMisEntryType());
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
					logger.error("CommonUtils.constructMissingDocs() - Unable to parse string to date : for App Name : "+appName,ex.getMessage());
				}
				missingDocs.add(exportDocs);
			}
		} catch (Exception ex) {
			missingDocs = new ArrayList<DdpExportMissingDocs>();
			logger.error("DdpBackDocumentProcess.constructMissingDocs() - Unable to construct the Missing docs for App Name : "+appName, ex.getMessage());
		}
		return missingDocs;
	}
	
	/**
	 * Method used for performing the merge operation.
	 * 
	 * @param rObjList
	 * @param docName
	 * @param vdLocation
	 * @param objType
	 * @param dfSession
	 * @return
	 */
	public String performMergeOperation(List<String> rObjList,String docName,String vdLocation,String objType,IDfSession dfSession,String appName) {
		
		String robjectID = null;
		DdpVirtualDocBuilder docBuilder = new DdpVirtualDocBuilder();
		String sysObjectID = docBuilder.createVirtualDocumentObject(dfSession, docName, rObjList, vdLocation, objType,appName);
		
		if(sysObjectID == null) {
			sysObjectID = docBuilder.createVirtualDocumentObject(dfSession, docName, rObjList, vdLocation, objType,appName);
			//docBuilder.createVirtualDocumentObject(dfSession, docName, rObjList, vdLocation, objType,appName);
			if (sysObjectID == null) {
				logger.info("AppName : "+appName+" CommonUntils.preformMergeOperation() - Unable to create virtual");
				return robjectID;
			}
		}
		
		DdpVirtualDocTransformer transformer = new DdpVirtualDocTransformer();
		robjectID =	transformer.initiateCTSRequestSetup(dfSession, sysObjectID, objType, vdLocation,appName,env);
		if (robjectID == null) {
			//retry
			logger.info("AppName : "+appName+". CommonUntils.performMergeOperation(). Retry again due to the document as not been merged.");
			robjectID =	transformer.initiateCTSRequestSetup(dfSession, sysObjectID, objType, vdLocation,appName,env);
		}
		
		return robjectID;
		
	}
	
	/**
	 * Method for Logging the FTP location.
	 * 
	 * @param ddpFTPClient
	 * @param ftpDetails
	 * @return
	 */
	public boolean LoggedIntoFTP (DdpFTPClient ddpFTPClient,DdpCommFtp ftpDetails,FTPClient ftpClient) {
		
		
		boolean isLogged = false;
		try {
    	
    			 isLogged = ddpFTPClient.loginFTP(ftpClient, ftpDetails.getCftFtpUserName(), ftpDetails.getCftFtpPassword());
    	} catch(Exception ex) {
    			ex.printStackTrace();
    	}
    		
    			
    	return isLogged;
	}
	
	/**
	 * Method for Logging the FTP location.
	 * 
	 * @param ddpFTPClient
	 * @param ftpDetails
	 * @return
	 */
	public boolean LoggedIntoFTPS (DdpFTPSClient ddpFTPsClient,DdpCommFtp ftpDetails,FTPSClient ftpsClient) {
		
		
		boolean isLogged = false;
		try {
    	
    			 isLogged = ddpFTPsClient.loginFTPS(ftpsClient, ftpDetails.getCftFtpUserName(), ftpDetails.getCftFtpPassword());
    	} catch(Exception ex) {
    			ex.printStackTrace();
    	}
    		
    			
    	return isLogged;
	}
	

	/**
	 * Method used for getting the destination location.
	 * 
	 * @param destinationLocation
	 * @return
	 */
	public String getDestinationLocation(String destinationLocation) {
		
		destinationLocation = destinationLocation.substring(nthOccurrence(destinationLocation,'/',1)+1);
		
		return destinationLocation;
	}
	
	/**
	 * Method used for getting the host name.
	 * 
	 * @param destinationLocation
	 * @return
	 */
	public String getHostName(String destinationLocation) {
		
		destinationLocation = destinationLocation.substring(nthOccurrence(destinationLocation,'/',1)+1, nthOccurrence(destinationLocation,'/',2));
		
		return destinationLocation;
	}
	

	
	/**
	 * Method used for connecting the UNC path.
	 * 
	 * @param ddpUNC
	 * @return
	 */
	public SmbFile connectUNCPath(DdpCommUnc ddpUNC) {
		
		String userName =  ddpUNC.getCunUncUserName(); //"svc-dmsddp";
		 String password = ddpUNC.getCunUncPassword();//"P@ssw0rd123";
		 String domain = "";
		 String destPath = "smb:"+ddpUNC.getCunUncPath() + "/"; //scandoc.agilitylogistics.com/svc-dmsddp/DDPDATA-UAT/testing";
		 SmbFile sFile = null;
		 try {
	  	     DdpUNCClient ddpUNCClient = new DdpUNCClient();
	  	     NtlmPasswordAuthentication auth = ddpUNCClient.uncAutentication(domain, userName, password);
	  	      sFile = new SmbFile(destPath, auth);
	  	      sFile.canRead();
	  	      
		 } catch(Exception ex) {
			 logger.error("CommonUtils - connectUNCPath() - Unable to connect to UNC path. ", ex);
			 sFile = null;
		 }
		 return sFile;
	}
	
	
	/**
	 * Method used for connecting the FTP location.
	 * 
	 * @param ddpFtpClient
	 * @param ftpDetails
	 * @return
	 */
	public FTPClient connectFTP(DdpFTPClient ddpFtpClient,DdpCommFtp ftpDetails) {
		
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
	 * Method used for getting the document name.
	 * 
	 * @param objectName
	 * @param namingConvention
	 * @param exportDocs
	 * @param invoiceNumber
	 * @param fileNameList
	 * @return
	 */
	public String getDocumentName(String objectName,DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,String invoiceNumber,List<String> fileNameList,Map<String, String> loadNumberMap) {
			
			//need to get the file extension or object name.
			String extension = null;
			String fileName = null;
			extension = ((extension =FileUtils.getFileExtension(objectName)) == null ? ".pdf":extension);
			
			 if (namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().length() != 0) {
				 
				 fileName = NamingConventionUtil.getGenericDocumentName(namingConvention.getDcvGenNamingConv(), exportDocs.getMisJobNumber(), 
						 exportDocs.getMisConsignmentId(), exportDocs.getMisDocType(), exportDocs.getMisCompany(), exportDocs.getMisBranch(), 
						 null, invoiceNumber,exportDocs.getMisEntryNo(),loadNumberMap,exportDocs.getMisDocVersion());
				 
				 String seq = checkFileExists(fileNameList, fileName, null);
				 // if already document name exists then use the duplicate naming convention.
				 if (seq != null && namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().length() != 0) {
					 fileName = getDuplicateNamingConventionName(objectName, namingConvention, exportDocs, invoiceNumber,seq,fileNameList,loadNumberMap);
				 } else if (seq != null && (namingConvention.getDcvDupDocNamingConv() == null || namingConvention.getDcvDupDocNamingConv().length() == 0)) {
					// fileName = null;
					 fileName += "_Copy_"+seq;
					 seq = checkFileExists(fileNameList, fileName, seq);
					 if (seq != null) {
						 fileName = getDocumentName(fileName, namingConvention, exportDocs, invoiceNumber, fileNameList,loadNumberMap);
					 }

				 }
				
			 } else if (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().length() != 0) {
				 fileName = getDuplicateNamingConventionName(objectName, namingConvention, exportDocs, invoiceNumber,"1",fileNameList,loadNumberMap);
			 }
				
			 if (fileName != null)
				 fileName += extension;
			 else 
				 fileName = getFileNameWithCopy(objectName, fileNameList, null, null);
			 
			//	logger.info("getDocumentName() - File Name : "+fileName);
			return fileName;
	}
	
	/**
	 * Method used for getting the file name with copy.
	 * 
	 * @param objectName
	 * @param fileNameList
	 * @param fileName
	 * @param seq
	 * @return
	 */
	public String getFileNameWithCopy(String objectName,List<String> fileNameList,String fileName,String seq) {
		
		String extension = null;
		extension = ((extension =FileUtils.getFileExtension(objectName)) == null ? ".pdf":extension);
		String file = getDocumentObjectName(objectName, fileNameList, fileName, seq);
		fileName = file +extension;
		return fileName;
		
	}
	
	/**
	 * Method used for getting the file name.
	 * 
	 * @param objectName
	 * @param fileNameList
	 * @param fileName
	 * @param seq
	 * @return
	 */
	public String getDocumentObjectName(String objectName,List<String> fileNameList,String fileName,String seq) {
		
	
		String fileNameWithOutExt = FileUtils.getFileNameWithOutExtension(objectName);
		
		if (seq == null) {
			seq = checkFileExists(fileNameList, fileNameWithOutExt, seq);
			if (seq != null)
				fileName = getDocumentObjectName(objectName,fileNameList, fileNameWithOutExt, seq);
			else 
				fileName = fileNameWithOutExt;
		} else {
			fileName = fileNameWithOutExt + "_Copy_"+seq;
			seq = checkFileExists(fileNameList, fileName, seq);
			if (seq != null) {
				fileName = getDocumentObjectName(objectName,fileNameList, fileNameWithOutExt, seq);
			}
		}
		return fileName;
	}
	/**
	 * Method used for getting the filename of duplicate naming convention.
	 * 
	 * @param objectName
	 * @param namingConvention
	 * @param exportDocs
	 * @param invoiceNumber
	 * @param sequenceNo
	 * @param fileNameList
	 * @return
	 */
	public String getDuplicateNamingConventionName(String objectName,DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,
			String invoiceNumber,String sequenceNo,List<String> fileNameList,Map<String, String> loadNumberMap) {
		
		String fileName = NamingConventionUtil.getGenericDocumentName(namingConvention.getDcvDupDocNamingConv(), exportDocs.getMisJobNumber(), exportDocs.getMisConsignmentId(), 
				exportDocs.getMisDocType(),exportDocs.getMisCompany(), exportDocs.getMisBranch(),sequenceNo, invoiceNumber,exportDocs.getMisEntryNo(),loadNumberMap,exportDocs.getMisDocVersion());
		
		String seq = checkFileExists(fileNameList, fileName, sequenceNo);
		
		if (seq != null) {
			//if copy is not present the adding the copy
			if (!namingConvention.getDcvDupDocNamingConv().toLowerCase().contains("%%seq%%")) {
				fileName += "_Copy_"+seq;
			}
			 seq = checkFileExists(fileNameList, fileName, sequenceNo);
			 if (seq != null) {
				 fileName = getDuplicateNamingConventionName(objectName, namingConvention, exportDocs, invoiceNumber,seq, fileNameList,loadNumberMap);
			 }
		}
		return fileName;
		
	}
	
	
	
	/**
	 * Method used for checking the file exists in the FTP, UNC or local file system.
	 * 
	 * @param fileNames
	 * @param fileName
	 * @param sequence
	 * @return
	 */
	public String checkFileExists(List<String> fileNames,String fileName,String sequence) {
			
		String numberfileExists = null;
		
		for (String file : fileNames) {
			if (file.startsWith(fileName)) {
				if (sequence != null) {
					int number = Integer.parseInt(sequence);
					number += 1;
					numberfileExists = number + "";
				} else{
					numberfileExists = "1";
				}
				
				break;
			}
		}
			
		return numberfileExists;
	}
		
	
	/**
	 * Method used for reading the local file system.
	 * 
	 * @param sourceLocation
	 * @return
	 */
	public List<String> readListOfFileInLocal (String sourceLocation) {
		
		List<String> fileNames = new ArrayList<String>();
		File folder = new File(sourceLocation);
		File[] listOfFiles = folder.listFiles();
	
		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		    		String[] fileName = listOfFiles[i].getName().split("\\.");
					if (fileName.length > 0)
					fileNames.add(fileName[0]);
		      } 
		    }
		  return fileNames;
	}
	
	/**
	 * Method used for reading the FTP file system.
	 * 
	 * @param client
	 * @param destLocation
	 * @return
	 */
	public  List<String> readListofFilesInFTP (FTPClient client,String destLocation) {
			
			List<String> fileNames = new ArrayList<String>();
			try {
				FTPFile[] files = client.listFiles(destLocation);
				for (FTPFile file : files) {
					String[] fileName = file.getName().split("\\.");
					if (fileName.length > 0)
					fileNames.add(fileName[0]);
				}
			} catch (Exception ex) {
				logger.error("DdpBackUpDocumentProcess - readListOfFilesInFTP() : Unable to read the files", ex);
			}
			return fileNames;
			
	}
	
	/**
	 * Method used for reading the unc file system.
	 * 
	 * @param smbFile
	 * @return
	 */
	public List<String> readListOfFilesInUNC(SmbFile smbFile) {
		
		List<String> fileNames = new ArrayList<String>();
		try {
		SmbFile[] files = smbFile.listFiles();
		 for (SmbFile file : files) {
			 if (file.isFile()) {
				 String[] fileName = file.getName().split("\\.");
				 if (fileName.length > 0)
					fileNames.add(fileName[0]);
			 }
			 
		 }
		} catch (Exception ex) {
			logger.error("DdpBackUpDocumentProcess - readListOfFilesInUNC() : Unable to read the files", ex);
		}
		return fileNames;
	}
	
	/**
	 * Method used for reading the List of files in the destination folder.
	 * 
	 * @param channel
	 * @param destFolder
	 * @return
	 */
	public List<String> readListOfFilesInSFTP(Channel channel,String destFolder,DdpSFTPClient ddpSFTPClient,DdpCommFtp commFtp) {
		
		List<String> fileNames = new ArrayList<String>();
		ChannelSftp channelSftp = (ChannelSftp)	channel; 
		try {
			if (destFolder.contains("/")) {
				String[] strArray = destFolder.split("/");
				String strFTPURL = destFolder.substring(0, strArray[0].length());
				 //Avoiding "/" for ftp destination path
				destFolder = destFolder.substring(strFTPURL.length()+1);
			}
			
			if (!channelSftp.isConnected()) {
				logger.info("CommonUtil.readListOfFilesInSFTP() - trying to reconnect the ftp location");
				channelSftp = connectChannelSftp(ddpSFTPClient, commFtp);				
			}
			
			Vector<ChannelSftp.LsEntry> vectors = channelSftp.ls(destFolder);
			for (ChannelSftp.LsEntry entry : vectors) {
				if (entry.getFilename().contains(".")) {
					 String[] fileName = entry.getFilename().split("\\.");
					 if (fileName.length > 0)
						fileNames.add(fileName[0]);
				}
			}
		} catch (SftpException e) {
			logger.error("DdpBackUpDocumentProcess -readListOfFilesInSFTP() : Unable to read the files",e);
		}
		
		return fileNames;
	}
	
	/**
	 * Method used to get the Matched RuleDetails using scheduler id.
	 * 
	 * @param intSchId
	 * @return
	 */
	public List<DdpRuleDetail> getMatchedSchdIDAndRuleDetailForExport(Integer intSchId)
    {
		logger.debug("CommonUtil.getMatchedSchdIDAndRuleDetailForExport() method invoked.");
                   
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
        	logger.error("CommonUtil.getMatchedSchdIDAndRuleDetailForExport() - Exception while retrieving Matched Schduler ID and Rule Detail ID for Export rules - Error message [{}].",ex.getMessage());
        	ex.printStackTrace();
        }
        
		
        //System.out.println("DdpRuleSchedulerJob.getMatchedSchdIDAndRuleDetailForExport() executed successfully.");
        return ruleDetails;
    }
	
	/**
	 * Method used for getting the channel of SFTP connection.
	 * 
	 * @param sftpClient
	 * @param commFtp
	 * @return
	 */
	public  ChannelSftp connectChannelSftp(DdpSFTPClient sftpClient,DdpCommFtp commFtp) {
		
		ChannelSftp channelSftp = null;
		
		String ftpPath = commFtp.getCftFtpLocation();
		String strFTPURL = null; 
		try {
			//getting the sftp url path.
			if (ftpPath.contains("/")) {
				
				ftpPath = ftpPath.substring(7, ftpPath.length());
				 String[] strArray = ftpPath.split("/");
				 strFTPURL = ftpPath.substring(0, strArray[0].length());
			}
		
			Session session = sftpClient.getJSchSession(strFTPURL, commFtp.getCftFtpUserName(), commFtp.getCftFtpPassword(), commFtp.getCftFtpPort().intValue());
			if (session == null)
				return channelSftp;
			channelSftp = sftpClient.connectToSFTP(session);
		} catch (Exception ex) {
			logger.error("CommonUtil.connectChannelSftp() - Unable to get the connection channel for SFTP", ex);
		}
		
		return channelSftp;
	}
	
	/**
	 * Method used for reading the FileNames form the location & local download location for naming convension.
	 * 
	 * @param sourceFolder
	 * @param isFTPType
	 * @param ftpClient
	 * @param destinationLocation
	 * @param smbFile
	 * @param channelSftp
	 * @param fileNameList
	 */
	public  void readFileNamesFromLocation(String sourceFolder,boolean isFTPType,FTPClient ftpClient,String destinationLocation,SmbFile smbFile,ChannelSftp channelSftp,List<String> fileNameList,
			DdpSFTPClient ddpSFTPClient,DdpCommFtp commFtp) {
		
		fileNameList.addAll(readListOfFileInLocal(sourceFolder));
		
		if (isFTPType)  {
			if (ftpClient != null)
				fileNameList.addAll(readListofFilesInFTP(ftpClient, destinationLocation));
			else 
				fileNameList.addAll(readListOfFilesInSFTP(channelSftp, destinationLocation,ddpSFTPClient,commFtp));
		} else {
			fileNameList.addAll(readListOfFilesInUNC(smbFile));
		}
		
	}
	
	/**
	 * Method used for reading the files from the location.
	 * 
	 * @param sourceFolder
	 * @param ddpTransferObject
	 * @param fileNameList
	 */
	public void readFileNamesFromLocation(String sourceFolder,DdpTransferObject ddpTransferObject,List<String> fileNameList) {
		
		 try {
			 fileNameList.addAll(readListOfFileInLocal(sourceFolder));
			if (ddpTransferObject.isFTPType()) {
				
				if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("ftp")) 
					fileNameList.addAll(readListofFilesInFTP(ddpTransferObject.getFtpClient(), ddpTransferObject.getDestLocation()));
				else if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("sftp")) {
					DdpSFTPClient ddpSFTPClient = (DdpSFTPClient) ddpTransferObject.getDdpTransferClient();
					fileNameList.addAll(readListOfFilesInSFTP(ddpTransferObject.getChannelSftp(), ddpTransferObject.getDestLocation(),ddpSFTPClient,ddpTransferObject.getFtpDetails()));
				} else if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("ftps")) {
					fileNameList.addAll(readListofFilesInFTP(ddpTransferObject.getFtpClient(), ddpTransferObject.getDestLocation()));
				}
				
			} else {
				if (!ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("smtp")) 			
					fileNameList.addAll(readListOfFilesInUNC(ddpTransferObject.getSmbFile()));
			}
		 } catch (Exception ex) {
			 logger.error("CommonUtil.readFileNamesFromLocation() - unable to read filename from the location", ex);
		 }
		
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
	public String getDocumentFileName (String objectName,DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,String invoiceNumber, List<String> fileNameList,Map<String, String> loadNumberMap) {
		
		String fileName = null;
		
		//check the name is already exists in the destination (FTP & Local) locations. if exists then add sequence number.
		 if  (namingConvention != null) 
			 fileName = getDocumentName(objectName, namingConvention, exportDocs, invoiceNumber,fileNameList,loadNumberMap);
		 else 
			 fileName = getFileNameWithCopy(objectName, fileNameList, null, null);
		 
		 return fileName;
	}
	
	
	/**
	 * Method used for getting load number from the Control DB.
	 * 
	 * @param namingConvention
	 * @param exportDocs
	 * @param appName
	 * @param env
	 * @return
	 */
	public Map<String, String> getLoadNumberMapDetails(DdpDocnameConv namingConvention,DdpExportMissingDocs exportDocs,String appName,ApplicationProperties env) {
		
		Map<String, String> map =	getGenericLoadNumberMap(namingConvention, appName, env, exportDocs.getMisJobNumber(), exportDocs.getMisConsignmentId());
		
		if (namingConvention != null) {
			
			if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%SUPP_NAME%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%SUPP_NAME%%"))) {
					map.put("%%SUPP_NAME%%", exportDocs.getMisSuppName());
						
			}
			
			if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%C2C_NUM%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%C2C_NUM%%"))) {
					map.put("%%C2C_NUM%%", exportDocs.getMisC2cNum());
				}
		}
	
		return map; 
	}
	
	/**
	 * Method used for getting the load number details.
	 * 
	 * @param namingConvention
	 * @param appName
	 * @param env
	 * @param jobNumber
	 * @param consignmentID
	 * @return
	 */
	public Map<String,String> getGenericLoadNumberMap(DdpDocnameConv namingConvention,String appName,ApplicationProperties env,String jobNumber,String consignmentID) {
		
		Map<String, String> map = new HashMap<String, String>();
		int maxCount = 3;
		int sleepInSeconds = 3;
		
		if (namingConvention != null) {
			
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%CONSIGNEE_REF%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%CONSIGNEE_REF%%"))) {
					String query = env.getProperty("export.rule."+appName+".consigneeref.customerQuery"); 
					query = query.replaceAll("%%CONSIGNEE_REF%%", jobNumber);
					logger.info("For CONSIGNEE_REFERENCE : "+query);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							if (mapObj.get("JBCGRF") != null) {
								map.put("%%CONSIGNEE_REF%%", mapObj.get("JBCGRF").toString().trim());
								break;
							}
						}
					}
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the Consignment ID from Go2 enivnorment for CONSIGNEE_REF.", ex);
			}
			
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%SHIP_REF%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%SHIP_REF%%"))) {
					String query = env.getProperty("export.rule."+appName+".shipperref.customerQuery"); 
					query = query.replaceAll("%%JOBNUMBER%%", jobNumber);
					logger.info("For SHIPPER_REFERENCE : "+query);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							if (mapObj.get("JBSHRF") != null) {
								map.put("%%SHIP_REF%%", mapObj.get("JBSHRF").toString().trim());
								break;
							}
						}
					}
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the Consignment ID from Go2 enivnorment for SHIP_REF.", ex);
			}
		
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%CONS_LOAD_NUM%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%CONS_LOAD_NUM%%"))) {
					String query = env.getProperty("export.rule."+appName+".consignmentID.loadNumQuery"); 
					query = query.replaceAll("%%LOADNUMBER%%", consignmentID);
					logger.info("CONS_LOAD_NUM :"+query);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							if (mapObj.get("X4RFNO") != null) {
								map.put("%%CONS_LOAD_NUM%%", mapObj.get("X4RFNO").toString().trim());
								break;
							}
						}
					}
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the Consignment ID from Go2 enivnorment for CONS_LOAD_NUM.", ex);
			}
			
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%JOB_LOAD_NUM%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%JOB_LOAD_NUM%%"))) {
					
					String query = env.getProperty("export.rule."+appName+".jobNumber.loadNumQuery");
					query = query.replaceAll("%%LOADNUMBER%%", jobNumber);
					logger.info("JOB_LOAD_NUM :"+query);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							if (mapObj.get("X4RFNO") != null) {
								map.put("%%JOB_LOAD_NUM%%", mapObj.get("X4RFNO").toString().trim());
								break;
							}
						}
					}
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the JobNumber ID from Go2 enivnorment for JOB_LOAD_NUM.", ex);
			}
			
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%ITN_DATE%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%ITN_DATE%%"))) {
					
					String query = env.getProperty("export.rule."+appName+".JBDPDT.customQuery");
					query = query.replaceAll("%%CONSIGNMENTID%%", consignmentID);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							if (mapObj.get("JBDPDT") != null) {
								map.put("%%ITN_DATE%%", mapObj.get("JBDPDT").toString().trim());
								break;
							}
						}
					}
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the ITN_DATE(JDBPDT) from Go2 enivnorment.", ex);
			}
		
		
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%ITN_NUMBER%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%ITN_NUMBER%%"))) {
					
					String query = env.getProperty("export.rule."+appName+".ITNO.customQuery");
					query = query.replaceAll("%%CONSIGNMENTID%%", consignmentID);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							
							String 	fldValue="NOITN";
							
							if (mapObj.get("XXXTN#") != null) {						
								fldValue = mapObj.get("XXXTN#").toString().trim();						
								if (fldValue.startsWith("X")) 
									fldValue=fldValue.substring(1).trim();
								map.put("%%ITN_NUMBER%%",fldValue );
							} else {
								map.put("%%ITN_NUMBER%%", fldValue);
							}
							break;
						}
					}
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the ITN_NUMBER(XXXTN#) from Go2 enivnorment.", ex);
			}
			
			// *DIV reference begin
			try {
				if ((namingConvention.getDcvGenNamingConv() != null && namingConvention.getDcvGenNamingConv().toUpperCase().contains("%%DIV_REF%%")) || (namingConvention.getDcvDupDocNamingConv() != null && namingConvention.getDcvDupDocNamingConv().toUpperCase().contains("%%DIV_REF%%"))) {
					
					String query = env.getProperty("export.rule."+appName+".divref.customerQuery");
					query = query.replaceAll("%%CONSIGNMENTID%%", consignmentID);
					List<Map<String, Object>> resultSet = getControlMetaData(query, 0, maxCount, sleepInSeconds);
					String 	refValue="FAIL";
					if (resultSet != null) {
						for (Map<String,Object> mapObj : resultSet) {
							
							if (mapObj.get("X4RFNO") != null) {						
								refValue = mapObj.get("X4RFNO").toString().trim();						
							}
							break;
						}
					}
					map.put("%%DIV_REF%%", refValue);
				}
			} catch (Exception ex) {
				logger.error("AppName : "+appName+". CommonUtil.getLoadNumberMapDetails() - Unable to get the DIV_REF(X4RFNO) from Go3 enivnorment.", ex);
			}
			// *DIV reference ends
		}
		return map;
	}
	
	/**
	 * Method used for fetching the control meta data from control database.
	 * 
	 * @param query
	 * @param count
	 * @param maxCount
	 * @param sleepInSeconds
	 * @return
	 */
	public List<Map<String, Object>> getControlMetaData(String query,int count,int maxCount,int sleepInSeconds) {
		
		List<Map<String, Object>> resultSet = null;
		int queryTimeOut = 30;
		
		try {
			if (count == maxCount) {
				queryTimeOut = 120;
				sleepInSeconds = sleepInSeconds*3;
			}
			//System.out.println("Querty time out..... : "+queryTimeOut);
			controlJdbcTemplate.setQueryTimeout(queryTimeOut);
			resultSet = controlJdbcTemplate.queryForList(query);
			if (resultSet == null || resultSet.isEmpty()) {
				try {
					TimeUnit.SECONDS.sleep(sleepInSeconds);
				} catch (Exception e) {
					
				}
				if (count <= maxCount) {
					return getControlMetaData(query, ++count,maxCount,sleepInSeconds);
				}
			}
		} catch (Exception ex) {
			logger.error("CommonUtil.getControlMetaData() - Unable to connect control number times : "+count+" : Query : "+query,ex);
			try {
				TimeUnit.SECONDS.sleep(sleepInSeconds);
			} catch (Exception e) {
				
			}
			if (count <= maxCount) {
				return getControlMetaData(query, ++count,maxCount,sleepInSeconds);
			}
		}
		
		return resultSet;
	}
	
	/**
	 * Method used for connecting the FTP location.
	 * 
	 * @param ddpFtpClient
	 * @param ftpDetails
	 * @return
	 */
	public FTPSClient connectFTPS(DdpFTPSClient ddpFtpsClient,DdpCommFtp ftpDetails) {
		
		FTPSClient ftpsClient = null;
		String ftpPath = ftpDetails.getCftFtpLocation();
		String strFTPURL = null; 
		
		if (ftpPath.contains("/")) {
			
			ftpPath = ftpPath.substring(7, ftpPath.length());
			 String[] strArray = ftpPath.split("/");
			 strFTPURL = ftpPath.substring(0, strArray[0].length());
		}
		try {
			
			ftpsClient = ddpFtpsClient.connectFTPS(strFTPURL, ftpDetails.getCftFtpPort().intValue());
		} catch (Exception ex) {
			logger.error("DdpBackUpDocumentProcess- ConnectFTPS() : Unable to connect to FTPS location " , ex.getMessage());
		}
    	return ftpsClient;
	
	}
	
	/**
	 * Method used for fetching the rule details based on the SLA frequency.
	 * 
	 * @param query
	 * @return
	 */
	public List<DdpRuleDetail> fetchRuleDetailsBasedSLAFrequency(String query) {
		
		List<DdpRuleDetail> ruleDetails = null;
        
		try
        {
			ruleDetails = this.jdbcTemplate.query(query, new Object[] {}, new RowMapper<DdpRuleDetail>() {
            
				public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpRuleDetail rdtId = ddpRuleDetailService.findDdpRuleDetail(rs.getInt("RDT_ID"));
					return rdtId;
				}
			});
		}
        catch(Exception ex)
        {
        	logger.error("CommonUtil.fetchRuleDetailsBasedSLAFrequency() - Exception while retrieving  and Rule Details. Query is :  "+query,ex.getMessage());
        }
		
        return ruleDetails;
	}
	
	/**
	 * Method used for finding the categorized docs Base SLA frequency.
	 * 
	 * @param ruleDetailID
	 * @param startDate
	 * @return
	 */
	public List<Integer> findCategorizedDocForSLAFrequency(String ruleDetailID,Calendar startDate) {
	
		List<Integer> categorizedDocIds = null;
		try {
			categorizedDocIds = this.jdbcTemplate.query(Constant.DDP_SQL_SELECT_MATCHED_CATEGORIZED_DOCS_FOR_SLA,new Object[]{ruleDetailID,startDate}, new RowMapper<Integer>(){
				@Override
				public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
					Integer cat_id = rs.getInt("CAT_ID");
					return cat_id;
				}
	   	 	});
		} catch (Exception ex) {
			logger.error("CommonUtil.findCategorizedDocForSLAFrequency(Integer ruleDetailID,Calendar startDate) unable to categorized docs for Rule details id : "+ruleDetailID , ex.getMessage());
		}
		return categorizedDocIds;
	}
	
	
	/**
	 * 
	 * @param list
	 * @param startTag
	 * @param endTag
	 * @param separator
	 * @return
	 */
	public String joinString(Set<String> list,String startTag,String endTag,String separator) {
		
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
	 * @return
	 */
	public String joinString(List<String> list,String startTag,String endTag,String separator) {
		
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
	 * @return
	 */
	public String joinString(String[] list,String startTag,String endTag,String separator) {
		
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
	 * @param missDocID
	 * @param ruleID
	 * @param jobNumber
	 * @param consigmentID
	 * @param appName
	 * @param fileName
	 * @param fileSize
	 * @param typeOfService
	 * @param referenceNumber
	 * @param masterJobNumber
	 * @param dmsCreationDate
	 * @return
	 */
	public DdpExportSuccessReport constructExportReportDomainObject(Integer missDocID,Integer ruleID,String jobNumber,String consigmentID,
			String appName,String fileName,Integer fileSize,int typeOfService,String referenceNumber,String masterJobNumber,Calendar dmsCreationDate) {
		
		DdpExportSuccessReport report = new DdpExportSuccessReport();
		report.setEsrAppName(appName);
		report.setEsrMisId(missDocID);
		report.setEsrRuleId(ruleID);
		report.setEsrJobNumber(jobNumber);
		report.setEsrConsignmentId(consigmentID);
		report.setEsrFileName(fileName);
		report.setEsrFileSize((int)fileSize);
		report.setEsrCreationDate(GregorianCalendar.getInstance());
		report.setEsrDmsCreationDate(dmsCreationDate);
		report.setEsrExportTime(GregorianCalendar.getInstance());
		report.setEsrTypeOfService(typeOfService+"");
		report.setEsrReferenceNumber(referenceNumber);
		report.setEsrModeOfExport(masterJobNumber);
		
		return report;
	}
	
	
	/**
	 * 
	 * @param missingDocs
	 * @param fileSize
	 * @param fileName
	 * @param typeOfService
	 * @return
	 */
	public DdpExportSuccessReport constructExportReportDomainObject(DdpExportMissingDocs missingDocs,long fileSize,String fileName,int typeOfService) {
		
		DdpExportSuccessReport report = new DdpExportSuccessReport();
		
		report.setEsrAppName(missingDocs.getMisAppName());
		report.setEsrMisId(missingDocs.getMisId());
		report.setEsrRuleId(missingDocs.getMisExpRuleId());
		report.setEsrJobNumber(missingDocs.getMisJobNumber());
		report.setEsrConsignmentId(missingDocs.getMisConsignmentId());
		report.setEsrFileName(fileName);
		report.setEsrFileSize((int)fileSize);
		report.setEsrCreationDate(GregorianCalendar.getInstance());
		report.setEsrDmsCreationDate(missingDocs.getMisDmsRCreationDate());
		report.setEsrExportTime(GregorianCalendar.getInstance());
		report.setEsrTypeOfService(typeOfService+"");
		report.setEsrReferenceNumber(missingDocs.getMisEntryNo());
		report.setEsrModeOfExport(missingDocs.getMisMasterJob());
		
		return report;
	}
	
	/**
	 * 
	 * @param missingDocs
	 * @param fileSize
	 * @param fileName
	 * @param typeOfService
	 * @return
	 */
	public List<DdpExportSuccessReport> constructExportReportDomainObject(List<DdpExportMissingDocs> missingDocsList,long fileSize,String fileName,int typeOfService) {
		
		List<DdpExportSuccessReport> reportList = new ArrayList<DdpExportSuccessReport>();
		for (DdpExportMissingDocs missingDocs: missingDocsList) {
			
			DdpExportSuccessReport report = new DdpExportSuccessReport();
			
			report.setEsrAppName(missingDocs.getMisAppName());
			report.setEsrMisId(missingDocs.getMisId());
			report.setEsrRuleId(missingDocs.getMisExpRuleId());
			report.setEsrJobNumber(missingDocs.getMisJobNumber());
			report.setEsrConsignmentId(missingDocs.getMisConsignmentId());
			report.setEsrFileName(fileName);
			report.setEsrFileSize((int)fileSize);
			report.setEsrCreationDate(GregorianCalendar.getInstance());
			report.setEsrDmsCreationDate(missingDocs.getMisDmsRCreationDate());
			report.setEsrExportTime(GregorianCalendar.getInstance());
			report.setEsrTypeOfService(typeOfService+"");
			report.setEsrReferenceNumber(missingDocs.getMisEntryNo());
			report.setEsrModeOfExport(missingDocs.getMisMasterJob());
			reportList.add(report);
			
		}
		return reportList;
	}
	
	/**
	 * Method used for generating the reports for export module.
	 * 
	 * @param ddpScheduler
	 * @param startDate
	 * @param endDate
	 */
	public File generateReports(DdpScheduler ddpScheduler,Calendar startDate,Calendar endDate,Integer type,String typeOfStatus) {
		
		logger.info("CommonUtils.generateReports() - Invoked for scheduler Id : "+ddpScheduler.getSchId());
		File reportsFolder = null;
		String appName = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
		
		//Fetch the export rule details
		DdpExportRule exportRule = getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),"For Export the reports");
		
		if (exportRule == null || exportRule.getExpStatus() != 0) {
			logger.info("CommonUtils.generateReports() - DdpScheduler is inactive for id : "+ddpScheduler.getSchId());
			return reportsFolder;
		}
			
		if (ddpScheduler.getSchRuleCategory() != null && ddpScheduler.getSchRuleCategory().length() > 0) {
			appName = ddpScheduler.getSchRuleCategory();
		} else {
			List<DdpRuleDetail> ruleDetails = getAllRuleDetails(exportRule.getExpRuleId()); 
			appName = ruleDetails.get(0).getRdtPartyId();
		}
		
		if (typeOfStatus.equalsIgnoreCase(Constant.EXECUTION_STATUS_SUCCESS))
			reportsFolder = generateReportBasedOnExportReportTable(reportsFolder, ddpScheduler, exportRule, startDate, endDate, type, appName);
		else 
			reportsFolder = generateReportBasedCategorizedDocsTable(reportsFolder, ddpScheduler, exportRule, startDate, endDate, type, appName);
		
		return reportsFolder;
		
	}
	
	/**
	 * Method used for getting reports based on Export report table.
	 * 
	 * @param reportsFolder
	 * @param ddpScheduler
	 * @param exportRule
	 * @param startDate
	 * @param endDate
	 * @param type
	 * @param appName
	 * @return
	 */
	private File generateReportBasedOnExportReportTable(File reportsFolder,DdpScheduler ddpScheduler,
				DdpExportRule exportRule,Calendar startDate,Calendar endDate,Integer type,String appName) {
		
		
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
		appName = (appName.length() > 15) ? appName.substring(0, 15): appName ;
		
		//Based start date & end date fetch the record from ddp export success report table
		List<DdpExportSuccessReport> reports = fetchExportReportsBasedOnDates(exportRule.getExpRuleId(), startDate, endDate);
				
		//construct the excel file in the temp location (naming conversion will be  client_id for rule by client id & appname rule by query .
				try {
					reportsFolder = generateExcelReport(reports, reportsFolder, appName.replaceAll("[^a-zA-Z0-9_.]", ""), startDate, endDate,type);
					
					//send the mail (mailing template)
					DdpNotification notification = exportRule.getExpNotificationId();
					String smtpAddress = env.getProperty("mail.smtpAddress");
					
					String toAddress = null;
					String ccAddress = null;
					
					if (type == 1) {
						toAddress = notification.getNotSuccessEmailAddress();
						ccAddress = notification.getNotFailureEmailAddress();
					} else {
						toAddress = ddpScheduler.getSchReportEmailTo();
						ccAddress = ((ddpScheduler.getSchReportEmailCc() == null || ddpScheduler.getSchReportEmailCc().isEmpty())? null : ddpScheduler.getSchReportEmailCc()) ;
					}
					
					String fromAddress = env.getProperty("export.reports.formAddress");
					
					String subject = env.getProperty("export.reports.subject");
					
					if (type == 1)
						subject = subject.replaceAll("%%STATUS%%", "OnDemand");
					else 
						subject = subject.replaceAll("%%STATUS%%", "General");
					
					subject = subject.replaceAll("%%DDPCLIENTID%%", appName);
					subject = subject.replaceAll("%%FROMDATE%%", dateFormat.format(startDate.getTime()));
					subject = subject.replaceAll("%%TODATE%%", dateFormat.format(endDate.getTime()));
					
					String body = env.getProperty("export.reports.body");
					
					if (type == 1)
						body = body.replaceAll("%%STATUS%%", "OnDemand");
					else 
						body = body.replaceAll("%%STATUS%%", "General");
					
					body = body.replaceAll("%%DDPCLIENTID%%", appName);
					body = body.replaceAll("%%FROMDATE%%", dateFormat.format(startDate.getTime()));
					body = body.replaceAll("%%TODATE%%", dateFormat.format(endDate.getTime()));
					body = body.replaceAll("%%EXEDATE%%", dateFormat.format(new Date()));
					body = body.replaceAll("%%DOCCOUNT%%", ((reports != null)?reports.size() : 0)+"");
					body = body.replaceAll("%%DDPCLIENTDESC%%", exportRule.getDdpRule().getRulDescription());
					
					if (type != 1)
						taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, reportsFolder);
					
				} catch (Exception ex) {
					logger.error("CommonUtil.generateReports() - Unable to generate report for scheduler id : "+ddpScheduler.getSchId(), ex);
				} 
				
				return reportsFolder;
	}
	
	/**
	 * 
	 * @param reportsFolder
	 * @param ddpScheduler
	 * @param exportRule
	 * @param startDate
	 * @param endDate
	 * @param type
	 * @param appName
	 * @return
	 */
	private File generateReportBasedCategorizedDocsTable(File reportsFolder,DdpScheduler ddpScheduler,
			DdpExportRule exportRule,Calendar startDate,Calendar endDate,Integer type,String appName) {
		
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
		appName = (appName.length() > 15) ? appName.substring(0, 15) : appName;
		
		List<DdpExportMissingDocs> missingDocs = null;
		List<DdpCategorizedDocs> categorizedDocs = null;
		
		if (ddpScheduler.getSchRuleCategory() != null && ddpScheduler.getSchRuleCategory().length() > 0) {
			missingDocs = fetchExportMissingDocReportsBasedOnDates(exportRule.getExpRuleId(), startDate, endDate);
		} else {
			categorizedDocs = fetchCategorizedDocExportReportsBasedOnDates(exportRule.getExpRuleId(), startDate, endDate);
		}
		
		try {
			reportsFolder = generateExcelReportForCategorizedDocsTable(categorizedDocs, missingDocs, reportsFolder, appName, startDate, endDate, type);
			//send the mail (mailing template)
			DdpNotification notification = exportRule.getExpNotificationId();
			String smtpAddress = env.getProperty("mail.smtpAddress");
			
			String toAddress = null;
			String ccAddress = null;
			
			if (type == 1) {
				toAddress = notification.getNotSuccessEmailAddress();
				ccAddress = notification.getNotFailureEmailAddress();
			} else {
				toAddress = ddpScheduler.getSchReportEmailTo();
				ccAddress = ((ddpScheduler.getSchReportEmailCc() == null || ddpScheduler.getSchReportEmailCc().isEmpty())? null : ddpScheduler.getSchReportEmailCc()) ;
			}
			
			String fromAddress = env.getProperty("export.reports.formAddress");
			
			String subject = env.getProperty("export.reports.subject");
			
			if (type == 1)
				subject = subject.replaceAll("%%STATUS%%", "OnDemand");
			else 
				subject = subject.replaceAll("%%STATUS%%", "General");
			
			subject = subject.replaceAll("%%DDPCLIENTID%%", appName);
			subject = subject.replaceAll("%%FROMDATE%%", dateFormat.format(startDate.getTime()));
			subject = subject.replaceAll("%%TODATE%%", dateFormat.format(endDate.getTime()));
			
			String body = env.getProperty("export.reports.body");
			
			if (type == 1)
				body = body.replaceAll("%%STATUS%%", "OnDemand");
			else 
				body = body.replaceAll("%%STATUS%%", "General");
			
			body = body.replaceAll("%%DDPCLIENTID%%", appName);
			body = body.replaceAll("%%FROMDATE%%", dateFormat.format(startDate.getTime()));
			body = body.replaceAll("%%TODATE%%", dateFormat.format(endDate.getTime()));
			body = body.replaceAll("%%EXEDATE%%", dateFormat.format(new Date()));
			int count = (missingDocs != null ? missingDocs.size() : (categorizedDocs == null? 0 :categorizedDocs.size()));
			body = body.replaceAll("%%DOCCOUNT%%", count+"");
			
			if (type != 1)
				taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, reportsFolder);
			
	} catch (Exception ex) {
		logger.error("CommonUtil.generateReports() - Unable to generate report for scheduler id : "+ddpScheduler.getSchId(), ex);
	} 
		
		
		return reportsFolder;
	}
	
	/**
	 * Method used for generate excel report.
	 * 
	 * @param reports
	 * @param reportsFolder
	 * @param appName
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public File generateExcelReportForCategorizedDocsTable(List<DdpCategorizedDocs> catList,List<DdpExportMissingDocs> misList,File reportsFolder,String appName,Calendar startDate,Calendar endDate,Integer type) {
		
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
		
		try {
			
				
				String tempFilePath = env.getProperty("ddp.export.folder");
				String tempFolderPath = null;
								
				tempFolderPath = tempFilePath + "/temp_" + appName + "_report_" + dateFormat.format(new Date()); 
				reportsFolder = FileUtils.createFolder(tempFolderPath);
				
				FileWriter csvFile = new FileWriter(reportsFolder+"/"+appName+"_"+dateFormat.format(startDate.getTime())+"-"+dateFormat.format(endDate.getTime())+".csv");
				csvFile.append("Document Type");
				csvFile.append(",");
				csvFile.append("MasterJob Number");
				csvFile.append(",");
				csvFile.append("Job Number");
				csvFile.append(",");
				csvFile.append("Consignment ID");
				csvFile.append(",");
				csvFile.append("Reference Number");
				csvFile.append(",");
				csvFile.append("Created Date");
				csvFile.append(",");
				csvFile.append("Status");
				csvFile.append(",");
				csvFile.append("\n");
				
				if (catList != null) {
					for (DdpCategorizedDocs report : catList) {
						
						List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(report.getCatDtxId().getDtxId());
						
						csvFile.append(ddpDmsDocsDetails.get(0).getDddControlDocType());
						csvFile.append(",");
						csvFile.append(ddpDmsDocsDetails.get(0).getDddMasterJobNumber());
						csvFile.append(",");
						csvFile.append(ddpDmsDocsDetails.get(0).getDddJobNumber());
						csvFile.append(",");
						csvFile.append(ddpDmsDocsDetails.get(0).getDddConsignmentId());
						csvFile.append(",");
						csvFile.append(ddpDmsDocsDetails.get(0).getDddDocRef());
						csvFile.append(",");
						csvFile.append(dateFormat.format(report.getCatCreatedDate().getTime()));
						csvFile.append(",");
						csvFile.append(report.getCatStatus()+"");
						csvFile.append("\n");
					}
				}
				
				if (misList != null) {
					
					for (DdpExportMissingDocs report : misList) {
						
						csvFile.append(report.getMisDocType());
						csvFile.append(",");
						csvFile.append(report.getMisMasterJob());
						csvFile.append(",");
						csvFile.append(report.getMisJobNumber());
						csvFile.append(",");
						csvFile.append(report.getMisConsignmentId());
						csvFile.append(",");
						csvFile.append(report.getMisEntryType());
						csvFile.append(",");
						csvFile.append(dateFormat.format(report.getMisCreatedDate().getTime()));
						csvFile.append(",");
						csvFile.append(report.getMisStatus()+"");
						csvFile.append("\n");
					}
				}
				csvFile.flush();
				csvFile.close();
				
		} catch (Exception ex) {
			logger.error("CommonUtil.generateExcelReport() - Unable to generate excel file", ex);
		}
		return reportsFolder;
	}
	
	/**
	 * Method used for generate excel report.
	 * 
	 * @param reports
	 * @param reportsFolder
	 * @param appName
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public File generateExcelReport(List<DdpExportSuccessReport> reports,File reportsFolder,String appName,Calendar startDate,Calendar endDate,Integer type) {
		
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MMM-dd HH-mm-ss");
		
		try {
			if (reports.size() > 0 || type == 1) {
				
				String tempFilePath = env.getProperty("ddp.export.folder");
				String tempFolderPath = null;
								
				tempFolderPath = tempFilePath + "/temp_" + appName + "_report_" + dateFormat.format(new Date()); 
				reportsFolder = FileUtils.createFolder(tempFolderPath);
				
				FileWriter csvFile = new FileWriter(reportsFolder+"/"+appName+"_"+dateFormat.format(startDate.getTime())+"-"+dateFormat.format(endDate.getTime())+".csv");
				csvFile.append("Exported Date");
				csvFile.append(",");
				csvFile.append("MasterJob Number");
				csvFile.append(",");
				csvFile.append("Job Number");
				csvFile.append(",");
				csvFile.append("Consignment ID");
				csvFile.append(",");
				csvFile.append("Reference Number");
				csvFile.append(",");
				csvFile.append("File Name");
				csvFile.append(",");
				csvFile.append("File Size");
				csvFile.append(",");
				csvFile.append("\n");
				
				for (DdpExportSuccessReport report : reports) {
					
					csvFile.append(dateFormat.format(report.getEsrCreationDate().getTime()));
					csvFile.append(",");
					csvFile.append(report.getEsrModeOfExport());
					csvFile.append(",");
					csvFile.append(report.getEsrJobNumber());
					csvFile.append(",");
					csvFile.append(report.getEsrConsignmentId());
					csvFile.append(",");
					csvFile.append(report.getEsrReferenceNumber());
					csvFile.append(",");
					csvFile.append(report.getEsrFileName());
					csvFile.append(",");
					csvFile.append(report.getEsrFileSize()+"");
					csvFile.append("\n");
				}
				csvFile.flush();
				csvFile.close();
			}
		} catch (Exception ex) {
			logger.error("CommonUtil.generateExcelReport() - Unable to generate excel file", ex);
		}
		return reportsFolder;
	}
	
	/**
	 * 
	 * @param ruleID
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DdpExportSuccessReport> fetchExportReportsBasedOnDates(Integer ruleID,Calendar startDate,Calendar endDate) {
		
		List<DdpExportSuccessReport> list = jdbcTemplate.query(Constant.DDP_SQL_SELECT_EXPORT_REPORTS_BASED_DATE_RANGE, new Object[] {ruleID,startDate,endDate},new RowMapper<DdpExportSuccessReport>(){

			@Override
			public DdpExportSuccessReport mapRow(ResultSet rs, int rowNum)
					throws SQLException {
				
				DdpExportSuccessReport exportReports = ddpExportSuccessReportService.findDdpExportSuccessReport(rs.getInt("ESR_ID"));
				return exportReports;
			}
			
		});
		return list;
	}
	
	/**
	 * Method used for fetching the ddp categorized docs details from table.
	 * 
	 * @param ruleID
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DdpCategorizedDocs> fetchCategorizedDocExportReportsBasedOnDates(Integer ruleID,Calendar startDate,Calendar endDate) {
		
		List<DdpCategorizedDocs> list =  jdbcTemplate.query(Constant.DDP_SQL_SELECT_EXPORT_REPORTS_CATEGORIZED_DOCS,new Object[] {ruleID,startDate,endDate},new RowMapper<DdpCategorizedDocs>() {

			@Override
			public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum)
					throws SQLException {
				
				DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
				return ddpCategorizedDocs;
			}
			
		});
		
		return list;
		
	}
	
	/**
	 * Method used for getting the multiple Email ids for Consolidated AED rules.
	 * 
	 * @param communicationID
	 * @return
	 */
	public List<DdpMultiEmails> getMultiEmailsByCmsID(int communicationID)
	{
		List<DdpMultiEmails> ddpMultiEmails = this.jdbcTemplate.query("SELECT * FROM DDP_MULTI_EMAILS WHERE MES_CMS_ID = ?", new Object[]{communicationID},new RowMapper<DdpMultiEmails>(){
			@Override
			public DdpMultiEmails mapRow(ResultSet rs, int rowNum) throws SQLException {
				DdpMultiEmails multiEmails = new DdpMultiEmails();
				multiEmails.setMesEmailId(rs.getInt("MES_EMAIL_ID"));
				multiEmails.setMesEmailTo(rs.getString("MES_EMAIL_TO"));
				multiEmails.setMesEmailCc(rs.getString("MES_EMAIL_CC"));
				multiEmails.setMesDestCompany(rs.getString("MES_DEST_COMPANY"));
				multiEmails.setMesDestRegion(rs.getString("MES_DEST_REGION"));
				return multiEmails;
			}
		});
		return ddpMultiEmails;
	}
	
	/**
	 * Method used for fetching the export missing docs from the table.
	 * 
	 * @param ruleID
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public List<DdpExportMissingDocs> fetchExportMissingDocReportsBasedOnDates(Integer ruleID,Calendar startDate,Calendar endDate) {
		
		List<DdpExportMissingDocs> list = jdbcTemplate.query(Constant.DDP_SQL_SELECT_MIS_EXPORT_MISSING_DOCS, new Object[] {ruleID,startDate,endDate},new RowMapper<DdpExportMissingDocs>() {

			@Override
			public DdpExportMissingDocs mapRow(ResultSet rs, int rowNum)
					throws SQLException {
				
				DdpExportMissingDocs missingDocs = ddpExportMissingDocsService.findDdpExportMissingDocs(rs.getInt("MIS_ID"));
				
				return missingDocs;
			}
			
		});
		
		return list;
	}
	
	 /**
	   * 
	   * @param str
	   * @param c
	   * @param n
	   * @return
	   */
	 public  int nthOccurrence(String str, char c, int n) {
		    int pos = str.indexOf(c, 0);
		    while (n-- > 0 && pos != -1)
		        pos = str.indexOf(c, pos+1);
		    return pos;
	}
	   
	/**
	 * Method used for sending the email for export module.
	 * 
	 * @param sourceFolder
	 * @param ddpCommEmail
	 * @return
	 */
	public boolean sendEmailForExportModule(File sourceFolder,DdpCommEmail ddpCommEmail,String company,String clientID,
			Calendar fromDate,Calendar toDate,String typeOfService) {
		
		logger.info("Inside the CommonUtil.sendEmailForExportModule() invoked"); 
		boolean isMailSend = false;
		
		String body = null;
		String subject = null;
		body = env.getProperty("email.export.body."+company);
		
				
		if (body == null) 
			body = env.getProperty("email.export.body");
		
		subject = env.getProperty("email.export.subject."+company); 
		if (subject == null)
			subject = env.getProperty("email.export.subject");
		
		if (subject != null && body != null) {
		
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
			int totalFileCount = 0;
			String smtpAddress = env.getProperty("mail.smtpAddress");
			
			subject = subject.replaceAll("%%DDPClIENTID%%", clientID);
		
			body = body.replaceAll("%%DDPCLIENTID%%", clientID);
			body = body.replaceAll("%%TYPEOFSERVICE%%", env.getProperty(typeOfService));
			body = body.replaceAll("%%FROMDATE%%", dateFormat.format(fromDate.getTime()));
			body = body.replaceAll("%%TODATE%%", dateFormat.format(toDate.getTime()));
			
			if (sourceFolder != null) {
				File[] listOfFiles = sourceFolder.listFiles();
				if (listOfFiles != null) 
					totalFileCount = listOfFiles.length; 
			}
			
			body = body.replaceAll("%%DOCCOUNT%%", totalFileCount+"");
			logger.info("CommonUtil.sendEmailForExportModule() - sending the email for : "+ddpCommEmail.getCemEmailTo());
			int sendEmail = taskUtil.sendMail(smtpAddress, ddpCommEmail.getCemEmailTo(), ddpCommEmail.getCemEmailCc(), 
					ddpCommEmail.getCemEmailBcc(),( ddpCommEmail.getCemEmailFrom() == null || ddpCommEmail.getCemEmailFrom().isEmpty() ? env.getProperty("mail.fromAddress") : ddpCommEmail.getCemEmailFrom()), ddpCommEmail.getCemEmailReplyTo(), subject, body, sourceFolder);
			
			if (sendEmail == 1)
				isMailSend = true;
		}
		
		
		return isMailSend;
	}
	
	/**
	 * Method used for filtering the document type based on the branches.
	 * 
	 * @param ruleDetails
	 * @param uniqueBraches
	 * @param uniqueDocType
	 */
	public void filterDocTypesBasedOnRuleDetails(List<DdpRuleDetail> ruleDetails,Map<String, List<String>> uniqueBraches,Map<String, List<DdpRuleDetail>> uniqueDocType) {
		
		for (DdpRuleDetail ddpRuleDetail : ruleDetails) {
				
	    		if (!uniqueBraches.containsKey(ddpRuleDetail.getRdtDocType().getDtyDocTypeName())) {
	    			List<String> branchNames = new ArrayList<String>();
	    			branchNames.add(ddpRuleDetail.getRdtBranch().getBrnBranchCode());
	    			List<DdpRuleDetail> ruleDetailSet = new ArrayList<DdpRuleDetail>();
	    			ruleDetailSet.add(ddpRuleDetail);
	    			uniqueBraches.put(ddpRuleDetail.getRdtDocType().getDtyDocTypeName(), branchNames);
	    			uniqueDocType.put(ddpRuleDetail.getRdtDocType().getDtyDocTypeName(), ruleDetailSet);
	    		} else {
	    			List<String> branchNames = uniqueBraches.get(ddpRuleDetail.getRdtDocType().getDtyDocTypeName());
	    			branchNames.add(ddpRuleDetail.getRdtBranch().getBrnBranchCode());
	    			List<DdpRuleDetail> ruleDetailSet = uniqueDocType.get(ddpRuleDetail.getRdtDocType().getDtyDocTypeName());
	    			ruleDetailSet.add(ddpRuleDetail);
	    			uniqueBraches.put(ddpRuleDetail.getRdtDocType().getDtyDocTypeName(), branchNames);
	    			uniqueDocType.put(ddpRuleDetail.getRdtDocType().getDtyDocTypeName(), ruleDetailSet);
	    		}
			}
	}
	
	/**
	 * Method used for getting the transfer object details based rule details.
	 * 
	 * @param ruleDetails
	 * @param ddpTransferObjects
	 * @return
	 */
	public DdpTransferObject getTransferObjectBasedRuleDetails(List<DdpRuleDetail> ruleDetails,List<DdpTransferObject> ddpTransferObjects) {
		
		logger.info("Inside the CommonUtil.getTrnasferObjectBasedRuleDetails()");
		DdpTransferObject ddpTransferObject = null;
		if (ruleDetails.size() > 0) {
		    		
		    	DdpCommunicationSetup commSetup = getMatchedCommunicationSetup(ruleDetails.get(0).getRdtId());
		    		
		    	ddpTransferObjects.addAll(ddpTransferFactory.constructTransferObject(commSetup));
		    			
		    	if (ddpTransferObjects != null) {
		    		for (DdpTransferObject transferObject : ddpTransferObjects) {
		    			if (transferObject.isConnected()) {
							ddpTransferObject = transferObject;
							break;
						}
					}
		    	}
		    }
		logger.info("End of CommonUtil.getTrnasferObjectBasedRuleDetails() & DdpTrnsferObject ID : "+(ddpTransferObject == null? "null" : ddpTransferObject.getTypeOfConnection())+" : for Rule detail ID : "+ ruleDetails.get(0).getRdtId());
		
		return ddpTransferObject;
	}
	
	
	/**
	 * 
	 * @param expRuleID
	 * @return {@link DdpCommunicationSetup}
	 */
	public DdpCommunicationSetup getMatchedCommunicationSetup(Integer expRuleID) {
		
		DdpCommunicationSetup setup = null;
		
		logger.debug("CommonUtils.getMatchedCommunicationSetup() method invoked");
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
			logger.error("CommonUtils.getMatchedCommunicationSetup() - Exception while retrieving Matched  Export id - Error message [{}].",ex.getMessage());
			ex.printStackTrace();
		}
		
		logger.info("CommonUtils.getMatchedCommunicationSetup() executed successfully.");
		return setup;
		
	}
	
	/**
	 * Method used for getting the generating setup only.
	 * 
	 * @param ddpRuleDetailID
	 * @return
	 */
	public DdpGenSourceSetup getMatchedGeneratedSourceSetup(Integer ddpRuleDetailID)  {
		
		DdpGenSourceSetup setup = null;
		logger.debug("CommonUtils.getMatchedGeneratedSourceSetup() - For Rule Detail ID : "+ddpRuleDetailID);
		
		try {
			List<DdpGenSourceSetup> setupList =  this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_GENERATING_SOURCE, new Object[]{ddpRuleDetailID}, new RowMapper<DdpGenSourceSetup>(){

				@Override
				public DdpGenSourceSetup mapRow(ResultSet rs, int arg1) throws SQLException {
					
					String gssOption = rs.getString("GSS_OPTION");
					DdpGenSourceSetup GenSourcesetup = null;
					
					if (gssOption != null) {
						GenSourcesetup = new DdpGenSourceSetup();
						GenSourcesetup.setGssOption(gssOption);
					}
					return GenSourcesetup;
				}
				 
			 });
			
			
			if (setupList != null && setupList.size() > 0)
				setup = setupList.get(0);
		} catch (Exception ex) {
			logger.error("CommonUtils.getMatchedGeneratedSourceSetup() - Exception while retrieving Matched  Export id - Error message [{}].",ex.getMessage());
		}
		return setup;
	}
	
	
	/**
	 * Getting the details for Rate setup
	 * @param ddpRuleDetailID
	 * @return
	 */
	public List<DdpRateSetupEntity> getMatchedRateSetup(Integer ddpRuleDetailID)  {
		
	
		logger.debug("CommonUtils.getMatchedRateSetup() - For Rule Detail ID : "+ddpRuleDetailID);
		
			List<DdpRateSetupEntity> setupList = new ArrayList<DdpRateSetupEntity>();
			try {
				setupList =  this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_RATE_SETUP, new Object[]{ddpRuleDetailID}, new DdpRateSetupEntity());
			} catch (Exception ex) {
				logger.error("CommonUtils.getMatchedRateSetup() - Error occuried for  Rule Detail ID : "+ddpRuleDetailID, ex);
			}
		return setupList;
	}	
	
	/**
	 * Method used for getting the Query Based UI Using Export Rule ID.
	 * 
	 * @param expRuleID
	 * @return
	 */
	public List<DdpExportQueryUi> getExportQueryUiByRuleID(int expRuleID)
	   {
		   List<DdpExportQueryUi> exportQueryUis = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_EXPORT_QUERY_UI_BY_RUL_ID, new Object[]{expRuleID},new RowMapper<DdpExportQueryUi>(){
				@Override
				public DdpExportQueryUi mapRow(ResultSet rs, int rowNum) throws SQLException {
					DdpExportQueryUi exportQueryUi = ddpExportQueryUiService.findDdpExportQueryUi(rs.getInt("EQI_ID"));
					return exportQueryUi;
				}
			   });
		   return exportQueryUis;
	   }
	
	 /**
	  * Method used for getting the Query Based on Text area using Export Rule ID.
	  * 
	  * @param expRuleID
	  * @return
	  */
	 public List<DdpExportQuery> getExportQueryByRuleID(int expRuleID)
	   {
		   List<DdpExportQuery> exportQuerys = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_EXPORT_QUERY_BY_RUL_ID, new Object[]{expRuleID},new RowMapper<DdpExportQuery>(){
				@Override
				public DdpExportQuery mapRow(ResultSet rs, int rowNum) throws SQLException {
					DdpExportQuery exportQuery = ddpExportQueryService.findDdpExportQuery(rs.getInt("EXQ_ID"));
					return exportQuery;
				}
			   });
		   return exportQuerys;
	   }
	
	/**
	 * Method used for constructing the Query with UI details.
	 * 
	 * @param queryUis
	 * @return
	 */
	public String constructQueryWithExportQueryUIs(List<DdpExportQueryUi> queryUis)
	{
		String query = env.getProperty("export.ruleByQry.querystart").trim();
		for(DdpExportQueryUi exportQueryUi: queryUis)
		{
			if(exportQueryUi.getEqiOperator() != null)
				query+=" "+exportQueryUi.getEqiOperator();
			query +=" "+env.getProperty("export.rule.ruleByQry.begin")+partyService.findDdpParty(exportQueryUi.getEqiPartyCode()).getPtyPartyName()+" IN ("+joinString(exportQueryUi.getEqiPartyValue().split(","),"'","'",",")+")";
		}
		return query;
	}
	
	/**
	 * Method used for constructing the Query with UI details. First checks based on the appName. 
	 * 
	 * @param queryUis
	 * @param appName
	 * @return
	 */
	public String constructQueryWithExportQueryUIs(List<DdpExportQueryUi> queryUis, String key,String defaultKey)
	{
		String query = env.getProperty(key,defaultKey,key);
		String dynamicValues = "";
		
		if (queryUis != null && queryUis.size() > 0) {
			dynamicValues += "(";
			for(DdpExportQueryUi exportQueryUi: queryUis)
			{
				if(exportQueryUi.getEqiOperator() != null)
					dynamicValues+=""+exportQueryUi.getEqiOperator();
				dynamicValues +=" "+env.getProperty("export.rule.ruleByQry.begin")+partyService.findDdpParty(exportQueryUi.getEqiPartyCode()).getPtyPartyName()+" IN ("+joinString(exportQueryUi.getEqiPartyValue().split(","),"'","'",",")+")";
			}
			dynamicValues += ")";
		}
		
		query = query.replaceAll("%%DYNAMICPARTYIDS%%",dynamicValues);
		
		return query;
	}
	
	/**
	 * Method used for constructing the Query using the details entered text area.
	 * 
	 * @param queryFromTXT
	 * @return
	 */
	public String constructQueryFromTXT(String queryFromTXT)
	{
		List<DdpParty> parties =  partyService.findAllDdpPartys();
		for(DdpParty party:parties)
			queryFromTXT = queryFromTXT.replaceAll(party.getPtyPartyName(), env.getProperty("export.rule.ruleByQry.begin")+party.getPtyPartyName());
		String strquery = env.getProperty("export.ruleByQry.querystart").trim();
		if(! queryFromTXT.startsWith(strquery))
			queryFromTXT = strquery+queryFromTXT+" )";
		return queryFromTXT;
	}
	
	/**
	 * Method used for constructing the Query using the details entered text area.
	 * 
	 * @param queryFromTXT
	 * @param appName
	 * @return
	 */
	public String constructQueryFromTXT(String queryFromTXT,String appName)
	{
		List<DdpParty> parties =  partyService.findAllDdpPartys();
		for(DdpParty party:parties)
			queryFromTXT = queryFromTXT.replaceAll(party.getPtyPartyName(), env.getProperty("export.rule.ruleByQry.begin")+party.getPtyPartyName());
		String strquery = env.getProperty("export.ruleByQry.querystart").trim();
		if(! queryFromTXT.startsWith(strquery))
			queryFromTXT = strquery+queryFromTXT+" )";
		
		if (queryFromTXT.contains("*"))
			queryFromTXT = queryFromTXT.replaceAll("*", env.getProperty("export.rule.customQuery.metaData"));
		
		return queryFromTXT;
	}
	
	
	/* ================================================= Monitor Jobs ========================================= */
	/**
	 * Method used for fetching the job reference holder details.
	 * 
	 * @return
	 */
	public List<DdpJobRefHolder> fetchJobRefHolder() {
			
		List<DdpJobRefHolder> jobRefs = null;
			jobRefs = ddpJobRefHolderService.findAllDdpJobRefHolders();
		return jobRefs;
	}
	
	/**
	 * Method used for adding the consolidated AED thread.
	 * 
	 * @param threadName
	 */
	public void addConsolidatedAEDThread(String threadName) {
		try {
			consolidatedThreadMap.put(threadName, new Date());
		} catch (Exception ex) {
			logger.error("CommonUtil.addConsolidatedAEDThread() - unable to add thread name "+threadName, ex);
		}
	}
	
	/**
	 * Method used for removing the consolidated AED thread.
	 * 
	 * @param threadName
	 */
	public void removeConsolidatedAEDThread(String threadName) {
		try {
			if (consolidatedThreadMap.containsKey(threadName)) 
				consolidatedThreadMap.remove(threadName);
		} catch (Exception ex) {
			logger.error("CommonUtil.removeConsolidatedAEDThread() - unable to remove thread name "+threadName, ex);
		}
	}
	
	/**
	 * Method used for adding the export setup thread.
	 * 
	 * @param threadName
	 */
	public void addExportSetupThread(String threadName) {
		try {
			exportThreadMap.put(threadName, new Date());
		} catch (Exception ex) {
			logger.error("CommonUtil.addExportSetupThread() - unable to add thread name "+threadName, ex);
		}
	}
	
	/**
	 * Method used for removing the export setup thread.
	 * 
	 * @param threadName
	 */
	public void removeExportSetupThread(String threadName) {
		try {
			if (exportThreadMap.containsKey(threadName)) 
				exportThreadMap.remove(threadName);
		} catch (Exception ex) {
			logger.error("CommonUtil.addExportSetupThread() - unable to add thread name "+threadName, ex);
		}
	}
	
	
	/**
	 * @return the consolidatedThreadMap
	 */
	public Map<String, Date> getConsolidatedThreadMap() {
		return consolidatedThreadMap;
	}

	/**
	 * @return the exportThreadMap
	 */
	public Map<String, Date> getExportThreadMap() {
		return exportThreadMap;
	}
	
	/* ================================================================================================== */
}
