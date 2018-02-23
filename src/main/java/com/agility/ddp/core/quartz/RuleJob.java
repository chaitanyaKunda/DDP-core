package com.agility.ddp.core.quartz;

import java.io.File;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.components.DdpDFCClientComponent;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.core.util.SecurityUtils;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommEmailService;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DmsDdpSynService;
import com.documentum.fc.client.IDfSession;

@Configuration
//@PropertySource({"file:///E:/DDPConfig/custom.properties","file:///E:/DDPConfig/mail.properties","file:///E:/DDPConfig/ddp.properties"})
public class RuleJob extends QuartzJobBean {

	private static final Logger logger = LoggerFactory.getLogger(RuleJob.class);
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	TaskUtil taskUtil;
	
	@Autowired
	DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	DdpCommEmailService ddpCommEmailService;
	
	@Autowired
	DdpRuleDetailService ddpRuleDetailService;
	
	@Autowired
	DmsDdpSynService ddpSynService;
	
	@Autowired
	DdpCategorizedDocsService categorizedDocsService;
	
//	 @Value( "${ddp.export.folder}" )
//	 private String tempFilePath;
	 
	JdbcTemplate controlJdbcTemplate;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private	DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private CommonUtil commonUtil;
	
	public JdbcTemplate getControlJdbcTemplate() {
		return controlJdbcTemplate;
	}

	public void setControlJdbcTemplate(JdbcTemplate controlJdbcTemplate) {
		this.controlJdbcTemplate = controlJdbcTemplate;
	}
	public RuleJob() {

	}
	public RuleJob(DdpCategorizedDocsService ddpCategorizedDocsService2,
			DdpCommEmailService ddpCommEmailService2, TaskUtil taskUtil,ApplicationProperties env) {
		ddpCategorizedDocsService = ddpCategorizedDocsService2;
		ddpCommEmailService = ddpCommEmailService2;
		this.taskUtil = taskUtil;
		this.env = env;
	}

	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		// TODO Auto-generated method stub

	}
	//@AuditLog(dbAuditBefore=true, message="RuleJob.callWorkFlow(categorizedDocs) Method Invoked.")
	public void callWorkFlow(Object[] categorizedDocs) {

		Scheduler scheduler = null;
		DdpCategorizedDocs ddpCategorizedDoc = (DdpCategorizedDocs) categorizedDocs[0];
		logger.info("RuleJob.callWorkFlow(categorizedDocs) Method Invoked to serve "+ddpCategorizedDoc.getCatId()+" which belongs to "+ddpCategorizedDoc.getCatRuleType());
		Integer typeOfService = null;
		if (categorizedDocs.length >= 3)  {
			scheduler =  (Scheduler)categorizedDocs[1];
			typeOfService = Integer.parseInt(categorizedDocs[2].toString());
		}
		// DdpCategorizedDocs dummy = (DdpCategorizedDocs)categorizedDocs.;
		// List<DdpCategorizedDocs> ddpCategorizedDocs =
		// ddpCategorizedDocsService.findAllDdpCategorizedDocses();
		String fileName = null;
		Integer commId = null;
		String commProtocolId = null;
		String commProtocol = null;
		String ruleType = null;
		String extension = null;
		String primaryFileName = null;
		String indicatorFileName = null;
		File indicatorFolder = null;
		String tempFilePath = env.getProperty("ddp.export.folder");
		
		Date date = new Date() ;
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
    	String tempFolderPath = tempFilePath+"/temp_aedrule_"+ddpCategorizedDoc.getCatId() +"_" + dateFormat.format(date);
    	File tmpFile = new File(tempFolderPath);
    	tmpFile.mkdirs();
    	
		try {
			// Set the job status in progress
			/**
			 * Job not created = 0 Job created but failed = -1 Job created and
			 * processed success = 1 Job created in progress = 100
			 */
			// Job created in progress = 100
			
			List<DdpDmsDocsDetail> ddpDmsDocsDetails = taskUtil.getDdpDmsDocsDetail(ddpCategorizedDoc.getCatDtxId().getDtxId());
			DdpRuleDetail ddpRuleDetail = ddpRuleDetailService.findDdpRuleDetail(ddpCategorizedDoc.getCatRdtId());
			
			if (typeOfService != null && typeOfService.intValue() == 1) {
				//Checking AED rule, document is triggered with same details before 24 hours. if mail send then request user to process.
				boolean isRecordProcessed = checkAEDMailAlreadySend(ddpCategorizedDoc, ddpDmsDocsDetails.get(0), ddpRuleDetail);
				if (isRecordProcessed) {
					//CAT status is set 4 due to record need process by the printed user only.
					ddpCategorizedDoc.setCatStatus(4);
					ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDoc);
					return;
				}
			}
			//Excluding the document based configured in properties file.
			String excludeDocs = env.getProperty("aed.exclude.documents."+ddpDmsDocsDetails.get(0).getDddCompanySource().trim());
			if (excludeDocs != null && !excludeDocs.isEmpty()) {
				String[] execludeDocuments = excludeDocs.split(",");
				for(String execludeDoc : execludeDocuments) {
					if (execludeDoc.equalsIgnoreCase(ddpDmsDocsDetails.get(0).getDddContentType().toLowerCase())) {
						//Excluding the documents so the changed the status.
						ddpCategorizedDoc.setCatStatus(5);
						ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDoc);
						return;
					}
				}
			}
			//DdpCommunicationSetup communicationSetups = ddpRuleDetails2.get(0).getRdtCommunicationId();
			String trackUrl = env.getProperty(ddpDmsDocsDetails.get(0).getDddCompanySource().trim()+".tracking.url",env.getProperty("tracking.url.PROD"));
			String uatTrackUrl = env.getProperty(ddpDmsDocsDetails.get(0).getDddCompanySource().trim()+".tracking.url.UAT",env.getProperty("tracking.url.UAT"));
			String uncPath=env.getProperty("mail.fileLocation");
			if(env.getProperty("mail.evn").equals("UAT"))
			{
				trackUrl = uatTrackUrl;
				uncPath = env.getProperty("mail.fileLocation.UAT");
			}
			
			ruleType = ddpRuleDetail.getRdtRuleType();
			DdpCommunicationSetup communicationSetups = ddpRuleDetail.getRdtCommunicationId();
			  
			
			if (communicationSetups.getCmsStatus() == 0) {
				commId = communicationSetups.getCmsCommunicationId();
				commProtocolId = communicationSetups.getCmsProtocolSettingsId();
				commProtocol = communicationSetups.getCmsCommunicationProtocol();
			} 
	    //	String commProtocol = ddpRuleDetail.getRdtCommunicationId().getCmsCommunicationProtocol();
	    	DdpCommEmail ddpCommEmails = null;
	    	String fileLocation = null;
	    	boolean isFileCreated = false;
	    	int status = -1;
	    	
			if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
				ddpCommEmails =  ddpCommEmailService.findDdpCommEmail(Integer.parseInt(commProtocolId));
				fileLocation =  ddpCommEmails.getCemFtpLocation();

				if(null != ddpCommEmails.getCemFtpLocation()){
					fileLocation =  ddpCommEmails.getCemFtpLocation();
				}else if(null != ddpCommEmails.getCemUncPath()){
					fileLocation =  ddpCommEmails.getCemUncPath();
				}else{
					fileLocation=uncPath;
				}
			}else{
				fileLocation=uncPath;
			}
			List<String> salesDocs = Arrays.asList(env.getProperty("category.salesdoc").split(","));
			List<String> bulkDocs = Arrays.asList(env.getProperty("category.bulkdocs").split(",")); 
			List<String> docRefSpaces = Arrays.asList(env.getProperty("aed.containsSpace").split(","));
			for (DdpDmsDocsDetail ddpDmsDocsDetail : ddpDmsDocsDetails) {
				
					if (ddpDmsDocsDetail.getDddObjectName() != null) {
						extension = ddpDmsDocsDetail.getDddObjectName().substring(ddpDmsDocsDetail.getDddObjectName().lastIndexOf("."), ddpDmsDocsDetail.getDddObjectName().length());
					} else {
						extension = ".pdf";
					}
			/*** Condition start for constructing File for Control and Any Generated Documents	****/
				if (ddpDmsDocsDetail.getDddGeneratingSystem().toUpperCase().equalsIgnoreCase("CONTROL") || ddpDmsDocsDetail.getDddGeneratingSystem().toUpperCase().equalsIgnoreCase("ANY")) {
					boolean isRated = false;
					String refNumber = ddpDmsDocsDetail.getDddDocRef();
					// String requestorNumber = ddpDmsDocsDetail.getDddObjectName();
					String jobnumber = ddpDmsDocsDetail.getDddJobNumber();
					if (jobnumber.contains("*")) {
						jobnumber = ddpDmsDocsDetail.getDddConsignmentId();
					}
					// refNumber =refNumber.substring(refNumber.length()-3,refNumber.length());
					if (refNumber != null && !refNumber.isEmpty()) {
						if (refNumber.charAt(refNumber.length() - 1) == 'R') {
							refNumber=refNumber.substring(refNumber.indexOf("/")+1, refNumber.length()-2);
							isRated = true;
						} else if (refNumber.contains(" ") && docRefSpaces.contains(ddpDmsDocsDetail.getDddCompanySource())) {
							refNumber = refNumber.substring(refNumber.lastIndexOf(" ")).trim();
						} else {
							refNumber=refNumber.substring(refNumber.indexOf("/")+1, refNumber.length());
						}
					} else {
						refNumber = "";
					}
					// String requestorNum = requestorNumber.substring(requestorNumber.lastIndexOf("-")+1, requestorNumber.lastIndexOf("."));
					if ( salesDocs.contains(ddpDmsDocsDetail.getDddControlDocType())) 
					{
						// filename = << DocType >>-<< ConsId >>-<< JobNo >>-<< DocRef(InvoiceNo) >>-<< RuleId >>   
						fileName = ddpDmsDocsDetail.getDddControlDocType()+ "-" + ddpDmsDocsDetail.getDddConsignmentId()
								+ "-" + jobnumber+ "-" + ddpDmsDocsDetail.getDddDocRef().replace("\\", "/").replace("/", "") + "-"	+ ddpCategorizedDoc.getCatRulId().getRulId();
					} 
					else if (bulkDocs.contains(ddpDmsDocsDetail.getDddControlDocType())) 
					{
						// filename = << DocType >>-<< ConsId >>-<< ConsId >>-<< DocRef(InvoiceNo) >>-<< RuleId >>
						fileName = ddpDmsDocsDetail.getDddControlDocType()+ "-" + ddpDmsDocsDetail.getDddConsignmentId()
								+ "-" + ddpDmsDocsDetail.getDddConsignmentId()+ "-" + ddpDmsDocsDetail.getDddDocRef().replace("\\", "/").replace("/", "") + "-"	+ ddpCategorizedDoc.getCatRulId().getRulId();
					} 
					else
					{
						// filename = << DocType >>-<< ConsID >>-<< JobNo >>-<< DocRef Sequence >>-<< RuleId >>
						fileName = ddpDmsDocsDetail.getDddControlDocType()+ "-" + ddpDmsDocsDetail.getDddConsignmentId()
								+ "-" + jobnumber+ "-" + refNumber + "-"+ ddpCategorizedDoc.getCatRulId().getRulId();
					}

					if (isRated) {
						fileName = fileName + "-RATED";
					}
					fileName = fileName + extension;
					primaryFileName = new String(fileName);
					// if Company is GIL(Global) we are not checking File in FTP location, and directly fetching from DMS.
					 if ((!ddpRuleDetail.getRdtCompany().getComCompanyCode().equalsIgnoreCase("GIL")) && !checkFileExistsInLocation(fileName,fileLocation) && ddpDmsDocsDetail.getDddGeneratingSystem().toUpperCase().equalsIgnoreCase("CONTROL")) {
						 logger.info("RuleJob.callWorkFlow(categorizedDocs) : File not found in the location with ddpDmsDocsDetail details "+ fileName + " of Cat Id :" + ddpCategorizedDoc.getCatId()+" : File Location : "+fileLocation);
						 status = 2;
						 logger.info("RuleJob.callWorkFlow(categorizedDocs) : Status is changed to "+status+". Due to file not found in the location.");
						 fileName = null;
					 }
				} else {
					fileName = ddpDmsDocsDetail.getDddObjectName();
					fileName = fileName.replaceAll("\\*", "");
					primaryFileName = fileName;
				}
			}
			
			
			if (null != fileName) {
				//Avoiding the duplicate records for at same cycle.
				try {
					indicatorFileName = tempFilePath+"/AED-"+ddpRuleDetail.getRdtRuleId().getRulId()+"-"+ddpRuleDetail.getRdtId()+"-"+ddpDmsDocsDetails.get(0).getDddConsignmentId().replaceAll("[^a-zA-Z0-9_.]", "")+"-"+ddpDmsDocsDetails.get(0).getDddJobNumber().replaceAll("[^a-zA-Z0-9_.]", "")+"-"+ddpDmsDocsDetails.get(0).getDddDocRef().replaceAll("[^a-zA-Z0-9_.]", "");
					File indicatorFile = new File(indicatorFileName);
					if (indicatorFile.exists()) {
						processAEDMailSending(ddpCategorizedDoc, ddpDmsDocsDetails.get(0), ddpRuleDetail,Constant.DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY_WITH_OUT_CAT_STATUS,null);
						ddpCategorizedDoc.setCatStatus(4);
						ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDoc);
						return;
					} else {
						indicatorFolder = new File(indicatorFileName);
						indicatorFolder.mkdirs();
					}
				} catch (Exception ex) {
					logger.error("RuleJob.callWorkFlow() - Unable create indicator file folder in the temp", ex);
				}
				
				isFileCreated = createFile(tempFolderPath, fileName, fileLocation, ddpDmsDocsDetails.get(0).getDddRObjectId());			
				logger.info("RuleJob.callWorkFlow(categorizedDocs) : constructed file name with ddpDmsDocsDetail details "+ fileName + " of Cat Id :" + ddpCategorizedDoc.getCatId());
			}
//			TypedQuery<DdpRuleDetail> ddpRuleDetails = DdpRuleDetail
//					.findDdpRuleDetailsByRdtRuleId(ddpCategorizedDoc.getCatRulId());
//			List<DdpRuleDetail> ddpRuleDetails2 = ddpRuleDetails.getResultList();
			//ruleType =  ddpRuleDetails2.get(0).getRdtRuleType();
			// Get the communication setup
		
			// Call Communication setup
			if (isFileCreated)
				status= communication(commId, commProtocolId, commProtocol, fileName,ddpDmsDocsDetails.get(0),ruleType,ddpCategorizedDoc.getCatId(),trackUrl,ddpCommEmails,fileLocation,new File(tempFolderPath + "//" + fileName),ddpRuleDetail);
				/** Update the status -1 if job failed.
				 *  status is 1 if mail sent with attachment.
				 *  status is 2 if mail sent without attachment.**/
				ddpCategorizedDoc.setCatStatus(status);
				ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDoc);
				
				if (status == -1 || status == 2 || status == 0) {
					
					String isNotification = env.getProperty("mail.filenotfound.notify."+ddpDmsDocsDetails.get(0).getDddCompanySource()+"."+ddpDmsDocsDetails.get(0).getDddControlDocType());
					if(isNotification == null || isNotification.isEmpty())
						isNotification = env.getProperty("mail.filenotfound.notify."+ddpDmsDocsDetails.get(0).getDddCompanySource());
					if(isNotification == null || isNotification.isEmpty())
						isNotification = env.getProperty("mail.filenotfound.notify");

					if(status == 2 && isNotification.equalsIgnoreCase("N"))
						logger.info("RuleJob.callWorkFlow(categorizedDocs) : File Not Found Nofication not sent");
					else{
						String mailBody = "<table border='1'><tr><td>Company</td><td>"+ddpDmsDocsDetails.get(0).getDddCompanySource().trim()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Branch</td><td>"+ddpDmsDocsDetails.get(0).getDddBranchSource()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Cat ID</td><td>"+ddpCategorizedDoc.getCatId()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Rule ID</td><td>"+ddpCategorizedDoc.getCatRulId().getRulId()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Rule Detail ID</td><td>"+ddpCategorizedDoc.getCatRdtId()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Document type</td><td>"+ddpDmsDocsDetails.get(0).getDddControlDocType()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Job Number</td><td>"+ddpDmsDocsDetails.get(0).getDddJobNumber()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Consignment ID</td><td>"+ddpDmsDocsDetails.get(0).getDddConsignmentId()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Syn ID</td><td>"+ddpCategorizedDoc.getCatSynId()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Client ID</td><td>"+ddpRuleDetail.getRdtPartyId()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Client Code</td><td>"+ddpRuleDetail.getRdtPartyCode().getPtyPartyName()+"</td></tr>";
						mailBody = mailBody +"<tr><td>Status</td><td>"+status+"</td></tr></table>";
						taskUtil.sendMailByDevelopers("<h3> Missing AED Document details </h3> "+(fileName == null ?"File not found in FTP location.<br/>":"")+mailBody,"unable to send AED document & filename - "+primaryFileName);
					}
				}
				
		} catch (Exception e) {
			// Update the status when job failed
			logger.info("RuleJob.callWorkFlow(categorizedDocs) : sending mail failed for  :"+ ddpCategorizedDoc.getCatId());
			logger.error("RuleJob.callWorkFlow(categorizedDocs) : error"+e.getMessage() );
			ddpCategorizedDoc.setCatStatus(-1);
			logger.info("RuleJob.callWorkFlow(categorizedDocs) : changing the status to -1 for CATID : "+ddpCategorizedDoc.getCatId() );
			ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDoc);
		
			String mailBody = "<table border='1'><tr><td>Company</td><td>NULL</td></tr>";
			mailBody = mailBody +"<tr><td>Branch</td><td>NULL</td></tr>";
			mailBody = mailBody +"<tr><td>Cat ID</td><td>"+ddpCategorizedDoc.getCatId()+"</td></tr>";
			mailBody = mailBody +"<tr><td>Rule ID</td><td>"+ddpCategorizedDoc.getCatRulId().getRulId()+"</td></tr>";
			mailBody = mailBody +"<tr><td>Rule Detail ID</td><td>"+ddpCategorizedDoc.getCatRdtId()+"</td></tr>";
			mailBody = mailBody +"<tr><td>Document type</td><td>NULL</td></tr>";
			mailBody = mailBody +"<tr><td>Job Number</td><td>NULL</td></tr>";
			mailBody = mailBody +"<tr><td>Consignment ID</td><td>NULL</td></tr>";
			mailBody = mailBody +"<tr><td>Syn ID</td><td>"+ddpCategorizedDoc.getCatSynId()+"</td></tr>";
			mailBody = mailBody +"<tr><td>Client ID</td><td>NULL</td></tr>";
			mailBody = mailBody +"<tr><td>Status</td><td>-1</td></tr>"; 
			mailBody = mailBody +"<tr><td>Stacktrace</td><td>"+e+"</td></tr></table>";
			taskUtil.sendMailByDevelopers("<h3> Missing AED Document details </h3> "+mailBody,"unable to send AED document & filename - "+fileName);
			
		} finally {
			
			SchedulerJobUtil.deleteFolder(tmpFile);
			
			if (indicatorFileName != null && indicatorFolder != null) 
				SchedulerJobUtil.deleteFolder(indicatorFolder);
			
			try {
				if (scheduler != null)
					scheduler.shutdown();
			} catch (Exception ex) {
				logger.error("RuleJob.callWorkFlow() - unable to shutdown the scheduler thread", ex);
			}
		}
	}
	
	//@AuditLog
	public int communication(Integer commId,String commProtocolId,String commProtocol,String fileName,
			DdpDmsDocsDetail ddpDmsDocsDetail,String ruleType,Integer catId,String trackUrl,DdpCommEmail ddpCommEmails,
			String uncPath,File file,DdpRuleDetail ddpRuleDetail){
				
		
		//communicating with Control DB(DB2) to fetch printed user details.
//		String uname=ddpDmsDocsDetail.getDddUserId().trim();
//		String query = env.getProperty("control.query."+env.getProperty("mail.evn"));
//		List<Map<String,Object>> resultSet = controlJdbcTemplate.queryForList(query, new Object[]{uname});
		List<Map<String,Object>> resultSet = null;
		//checking for document is Invoice of Not
		String inv = env.getProperty("mail."+ddpDmsDocsDetail.getDddCompanySource()+".INVs");
		List<String> invDocs= new ArrayList<String>();
		if(!(inv==null || inv.equals("")))
		{
			String[] invArr = inv.split(",");
			invDocs = Arrays.asList(invArr);
		}
		String branchCCMailId=null;		
		//String telephone  = "";
		//String clientName = "";
		String userReference = "";
		Element firstElement = null;
	
		String xmlFileName = fileName.substring(0, fileName.lastIndexOf('.')).concat(".xml");
		//LOAD XML FILE
		try{
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = docBuilder.parse (new File(uncPath+"\\"+xmlFileName));
			// normalize text representation
			doc.getDocumentElement ().normalize ();
			NodeList rootNodeList = doc.getElementsByTagName(doc.getDocumentElement().getNodeName());
			Node rootNode = rootNodeList.item(0);
			firstElement = (Element)rootNode;
//			
//			NodeList telephoneList = firstElement.getElementsByTagName(env.getProperty("mail.xml.telephoneTag"));
//			Element telephoneElement = (Element)telephoneList.item(0);
//			
//			NodeList clientnameList = firstElement.getElementsByTagName(env.getProperty("mail.xml.clientNameTag"));
//			Element clientnameElement = (Element)clientnameList.item(0);
//			
			NodeList userReferenceList = firstElement.getElementsByTagName(env.getProperty("mail.xml.UserReferenceTag"));
			Element userReferenceElement = (Element)userReferenceList.item(0);
//			
//			if(telephoneElement != null)
//			{
//				NodeList telephoneNode = telephoneElement.getChildNodes();
//				telephone = ((Node)telephoneNode.item(0)).getNodeValue().trim();
//			}
//			if(clientnameElement != null)
//			{
//				NodeList clientNameNode = clientnameElement.getChildNodes();
//				clientName = ((Node)clientNameNode.item(0)).getNodeValue().trim();
//			}
			if(userReferenceElement != null)
			{
				NodeList userReferenceElementNode = userReferenceElement.getChildNodes();
				userReference = ((Node)userReferenceElementNode.item(0)).getNodeValue().trim();
			}
		}
		catch(Exception e){
			logger.error("RuleJob.communication() : error occur while parsing xml file "+e.getMessage());
		}
		String mailSubjectKey = "";
		String docRef = ddpDmsDocsDetail.getDddDocRef();
		String companySource= ddpDmsDocsDetail.getDddCompanySource().trim();
		docRef = (docRef.contains("-R"))? docRef.substring(0, docRef.indexOf("-R")) : docRef;
		
		//constructing mail subject
		if(ddpRuleDetail.getRdtCompany().getComCountryCode().equalsIgnoreCase(env.getProperty("mail.aed.global").trim()))
			companySource = env.getProperty("mail.aed.global").trim();
		String mailSubject = env.getProperty("mail.subject."+companySource+"."+ddpRuleDetail.getRdtPartyId().trim()+"."+ddpDmsDocsDetail.getDddControlDocType());
		mailSubjectKey = "mail.subject."+companySource+"."+ddpRuleDetail.getRdtPartyId().trim()+"."+ddpDmsDocsDetail.getDddControlDocType();
		if(invDocs.contains(ddpDmsDocsDetail.getDddControlDocType()) && (mailSubject == null || mailSubject.equals("")))
		{
			mailSubject = env.getProperty("mail.subject."+companySource+"."+ddpRuleDetail.getRdtPartyId().trim()+".INVOICE");
			mailSubjectKey = "mail.subject."+companySource+"."+ddpRuleDetail.getRdtPartyId().trim()+".INVOICE";
		}
		if(mailSubject == null || mailSubject.equals(""))
		{
			mailSubject = env.getProperty("mail.subject."+companySource+"."+ddpRuleDetail.getRdtPartyId().trim());
			mailSubjectKey = "mail.subject."+companySource+"."+ddpRuleDetail.getRdtPartyId().trim();
		}
		if(mailSubject == null || mailSubject.equals(""))
		{ 
			mailSubject = env.getProperty("mail.subject."+companySource+"."+ddpDmsDocsDetail.getDddControlDocType());
			mailSubjectKey = "mail.subject."+companySource+"."+ddpDmsDocsDetail.getDddControlDocType();
		}
		if(invDocs.contains(ddpDmsDocsDetail.getDddControlDocType()) && (mailSubject == null || mailSubject.equals("")))
		{
			mailSubject = env.getProperty("mail.subject."+companySource+".INVOICE");
			mailSubjectKey = "mail.subject."+companySource+".INVOICE";
		}
		if(mailSubject == null || mailSubject.equals(""))
		{ 
			mailSubject = env.getProperty("mail.subject."+companySource);
			mailSubjectKey = "mail.subject."+companySource;
		}
		if(mailSubject == null || mailSubject.equals(""))
		{ 
			mailSubject = env.getProperty("mail.subject");
			mailSubjectKey = "mail.subject";
		}
		//APPEND USER REFERENCE TO THE SUBJECT IF EXIST FOR NAM COMPANIES
		List<String> namCompanies = Arrays.asList(env.getProperty("nam.companies").split(","));
		if(namCompanies.contains(companySource))
		{
			if(! userReference.equals(""))
			{
				String customMailSubject = env.getProperty(mailSubjectKey+".custom");
				if(! (customMailSubject == null || customMailSubject.equals("")))
					mailSubject = customMailSubject;
			}
		}
		//constructing mail body
		String mailBody = env.getProperty("mail.body."+companySource+"."+ddpRuleDetail.getRdtPartyId()+"."+ddpDmsDocsDetail.getDddControlDocType());
		if(invDocs.contains(ddpDmsDocsDetail.getDddControlDocType()) && (mailBody == null || mailBody.equals(""))) 
			mailBody = env.getProperty("mail.body."+companySource+"."+ddpRuleDetail.getRdtPartyId()+".INVOICE");
		if(mailBody == null || mailBody.equals(""))
			mailBody = env.getProperty("mail.body."+companySource+"."+ddpRuleDetail.getRdtPartyId());
		if(mailBody == null || mailBody.equals(""))
			mailBody = env.getProperty("mail.body."+companySource+"."+ddpDmsDocsDetail.getDddControlDocType());
		if(invDocs.contains(ddpDmsDocsDetail.getDddControlDocType()) && (mailBody == null || mailBody.equals("")))
			mailBody = env.getProperty("mail.body."+companySource+".INVOICE");
		if(mailBody == null || mailBody.equals("")) 
			mailBody = env.getProperty("mail.body."+companySource);
		if(mailBody == null || mailBody.equals(""))
			mailBody = env.getProperty("mail.body");
		
		//document naming convention
		String mailFileName = env.getProperty("filename."+companySource+"."+ddpDmsDocsDetail.getDddControlDocType()+"."+ddpRuleDetail.getRdtPartyId());
		if (mailFileName == null || mailFileName.isEmpty())
			mailFileName = env.getProperty("filename."+companySource+"."+ddpDmsDocsDetail.getDddControlDocType());
		if (mailFileName == null || mailFileName.isEmpty())
			mailFileName = env.getProperty("filename."+companySource);
		//Generic Code
				HashMap<String,String> placeholderMap = new HashMap<String, String>();
				placeholderMap.put("mailSubject", mailSubject);
				placeholderMap.put("mailBody", mailBody);
				placeholderMap.put("fileName", (mailFileName == null ? fileName:mailFileName));
				//calling method
				replacePlaceHolder(firstElement, placeholderMap, catId, ddpDmsDocsDetail, ddpRuleDetail, docRef, trackUrl);
				
				mailBody = placeholderMap.get("mailBody");
				mailSubject = placeholderMap.get("mailSubject");
				
				if (mailFileName != null) {
					String newFileName = placeholderMap.get("fileName");
					try {
						String absolutePath = file.getAbsolutePath();
						String path = absolutePath.
					    	     substring(0,absolutePath.lastIndexOf(File.separator));
						String newExt = FileUtils.getFileExtension(file.getName());
						newFileName = newFileName.replaceAll("/", "");
						boolean isRenamed = file.renameTo(new File(path+"\\"+newFileName+""+newExt));
						if (isRenamed) {
							fileName = newFileName+""+newExt;
							file = new File(path+"\\"+fileName);
							logger.info("RuleJob.Communication() - File name updated to configured name : "+file.getName());
						}
						
					} catch(Exception ex) {
						logger.error("RoleJob.Communication() - Exception occurired while changing the file name : "+newFileName);
					}
				}
		    	
//				Set<String> dataSourceSet = new HashSet<String>();
//			    Pattern pattern = Pattern.compile(env.getProperty("placeholder.compile"));
//			    Matcher matcher = pattern.matcher(mailSubject);
//			    while (matcher.find()) {
//			    	String dataSource = env.getProperty(matcher.group(1));
//			    	placeholderMap.put(matcher.group(1), dataSource.substring(0,dataSource.indexOf(".")));
//			    	dataSourceSet.add(dataSource.substring(0,dataSource.indexOf(".")));
//			    }
//			    Matcher bodyMatcher = pattern.matcher(mailBody);
//			    while(bodyMatcher.find())
//			    {
//			    	String dataSource = env.getProperty(bodyMatcher.group(1));
//			    	placeholderMap.put(bodyMatcher.group(1), dataSource.substring(0,dataSource.indexOf(".")));
//			    	dataSourceSet.add(dataSource.substring(0,dataSource.indexOf(".")));
//			    }
//			    
//			    Iterator<String> dataSetItr = dataSourceSet.iterator();
//		    	while(dataSetItr.hasNext()){
//		    		try{
//			    		String dataSet = dataSetItr.next();
//			    		if(dataSet.equalsIgnoreCase("XML"))
//			    		{
//			    			List<String> xmlKeys = getKeyFromValue(placeholderMap,dataSet);
//			    			String tempValue="";
//							try{
//								for(String xmlKey : xmlKeys)
//				    			{
//									String tagName = env.getProperty("mail.xml."+xmlKey+"Tag").trim() ; 
//									NodeList nodeList = firstElement.getElementsByTagName(tagName);
//									
//			    					Element element = (Element)nodeList.item(0);
//			    					
//			    					if(element != null)
//			    					{
//			    						NodeList node = element.getChildNodes();
//			    						tempValue = ((Node)node.item(0)).getNodeValue().trim();
//			    					}
//			    					String placeholder = "%%"+xmlKey+"%%" ; 
//			    					mailSubject = mailSubject.replaceAll(placeholder, tempValue);
//			    					mailBody = mailBody.replaceAll(placeholder, tempValue);
//				    			}
//							}
//							catch(Exception e){
//								logger.error("RuleJob.communication() : error occur while parsing xml file for cat ID : "+catId+" , "+e.getMessage());
//							}
//			    		}
//			    		else if(dataSet.equalsIgnoreCase("control"))
//			    		{
//			    			try{
//				    			List<String> controlKeys = getKeyFromValue(placeholderMap,dataSet);
//				    			
//				    			for(String controlKey:controlKeys )
//				    			{
//				    				String tempValue="";
//				    				//Identify the way to pass the arguments
//				    				List<String> argsList = Arrays.asList(env.getProperty(controlKey+".args").trim().split(","));
//				    				String query = env.getProperty("control.query."+controlKey+"."+env.getProperty("mail.evn"));
//				    				query = query.replaceAll("%%CONTROLUSERID%%", ddpDmsDocsDetail.getDddUserId().trim());
//				    				query = query.replaceAll("%%CONTROLJOBNUMBER%%", ddpDmsDocsDetail.getDddJobNumber().trim());
//				    				List<Map<String,Object>> controlResultSet = commonUtil.getControlMetaData(query, 0, 3, 2);
//				    				if(! controlResultSet.isEmpty())
//				    				{
//	//			    					resultSet = controlResultSet;
//				    					String columnName = env.getProperty(controlKey).split("\\.")[1];
//				    					tempValue = controlResultSet.get(0).get(columnName).toString().trim();
//				    				}
//				    				else
//				    				{
//				    					logger.info("Result set is empty for cat ID:"+catId);
//				    					if(controlKey.equalsIgnoreCase("CONSIGNEEREF"))
//				    						mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%_", "");
//				    					mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%", "");
//				    					mailBody = mailBody.replaceAll("%%"+controlKey+"%%", "");
//				    					throw new Exception("RuleJob.communication() : Exception Due to control Result is Empty");
//				    				}
//				    				if(controlKey.equalsIgnoreCase("CONSIGNEEREF") && tempValue.equals(""))
//			    						mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%_", tempValue);
//			    					mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%", tempValue);
//			    					mailBody = mailBody.replaceAll("%%"+controlKey+"%%", tempValue);
//				    			}
//			    			}catch(Exception e){
//			    				logger.error("RuleJob.communication() : error occur while connecting to Control DB for cat ID: "+catId+" , "+e.getMessage());
//			    				throw new Exception("RuleJob.communication() : No Control Connection");
//			    			}
//			    		}
//			    		else if(dataSet.equalsIgnoreCase("DDP"))
//			    		{
//			    			mailSubject = mailSubject.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
//			    			mailSubject = mailSubject.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
//			    			mailSubject = mailSubject.replaceAll("%%DOCREF%%", docRef);
//			    			mailSubject = mailSubject.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef()); 
//			    			mailSubject = mailSubject.replaceAll("%%JOBNO%%", ddpDmsDocsDetail.getDddJobNumber());
//			    			mailSubject = mailSubject.replaceAll("%%REFERENCE%%", ddpDmsDocsDetail.getDddJobNumber());
//			    			mailSubject = mailSubject.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
//			    			mailSubject = mailSubject.replaceAll("%%COMPANY%%", ddpDmsDocsDetail.getDddCompanySource());
//			    			mailSubject = mailSubject.replaceAll("%%BRANCH%%", ddpDmsDocsDetail.getDddBranchSource());
//			    			mailSubject = mailSubject.replaceAll("%%DEPARTMENT%%", ddpDmsDocsDetail.getDddDeptSource());
//			    			
//			    			mailBody = mailBody.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
//			    			mailBody = mailBody.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
//			    			mailBody = mailBody.replaceAll("%%DOCREF%%", docRef);
//			    			mailBody = mailBody.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef()); 
//			    			mailBody = mailBody.replaceAll("%%JOBNO%%", ddpDmsDocsDetail.getDddJobNumber());
//			    			mailBody = mailBody.replaceAll("%%REFERENCE%%", ddpDmsDocsDetail.getDddJobNumber());
//			    			mailBody = mailBody.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
//			    			mailBody = mailBody.replaceAll("%%COMPANY%%", ddpDmsDocsDetail.getDddCompanySource());
//			    			mailBody = mailBody.replaceAll("%%BRANCH%%", ddpDmsDocsDetail.getDddBranchSource());
//			    			mailBody = mailBody.replaceAll("%%DEPARTMENT%%", ddpDmsDocsDetail.getDddDeptSource());
//			    		}
//			    		else
//			    		{
//			    			mailSubject=mailSubject.replaceAll("%%TRACKURL%%",(trackUrl==null) ? "" : trackUrl);
//			    			mailSubject=mailSubject.replaceAll("%%FILENAME%%",fileName);
//			    			if (mailSubject.contains("%%DDPTIMESTAMP%%") || mailBody.contains("%%DDPTIMESTAMP%%")) {
//			    				int synID = ddpCategorizedDocsService.findDdpCategorizedDocs(catId).getCatSynId();
//			    				mailSubject=mailSubject.replaceAll("%%DDPTIMESTAMP%%",""+ddpSynService.findDmsDdpSyn(synID).getSynCreatedDate().getTime());
//			    				mailBody=mailBody.replaceAll("%%DDPTIMESTAMP%%",""+ddpSynService.findDmsDdpSyn(synID).getSynCreatedDate().getTime());
//			    			}
//			    			mailBody=mailBody.replaceAll("%%TRACKURL%%",(trackUrl==null) ? "" : trackUrl);
//			    			mailBody=mailBody.replaceAll("%%FILENAME%%",fileName);
//			    		}
//			    	
//			    	}catch(Exception ex){
//			    		logger.error("RuleJob.communication() : No control connection");
//			    	}
//		    	}
//			    
			    
			    /***************** Generic Code Ends  ******************/
		
//		mailSubject = mailSubject.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
//		mailSubject = mailSubject.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
//		mailSubject = mailSubject.replaceAll("%%DOCREF%%", docRef);
//		mailSubject = mailSubject.replaceAll("%%USERREF%%", userReference);
//		mailSubject = mailSubject.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef()); 
//		mailSubject = mailSubject.replaceAll("%%JOBNO%%", ddpDmsDocsDetail.getDddJobNumber());
//		mailSubject = mailSubject.replaceAll("%%CLIENTNAME%%", clientName);
//		mailSubject = mailSubject.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
		String abrDoctypeinsubject=env.getProperty("mail.subject."+ddpDmsDocsDetail.getDddCompanySource().trim()+"."+ddpDmsDocsDetail.getDddControlDocType().trim()+".DOCTYPE");
		if(! (abrDoctypeinsubject == null || abrDoctypeinsubject.equals("")) )
		{
			mailSubject = mailSubject.replaceAll(ddpDmsDocsDetail.getDddControlDocType().trim(), abrDoctypeinsubject);
		}
		//APPEND HAWB AND MAWB  ONLY FOR LSG and LMY
		if(ddpDmsDocsDetail.getDddCompanySource().equalsIgnoreCase("LSG") || ddpDmsDocsDetail.getDddCompanySource().equalsIgnoreCase("LMY") )
		{
			if(! ddpDmsDocsDetail.getDddHouseAirwayBillNum().equals(""))
				mailSubject = mailSubject.concat("-"+ddpDmsDocsDetail.getDddHouseAirwayBillNum().toString());
			if(! ddpDmsDocsDetail.getDddMasterAirwayBillNum().equals(""))
				mailSubject = mailSubject.concat("-MAWB-"+ddpDmsDocsDetail.getDddMasterAirwayBillNum().toString());
		}
		
		logger.info("RuleJob.communication() : mail subject construction completed.");
		//constructing mail Body
		
		
		//BASED ON USER TYPE TRACKING URL KEY MAY CHANGE
//		mailBody = mailBody.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
//		mailBody=mailBody.replaceAll("%%TRACKURL%%",(trackUrl==null) ? "" : trackUrl);
//		mailBody=mailBody.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
//		mailBody=mailBody.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
//		mailBody=mailBody.replaceAll("%%UATTRACKURL%%", (uatTrackUrl==null) ? "" : uatTrackUrl);
//		mailBody=mailBody.replaceAll("%%FILENAME%%",fileName);
//		mailBody=mailBody.replaceAll("%%REFERENCE%%",  ddpDmsDocsDetail.getDddJobNumber());
//		mailBody=mailBody.replaceAll("%%JOBNO%%",  ddpDmsDocsDetail.getDddJobNumber());
//		mailBody=mailBody.replaceAll("%%BRANCH%%", ddpDmsDocsDetail.getDddBranchSource());
//		mailBody=mailBody.replaceAll("%%DOCREF%%", docRef);
//		mailBody=mailBody.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef());
//		mailBody=mailBody.replaceAll("%%MAILID%%",(resultSet.isEmpty())? env.getProperty("mail.fromAddress"): (String) resultSet.get(0).get("UDEADD").toString().trim());
//		mailBody=mailBody.replaceAll("%%CCMAILID%%", (resultSet.isEmpty())? env.getProperty("mail.fromAddress"): (String) resultSet.get(0).get("UDEADD").toString().trim() );
//		mailBody=mailBody.replaceAll("%%TELEPHONE%%",telephone);
//		mailBody=mailBody.replaceAll("%%USERNAME%%", (resultSet.isEmpty())? "": (String) resultSet.get(0).get("UDSDES").toString().trim() );
		String abrDoctype=env.getProperty("mail.body."+ddpDmsDocsDetail.getDddCompanySource().trim()+"."+ddpDmsDocsDetail.getDddControlDocType().trim()+".DOCTYPE");
		if(! (abrDoctype == null || abrDoctype.equals("")) )
		{
			mailBody = mailBody.replaceAll(ddpDmsDocsDetail.getDddControlDocType().trim(), abrDoctype);
		}
		logger.info("RuleJob.communication() : mail body construction completed. ");
		//mail from Address
		String mailFrom=env.getProperty("mail.fromAddress");
		
		//Check Branch CC mail ID required or not
		
		String flagbranchCCMailid = env.getProperty("mail.cc.userMailId."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
		if(flagbranchCCMailid == null || flagbranchCCMailid.equals(""))
			flagbranchCCMailid = env.getProperty("mail.cc.userMailId."+ddpDmsDocsDetail.getDddCompanySource());
		if(flagbranchCCMailid == null || flagbranchCCMailid.equals(""))
			flagbranchCCMailid = env.getProperty("mail.cc.userMailId");
		if(flagbranchCCMailid.equalsIgnoreCase("Y"))
		{
			if(resultSet == null)
			{
				String controlUID=ddpDmsDocsDetail.getDddUserId().trim();
    			String controlQuery = env.getProperty("control.query."+env.getProperty("mail.evn"));
    			controlQuery = controlQuery.replaceAll("%%CONTROLUSERID%%", controlUID);
    			List<Map<String,Object>> controlResultSet = commonUtil.getControlMetaData(controlQuery, 0, 3, 2);
    			resultSet = controlResultSet;
			}
			if(! resultSet.isEmpty())
			{
				if((String) resultSet.get(0).get("UDEADD").toString() != null)
					branchCCMailId = (String) resultSet.get(0).get("UDEADD").toString().trim();
			}
		}
		
		//Check Document Digital Signature is Required or not
		String passphrase = "";
		String isDocumentSignRequired = env.getProperty("document.digitalsign."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
		if( isDocumentSignRequired == null || isDocumentSignRequired.equals(""))
		{
			isDocumentSignRequired = env.getProperty("document.digitalsign."+ddpDmsDocsDetail.getDddCompanySource());
			if(isDocumentSignRequired == null || isDocumentSignRequired.equals(""))
			{
				isDocumentSignRequired = env.getProperty("document.digitalsign");
			}
			else{
				passphrase = env.getProperty("document.digitalsign."+ddpDmsDocsDetail.getDddCompanySource()+".passphrase");
				//Check if all documents in company except condition as below
				isDocumentSignRequired = env.getProperty("document.digitalsign."+ddpDmsDocsDetail.getDddCompanySource()+".except."+ddpDmsDocsDetail.getDddControlDocType());
			}
		}
		else{
			passphrase = env.getProperty("document.digitalsign."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType()+".passphrase");
			
		}
		
		String responseBase64Binary = null;
		String contentType = null;
		if(isDocumentSignRequired != null && isDocumentSignRequired.equalsIgnoreCase("Y"))
		{
			if(passphrase == null || passphrase.equals(""))
				passphrase = env.getProperty("document.dititalsign.passphrase");
			
			String base64Binary = TaskUtil.convertToBase64(file);
			//call SOAP service by passing binary document and passphrase
			if(ddpDmsDocsDetail.getDddContentType().trim().equalsIgnoreCase("pdf")){
				responseBase64Binary = taskUtil.callSoapWebService(base64Binary,fileName,passphrase);
				boolean isCopied = false;
				if(! responseBase64Binary.isEmpty())
					isCopied = TaskUtil.storeSignedPDF(file.getAbsolutePath(), responseBase64Binary);
				if(isCopied)
					responseBase64Binary = null;
				contentType = env.getProperty("mail.attachment.contenttype."+ddpDmsDocsDetail.getDddContentType().toLowerCase().trim());
			}
			
		}
		
		
		
		int attachFlag = 0;
		if( (commProtocol.equalsIgnoreCase("SMTP")) || (commProtocol.equalsIgnoreCase("HTTP")) ){
			
			//This should be get from the configuration file or from table
//			String smtpAddres ="10.20.1.233";
			String smtpAddres = env.getProperty("mail.smtpAddress");
			logger.info("RuleJob.communication() : calling sendMail() ");
			attachFlag = taskUtil.sendMail(ddpCommEmails,fileName,smtpAddres,mailSubject,mailBody,mailFrom,ruleType,branchCCMailId,catId,file, responseBase64Binary, contentType);
		}
		else if(commProtocol.equalsIgnoreCase("FTP"))
		{
			
		}
		return attachFlag;
	}
	

	public void readFile() {

	}

	public String removeCharacter(String str) {
		Pattern pt = Pattern.compile("[^a-zA-Z0-9_-]");
		Matcher match = pt.matcher(str);
		while (match.find()) {
			String s = match.group();
			str = str.replaceAll("\\" + s, "");
		}
		return str;
	}

	public static void main(String args[]) {
		String str = "BOL-SHA0223401-SH355*930I7-56465.pdf";
		if (str.contains("*")) {
		}
		String temp = str.substring(str.lastIndexOf("-") + 1,
				str.lastIndexOf("."));

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
	private boolean createFile(String folderName,String filename,String fileLocation,String objectID) {
		
		boolean isFileCreated = false;
		//Checking the filename in the FTP location. If not present in the FTP it fetch from DFC
		if (checkFileExistsInLocation(filename, fileLocation)) {
			isFileCreated = SchedulerJobUtil.copyFile(new File(fileLocation + "//" + filename), new File(folderName + "//" + filename));
		} else {
			IDfSession session = ddpDFCClientComponent.beginSession();
			if (session == null)
				taskUtil.sendMailByDevelopers("AED Rule unable to connect to DFC Session. Please restart the DDP machine", "DfC Session connection issue");
			try {
				isFileCreated =ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(objectID, folderName, filename, session);
			} finally {
				if (session != null)
					ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
			}
		}
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
		logger.info("DdpCreateMultiAedSchedulerTask.checkFileExistsInLocation(String fileName,String fileLocation)  : searching for file with name : "+fileLoc);
		File file = new File(fileLoc);
		try {
			isFileExists = file.exists();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return isFileExists;
	}
	
	/**
	 * 
	 * @param hm
	 * @param value
	 * @return
	 */
	public static List<String> getKeyFromValue(Map<?,?> hm, Object value) {
		List<String> keyLst = new ArrayList<String>();
	    for (Object o : hm.keySet()) {
	      if (hm.get(o).equals(value)) {
	    	  keyLst.add(o.toString());
	      }
	    }
	    return keyLst;
	}

	/**
	 * Method used for checking AED rule details with same details as been triggered before 24 hours.
	 * 
	 * @param ddpCategorizedDocs
	 * @param ddpDmsDocsDetail
	 * @param ddpRuleDetail
	 * @return
	 */
	public boolean checkAEDMailAlreadySend(DdpCategorizedDocs ddpCategorizedDocs,DdpDmsDocsDetail ddpDmsDocsDetail,DdpRuleDetail ddpRuleDetail) {
		
		boolean isMailSend = false;
		
		
			// Need to send the mail to customer for re-process the documents.
			String strRepeatDocumentInHrs = env.getProperty("mail.process."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType()+".hrs");
			if (strRepeatDocumentInHrs == null || strRepeatDocumentInHrs.trim().length() == 0) 
				strRepeatDocumentInHrs = env.getProperty("mail.process."+ddpDmsDocsDetail.getDddCompanySource()+".hrs");
			if (strRepeatDocumentInHrs == null ||strRepeatDocumentInHrs.trim().length() == 0 )
				strRepeatDocumentInHrs = env.getProperty("mail.process."+ddpDmsDocsDetail.getDddControlDocType()+".hrs");
			if (strRepeatDocumentInHrs == null)
				strRepeatDocumentInHrs = env.getProperty("mail.process.hrs");
			
			int repeatDocumentInHrs = Integer.parseInt(strRepeatDocumentInHrs);
			
			isMailSend = processAEDMailSending(ddpCategorizedDocs, ddpDmsDocsDetail, ddpRuleDetail,Constant.DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY,repeatDocumentInHrs);		
		
		return isMailSend;
	}
	
	/**
	 * Method used for processing the AED Mails.
	 * 
	 * @param ddpCategorizedDocs
	 * @param ddpDmsDocsDetail
	 * @param ddpRuleDetail
	 * @return
	 */
	private boolean processAEDMailSending(DdpCategorizedDocs ddpCategorizedDocs,DdpDmsDocsDetail ddpDmsDocsDetail,DdpRuleDetail ddpRuleDetail,String query,Integer repeatDocumentInHrs) {
		
		boolean isMailSend = false;
		List<Map<String, String>> controlUsers = getControlUserList(ddpCategorizedDocs, ddpDmsDocsDetail, ddpRuleDetail,query,repeatDocumentInHrs);
		if (controlUsers != null && controlUsers.size() > 0) {			
			
			String toAddress = null;
			String oldControlUserID = "";
			String oldControlUserName = "";
			
			String controlQuery = env.getProperty("control.query."+env.getProperty("mail.evn"));
			controlQuery = controlQuery.replaceAll("%%CONTROLUSERID%%", ddpDmsDocsDetail.getDddUserId().trim());
			try {
				List<Map<String,Object>> toAddresslResultSet = commonUtil.getControlMetaData(controlQuery, 0, 2, 2);
				if (!toAddresslResultSet.isEmpty()) {
					Map<String,Object> toAddressMap = toAddresslResultSet.get(0);
					toAddress = (String)toAddressMap.get("UDEADD");
				}
				if (!toAddresslResultSet.isEmpty()) {
					Map<String,Object> toAddressMap = toAddresslResultSet.get(0);
					oldControlUserID = (String)toAddressMap.get("UDEADD");
					oldControlUserName = (String)toAddressMap.get("UDSDES");
				} else {
					oldControlUserID = controlUsers.get(0).get("userid").trim();
					oldControlUserName = controlUsers.get(0).get("userid").trim()+"@agility.com";
				}
				
			} catch (Exception ex) {
				toAddress = env.getProperty("mail.process.cc.address");
				logger.error("RuleJob.checkAEDMailAlreadySend() - Unable to get control details from JDBC Template", ex);
			}
			if (toAddress == null || toAddress.isEmpty()) 
				toAddress = env.getProperty("mail.process.cc.address");
			
			String smtpAddress = env.getProperty("mail.smtpAddress");
			String fromAddress = env.getProperty("mail.fromAddress");
			
			String ccAddress = env.getProperty("mail.process.cc.address");
			String subject = env.getProperty("mail.process.subject");
			subject = subject.replaceAll("%%CLIENTVALUE%%", ddpRuleDetail.getRdtPartyId());
			subject = subject.replaceAll("%%JOBNUMBER%%", ddpDmsDocsDetail.getDddJobNumber());
			
			String body = env.getProperty("mail.process.body");
			body = body.replaceAll("%%CLIENTCODE%%", ddpRuleDetail.getRdtPartyCode().getPtyPartyName());
			body = body.replaceAll("%%CLIENTVALUE%%", ddpRuleDetail.getRdtPartyId());
			body = body.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
			body = body.replaceAll("%%JOBNUMBER%%", ddpDmsDocsDetail.getDddJobNumber());
			body = body.replaceAll("%%DOCUMENTTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
			body = body.replaceAll("%%RULEID%%", ddpRuleDetail.getRdtRuleId().getRulId()+"");
			body = body.replaceAll("%%COMPANYCODE%%", ddpDmsDocsDetail.getDddCompanySource());
			body = body.replaceAll("%%CONTROLUSERID%%", oldControlUserID);
			body = body.replaceAll("%%CONTROLUSERNAME%%", oldControlUserName);
			body = body.replaceAll("%%URL%%", env.getProperty(env.getProperty("mail.evn")));
			body = body.replaceAll("%%ENCCATID%%", SecurityUtils.agilityEncryptionOnlyNumbers(ddpCategorizedDocs.getCatId()+""));
			body = body.replaceAll("%%CATID%%", ddpCategorizedDocs.getCatId()+"");
			List<Map<String, String>> usersCont =	getControlUserList(ddpCategorizedDocs, ddpDmsDocsDetail, ddpRuleDetail,Constant.DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY_COUNT,null);
			body = body.replace("%%DUPDOC%%",( usersCont == null? 0 : usersCont.size())+"");
			body = body.replace("%%SENDDATE%%",controlUsers.get(0).get("date"));
			
			isMailSend = true;
			taskUtil.sendMail(smtpAddress, toAddress, ccAddress, fromAddress, subject, body, null);
			
			
		}
		
		return isMailSend;
	}
	
	/**
	 * Method used for fetching the details of control users list.
	 * 
	 * @param ddpCategorizedDocs
	 * @param ddpDmsDocsDetail
	 * @param ddpRuleDetail
	 * @return
	 */
	public List<Map<String,String>> getControlUserList(DdpCategorizedDocs ddpCategorizedDocs,DdpDmsDocsDetail ddpDmsDocsDetail,DdpRuleDetail ddpRuleDetail,String query,Integer repeatDocumentInHrs) {
		
		Object[] objArr = null;
		if(repeatDocumentInHrs == null)
			repeatDocumentInHrs = 24;
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.add(Calendar.HOUR_OF_DAY, -repeatDocumentInHrs);
		
		String strDoHoursCheck = env.getProperty("mail.process."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
		if (strDoHoursCheck == null || strDoHoursCheck.trim().length() == 0) 
			strDoHoursCheck = env.getProperty("mail.process."+ddpDmsDocsDetail.getDddCompanySource());
		if (strDoHoursCheck == null ||strDoHoursCheck.trim().length() == 0 )
			strDoHoursCheck = env.getProperty("mail.process."+ddpDmsDocsDetail.getDddControlDocType());
		if (strDoHoursCheck == null)
			strDoHoursCheck = env.getProperty("mail.process");
		
		if(strDoHoursCheck.equalsIgnoreCase("Y"))
		{
			objArr = new Object[]{ddpDmsDocsDetail.getDddCompanySource(),ddpDmsDocsDetail.getDddControlDocType(),ddpDmsDocsDetail.getDddDocRef(),ddpDmsDocsDetail.getDddConsignmentId(),ddpDmsDocsDetail.getDddJobNumber(),ddpCategorizedDocs.getCatRulId().getRulId(),ddpCategorizedDocs.getCatId(),calendar};
		}
		else
		{
			objArr = new Object[]{ddpDmsDocsDetail.getDddCompanySource(),ddpDmsDocsDetail.getDddControlDocType(),ddpDmsDocsDetail.getDddDocRef(),ddpDmsDocsDetail.getDddConsignmentId(),ddpDmsDocsDetail.getDddJobNumber(),ddpCategorizedDocs.getCatRulId().getRulId(),ddpCategorizedDocs.getCatId()};
			query = Constant.DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY_WITHOUT_TIME;
		}
		
		List<Map<String,String>> controlUsers = this.jdbcTemplate.query(query, objArr ,  new RowMapper<Map<String,String>>() {
			
			public Map<String,String> mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				Map<String,String> map = new HashMap<String, String>();
				  map.put("userid", rs.getString("DDD_USER_ID"));
				  map.put("date", rs.getString("CAT_MODIFIED_DATE"));
				 
				 return map;
	           }
		});	
		
		return controlUsers;
	}
	
	
	/**
	 * Method used for replacing the give subject, body & file name.
	 * 
	 * @param firstElement
	 * @param propertyMap
	 * @param catId
	 * @param ddpDmsDocsDetail
	 * @param ddpRuleDetail
	 * @param docRef
	 * @param trackUrl
	 */
	public void replacePlaceHolder(Element firstElement,Map<String,String> propertyMap,Integer catId,DdpDmsDocsDetail ddpDmsDocsDetail,
			DdpRuleDetail ddpRuleDetail,String docRef,String trackUrl) {
		
		String mailSubject = propertyMap.get("mailSubject");
		String mailBody = propertyMap.get("mailBody");
		String fileName = propertyMap.get("fileName");
		
		HashMap<String,String> placeholderMap = new HashMap<String, String>();
		Set<String> dataSourceSet = new HashSet<String>();
	    Pattern pattern = Pattern.compile(env.getProperty("placeholder.compile"));
	    Matcher matcher = pattern.matcher(mailSubject);
	    while (matcher.find()) {
	    	String dataSource = env.getProperty(matcher.group(1));
	    	placeholderMap.put(matcher.group(1), dataSource.substring(0,dataSource.indexOf(".")));
	    	dataSourceSet.add(dataSource.substring(0,dataSource.indexOf(".")));
	    }
	    
	    Matcher bodyMatcher = pattern.matcher(mailBody);
	    while(bodyMatcher.find())
	    {
	    	String dataSource = env.getProperty(bodyMatcher.group(1));
	    	placeholderMap.put(bodyMatcher.group(1), dataSource.substring(0,dataSource.indexOf(".")));
	    	dataSourceSet.add(dataSource.substring(0,dataSource.indexOf(".")));
	    }
	    
	    if (fileName != null) {
		    Matcher fileNameMatcher = pattern.matcher(fileName);
		    while(fileNameMatcher.find())
		    {
		    	String dataSource = env.getProperty(fileNameMatcher.group(1));
		    	placeholderMap.put(fileNameMatcher.group(1), dataSource.substring(0,dataSource.indexOf(".")));
		    	dataSourceSet.add(dataSource.substring(0,dataSource.indexOf(".")));
		    }
	    }
	    
	    Iterator<String> dataSetItr = dataSourceSet.iterator();
    	while(dataSetItr.hasNext()){
    		try{
	    		String dataSet = dataSetItr.next();
	    		if(dataSet.equalsIgnoreCase("XML"))
	    		{
	    			List<String> xmlKeys = getKeyFromValue(placeholderMap,dataSet);
	    			String tempValue="";
					try{
						for(String xmlKey : xmlKeys)
		    			{
							String tagName = env.getProperty("mail.xml."+xmlKey+"Tag").trim() ; 
							NodeList nodeList = firstElement.getElementsByTagName(tagName);
							
	    					Element element = (Element)nodeList.item(0);
	    					
	    					if(element != null)
	    					{
	    						NodeList node = element.getChildNodes();
	    						tempValue = ((Node)node.item(0)).getNodeValue().trim();
	    					}
	    					String placeholder = "%%"+xmlKey+"%%" ; 
	    					mailSubject = mailSubject.replaceAll(placeholder, tempValue);
	    					mailBody = mailBody.replaceAll(placeholder, tempValue);
	    					if (fileName != null)
	    						fileName = fileName.replaceAll(placeholder, tempValue);
		    			}
					}
					catch(Exception e){
						logger.error("RuleJob.communication() : error occur while parsing xml file for cat ID : "+catId+" , "+e.getMessage());
					}
	    		}
	    		else if(dataSet.equalsIgnoreCase("control"))
	    		{
	    			try{
		    			List<String> controlKeys = getKeyFromValue(placeholderMap,dataSet);
		    			
		    			for(String controlKey:controlKeys )
		    			{
		    				String tempValue="";
		    				//Identify the way to pass the arguments
		    				//List<String> argsList = Arrays.asList(env.getProperty(controlKey+".args").trim().split(","));
		    				String query = env.getProperty("control.query."+controlKey+"."+env.getProperty("mail.evn"));
		    				query = query.replaceAll("%%CONTROLUSERID%%", ddpDmsDocsDetail.getDddUserId().trim());
		    				query = query.replaceAll("%%CONTROLJOBNUMBER%%", ddpDmsDocsDetail.getDddJobNumber().trim());
		    				List<Map<String,Object>> controlResultSet = commonUtil.getControlMetaData(query, 0, 3, 2);
		    				if(! controlResultSet.isEmpty())
		    				{
//			    					resultSet = controlResultSet;
		    					String columnName = env.getProperty(controlKey).split("\\.")[1];
		    					tempValue = controlResultSet.get(0).get(columnName).toString().trim();
		    				}
		    				else
		    				{
		    					logger.info("Result set is empty for cat ID:"+catId);
		    					if(controlKey.equalsIgnoreCase("CONSIGNEEREF"))  {
		    						mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%_", "");
		    						if (fileName != null)
		    							fileName = fileName.replaceAll("%%"+controlKey+"%%_", "");
		    					}
		    					if(controlKey.equalsIgnoreCase("SHIPPERREF"))
		    					{
		    						mailSubject = mailSubject.substring(0,mailSubject.lastIndexOf(":"));
		    					}
		    					mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%", "");
		    					mailBody = mailBody.replaceAll("%%"+controlKey+"%%", "");
		    					if (fileName != null)
	    							fileName = fileName.replaceAll("%%"+controlKey+"%%", "");
		    				//	throw new Exception("RuleJob.communication() : Exception Due to control Result is Empty");
		    				}
		    				if(controlKey.equalsIgnoreCase("CONSIGNEEREF") && tempValue.equals("")) {
	    						mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%_", tempValue);
	    						if (fileName != null)
	    							fileName = fileName.replaceAll("%%"+controlKey+"%%", tempValue);
		    				}
	    					mailSubject = mailSubject.replaceAll("%%"+controlKey+"%%", tempValue);
	    					mailBody = mailBody.replaceAll("%%"+controlKey+"%%", tempValue);
	    					if (fileName != null)
    							fileName = fileName.replaceAll("%%"+controlKey+"%%", tempValue);
		    			}
	    			}catch(Exception e){
	    				logger.error("RuleJob.communication() : error occur while connecting to Control DB for cat ID: "+catId+" , "+e.getMessage());
	    				throw new Exception("RuleJob.communication() : No Control Connection");
	    			}
	    		}
	    		else if(dataSet.equalsIgnoreCase("DDP"))
	    		{
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
	    			mailSubject = mailSubject.replaceAll("%%CARRIERCODE%%", ddpDmsDocsDetail.getDddCarrierRef());
	    			mailSubject = mailSubject.replaceAll("%%MASTERAIRWAYBILL%%", ddpDmsDocsDetail.getDddMasterAirwayBillNum());
	    			mailSubject = mailSubject.replaceAll("%%DESTINATIONBRANCH%%", ddpDmsDocsDetail.getDddBranchDestination());
	    			mailSubject = mailSubject.replaceAll("%%CONSIGNEE%%", ddpDmsDocsDetail.getDddConsignee());
	    			mailSubject = mailSubject.replaceAll("%%SHIPPER%%", ddpDmsDocsDetail.getDddShipper());
	    			mailSubject = mailSubject.replaceAll("%%NOTIFYPARTY%%", ddpDmsDocsDetail.getDddNotifyParty());
	    			
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
	    			mailBody = mailBody.replaceAll("%%CARRIERCODE%%", ddpDmsDocsDetail.getDddCarrierRef());
	    			mailBody = mailBody.replaceAll("%%MASTERAIRWAYBILL%%", ddpDmsDocsDetail.getDddMasterAirwayBillNum());
	    			mailBody = mailBody.replaceAll("%%DESTINATIONBRANCH%%", ddpDmsDocsDetail.getDddBranchDestination());
	    			mailBody = mailBody.replaceAll("%%CONSIGNEE%%", ddpDmsDocsDetail.getDddConsignee());
	    			mailBody = mailBody.replaceAll("%%SHIPPER%%", ddpDmsDocsDetail.getDddShipper());
	    			mailBody = mailBody.replaceAll("%%NOTIFYPARTY%%", ddpDmsDocsDetail.getDddNotifyParty());
	    			
	    			if (fileName != null) {
	    				
	    				fileName = fileName.replaceAll("%%DOCTYPE%%", ddpDmsDocsDetail.getDddControlDocType());
	    				fileName = fileName.replaceAll("%%CONSIGNMENTID%%", ddpDmsDocsDetail.getDddConsignmentId());
	    				fileName = fileName.replaceAll("%%DOCREF%%", docRef);
		    			fileName = fileName.replaceAll("%%INVNO%%", ddpDmsDocsDetail.getDddDocRef()); 
		    			fileName = fileName.replaceAll("%%JOBNO%%", ddpDmsDocsDetail.getDddJobNumber());
		    			fileName = fileName.replaceAll("%%REFERENCE%%", ddpDmsDocsDetail.getDddJobNumber());
		    			fileName = fileName.replaceAll("%%CLIENTIDVALUE%%", ddpRuleDetail.getRdtPartyId().trim());
		    			fileName = fileName.replaceAll("%%COMPANY%%", ddpDmsDocsDetail.getDddCompanySource());
		    			fileName = fileName.replaceAll("%%BRANCH%%", ddpDmsDocsDetail.getDddBranchSource());
		    			fileName = fileName.replaceAll("%%DEPARTMENT%%", ddpDmsDocsDetail.getDddDeptSource());
	    			}
	    		}
	    		    	
	    	}catch(Exception ex){
	    		logger.error("RuleJob.communication() : No control connection",ex);
	    	}
    	}
    	
    	if (mailBody != null && mailSubject != null) {
    		
    		mailSubject=mailSubject.replaceAll("%%TRACKURL%%",(trackUrl==null) ? "" : trackUrl);
			mailSubject=mailSubject.replaceAll("%%FILENAME%%",fileName);
			if (mailSubject.contains("%%DDPTIMESTAMP%%") || mailBody.contains("%%DDPTIMESTAMP%%")) {
				int synID = ddpCategorizedDocsService.findDdpCategorizedDocs(catId).getCatSynId();
				mailSubject=mailSubject.replaceAll("%%DDPTIMESTAMP%%",""+ddpSynService.findDmsDdpSyn(synID).getSynCreatedDate().getTime());
				mailBody=mailBody.replaceAll("%%DDPTIMESTAMP%%",""+ddpSynService.findDmsDdpSyn(synID).getSynCreatedDate().getTime());
			}
			mailBody=mailBody.replaceAll("%%TRACKURL%%",(trackUrl==null) ? "" : trackUrl);
			mailBody=mailBody.replaceAll("%%FILENAME%%",fileName);
    	}
    	
    	propertyMap.put("mailSubject", mailSubject);
    	propertyMap.put("mailBody", mailBody);
    	propertyMap.put("fileName", fileName);
	}
}
