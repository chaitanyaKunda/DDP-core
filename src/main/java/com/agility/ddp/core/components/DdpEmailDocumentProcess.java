/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommEmailService;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpEmailTriggerSetup;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfSession;

/**
 * @author DGuntha
 *
 */
@Component
public class DdpEmailDocumentProcess {

	private static final Logger logger = LoggerFactory.getLogger(DdpEmailDocumentProcess.class);
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpDFCClientComponent ddpDFCClientComponent;
	
	@Autowired
	private Environment env;
	
//	 @Value( "${ddp.export.folder}" )
//	 private String tempFilePath;
	 
	 @Autowired
	private DdpCommEmailService ddpCommEmailService;
	 
	 @Autowired
	 private TaskUtil taskUtil;
	 
	 @Autowired
	 private DdpCategorizedDocsService ddpCategorizedDocsService;
	
	public boolean beginWorkFlowProcess(DdpCategorizedDocs ddpCategorizedDoc,int typeOfService) {
		
		boolean isProcessed = false;
		
		
		List<DdpRuleDetail> ddpRuleDetails = commonUtil.getAllRuleDetails(ddpCategorizedDoc.getCatRulId().getRulId());
		
		if (ddpRuleDetails == null || ddpRuleDetails.size() == 0) {
			logger.info("DdpEmailDocumentProcess.beginWorkFlowProcess(categorizedDocs) - Empty rule detail for the rule id : "+ddpCategorizedDoc.getCatRulId().getRulId()+" : Type of Service  : "+typeOfService);
			return isProcessed;
		}
		DdpEmailTriggerSetup ddpEmailTriggerSetup = commonUtil.getDdpEmailTriggerSetup(ddpCategorizedDoc.getCatRulId().getRulId());
		
		if (ddpEmailTriggerSetup != null)
			executeProcessFlowJob(ddpCategorizedDoc, ddpRuleDetails, ddpEmailTriggerSetup, typeOfService);
		else
			logger.info("DdpEmailDocumentProcess.beginWorkFlowProcess(categorizedDocs) - DdpEmailTriggerSetup is empty for CAT_ID : "+ddpCategorizedDoc.getCatId()+" : Type of Service : "+typeOfService);
		
		return isProcessed;
	}
	
	
	public boolean executeProcessFlowJob(DdpCategorizedDocs ddpCategorizedDoc,List<DdpRuleDetail> ddpRuleDetails,DdpEmailTriggerSetup ddpEmailTriggerSetup,int typeOfService) {
		
		boolean isExecuted = false;
		String docTypes = "";
		Set<String> mandiatoryDocTypes = new HashSet<String>();
		Set<String> optionalDocTypes = new HashSet<String>();
		Map<Integer, String> sequnceOrder = new HashMap<Integer, String>();
		
		//Looping for adding the primary & secondary document types
		for (DdpRuleDetail ruleDetail : ddpRuleDetails) {
			
		/*	
			if (ruleDetail.getRdtRelavantType() != null) {
				if (ruleDetail.getRdtRelavantType() == 1)
					primaryDocTypeList.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			}*/
			
			if (ruleDetail.getRdtForcedType() == 0) {
				optionalDocTypes.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			} else if (ruleDetail.getRdtForcedType() == 1) {
				mandiatoryDocTypes.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			} /*else if (ruleDetail.getRdtForcedType() == 2) {
				alteastDocTypes.add(ruleDetail.getRdtDocType().getDtyDocTypeCode());
			}*/
			//As the primary document details are fetched using dql query.
			if (ruleDetail.getRdtDocSequence() != 1)
				docTypes += "'" + ruleDetail.getRdtDocType().getDtyDocTypeCode() + "',"; 
			
			sequnceOrder.put(ruleDetail.getRdtDocSequence(), ruleDetail.getRdtDocType().getDtyDocTypeCode());
			
		}
		
		DdpDmsDocsDetail[] ddpDmsDocsDetails = (DdpDmsDocsDetail[]) ddpCategorizedDoc.getCatDtxId().getDdpDmsDocsDetails().toArray();
		
		if (ddpDmsDocsDetails == null || ddpDmsDocsDetails.length == 0) {
			logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable to get DdpDmsDocsDetails based on the Categorized docs where cat id : "+ddpCategorizedDoc.getCatId()+" : Type of Service : "+typeOfService);
			return isExecuted;
		}
			
		DdpDmsDocsDetail ddpDmsDocsDetail = ddpDmsDocsDetails[0];
		
		//To avoid time creation of session for each iteration of loop
    	IDfSession session = ddpDFCClientComponent.beginSession();
    	
    	if (session == null) {
    		logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable create the DFCClient Session for this Categorized id : "+ddpCategorizedDoc.getCatId());
    		return isExecuted;
    	}
    	
    	//TODO : based on configured Triggered Criteria we need to get key;
    	String key = "multiaed.rule.jobNumber.customQuery";
    	//TODO : based on configured Triggered Criteria we need to pass the value from DdpDMSDocsDetail
    	String  value =  ddpDmsDocsDetail.getDddJobNumber();
    	
    	try {
    		
    		DdpCommEmail ddpCommEmails = ddpCommEmailService.findDdpCommEmail(Integer.parseInt(ddpRuleDetails.get(0).getRdtCommunicationId().getCmsProtocolSettingsId()));
    		String query = env.getProperty(key);
    		query = query.replaceAll("%%DOCTYPES%%", docTypes);
    		query = query.replaceAll("%%JOBNUMBER%%", value);
    		
    		IDfCollection iDfCollection = commonUtil.getIDFCollectionDetails(query, session);
    		
    		if (iDfCollection == null) {
    			logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable fetch details based on the "+key+" for this Categorized id : "+ddpCategorizedDoc.getCatId()+" & Type of Servie : "+typeOfService);
    			return isExecuted;
    		}
    		
    		List<DdpExportMissingDocs> subDocsList  = commonUtil.constructMissingDocs(iDfCollection, "Email Scheduler", ddpCategorizedDoc.getCatId());
    		
    		 List<String> missingDocsList = checkAllMandatoryDocsAvailable(subDocsList, mandiatoryDocTypes, optionalDocTypes);
    		 
    		 if (missingDocsList.size() >= 1) {
    			 //TODO: need to send missing document list as mail
    			 logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - mandatory missing document list "+missingDocsList);
    			 return isExecuted;
    		 }
    		 String tempFilePath = "";
    		 //TODO download the document to local based on the merging type
    		 //TODO: Need to check type is merging
    		 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
    		 String localtmpPath = tempFilePath+"/temp_EmailScheduler_"+typeOfService+"_"+ddpCategorizedDoc.getCatId()+"_" + dateFormat.format(new Date());
    		 File localtmpFile = FileUtils.createFolder(localtmpPath);
    		 
    		 StringBuffer mailBody = new StringBuffer();
    		 String sourceLocation = localtmpPath;
    		 //Try to download as local.
    		 String fileName = ddpDmsDocsDetail.getDddObjectName();
    		 String ext = FileUtils.getFileExtension(fileName);
    		 String tmpFileName = FileUtils.getFileNameWithOutExtension(fileName) + ext;
    		 fileName = getDocumentName(tmpFileName, sourceLocation, null);
    		 //if merge is required enable the code with if condition.
    		/* List<String> rObjList =  getRobjectList(ddpDmsDocsDetail, subDocsList, sequnceOrder);
    		 String mergeID = commonUtil.performMergeOperation(rObjList, fileName, env.getProperty("ddp.vdLocation"), env.getProperty("ddp.objectType"), session);
				if (mergeID == null) {
					logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable to merge the documents");
					//TODO: failed mail need to send.
				}
				boolean isDocumentMerged = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(mergeID, sourceLocation, fileName, session);
				if (!isDocumentMerged)logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable to download the documents robjectid is "+mergeID);*/
				
    		 //TODO: if document mails is not merge then need use the  below code.
    		 boolean isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(ddpDmsDocsDetail.getDddRObjectId(), sourceLocation, fileName, session);
    		 if (!isDownload) {
    			 logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable to download the file from the DMS -  FileName : "+fileName);
    		 }
    		 constructMailBody(mailBody, fileName, ddpDmsDocsDetail.getDddJobNumber(), ddpDmsDocsDetail.getDddConsignmentId(), ddpDmsDocsDetail.getDddDocRef(), ddpDmsDocsDetail.getDddControlDocType());
    		 for (DdpExportMissingDocs doc : subDocsList) {
    			 
    			  fileName = doc.getMisRobjectName();
        		  ext = FileUtils.getFileExtension(fileName);
        		  tmpFileName = FileUtils.getFileNameWithOutExtension(fileName);
        		  fileName = getDocumentName(tmpFileName, sourceLocation, null) + ext;
    			  isDownload = ddpDFCClientComponent.downloadDocumentBasedOnObjectIDFromDFClient(doc.getMisRObjectId(), sourceLocation, fileName, session);
        		 if (!isDownload) {
        			 logger.info("DdpEmailDocumentProcess.executeProcessFlowJob() - Unable to download the file from the DMS -  FileName : "+fileName);
        			 constructMailBody(mailBody, fileName, doc.getMisJobNumber(), doc.getMisConsignmentId(), doc.getMisEntryType(), doc.getMisDocType());
        		 }
    		 }
    				 
    		int flag = sendSuccessMail(ddpCommEmails, ddpDmsDocsDetail, localtmpFile, ddpRuleDetails.get(0).getRdtRuleType(), mailBody);
    		
    		if (flag == 1) {
    			ddpCategorizedDoc.setCatStatus(1);
    			ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDoc);
    		}
    		
    	} catch (Exception ex) {
    		logger.error("DdpEmailDocumentProcess- executeProcessFlowJob() unable to execute the process for ServiceType : "+typeOfService, ex);
    	} finally {
    		if (null != session)
    			ddpDFCClientComponent.releaseDfSession(session.getSessionManager(), session);
    	}
		
		return isExecuted;
	}
	
	/**
	 * Method used for getting the Robject List.
	 * 
	 * @param ddpDmsDocsDetail
	 * @param subDocsList
	 * @param sequnceOrder
	 * @return
	 */
	private List<String> getRobjectList(DdpDmsDocsDetail ddpDmsDocsDetail,List<DdpExportMissingDocs> subDocsList,Map<Integer, String> sequnceOrder) {
		
		List<String> rObjectList = new LinkedList<String>();
		Map<String,String> documentTypeMap = new HashMap<String, String>();
		documentTypeMap.put(ddpDmsDocsDetail.getDddControlDocType(), ddpDmsDocsDetail.getDddRObjectId());
		
		for (DdpExportMissingDocs docs : subDocsList) 
			documentTypeMap.put(docs.getMisDocType(),docs.getMisRObjectId());
		
		//To get keys in the order for merging the documents.
		List<Integer> keys = new ArrayList<Integer>(sequnceOrder.keySet());
		Collections.sort(keys);
		
		for (Integer key : keys) {
			String rObjID = documentTypeMap.get(sequnceOrder.get(key));
			if (rObjID != null) {
				rObjectList.add(rObjID);
			}
		}
		
		return rObjectList;
		
	}
	
	private List<String> checkAllMandatoryDocsAvailable(List<DdpExportMissingDocs> subDocsList,Set<String> mandatoryDocTypes, Set<String> atleastDocTypes) {
		
		List<String> missType = new ArrayList<String>();
		List<String> docTypeList = new ArrayList<String>();
		for (DdpExportMissingDocs missDoc : subDocsList)
			docTypeList.add(missDoc.getMisDocType());
		
		//Checking all the mandatory documents.
		for (String mandatoryDocs : mandatoryDocTypes) {
			if (!docTypeList.contains(mandatoryDocs)) {
				System.out.println("DdpBackUpDocumentProcess.isDocExportable() -  Madatory documents are not able " );
				//subExportDocuments = null;
				missType.add(mandatoryDocs);
				//return subExportDocuments;
			}
		}
		
	/*	for (String atleastDocs : atleastDocTypes) {
			if (docTypeList.contains(atleastDocs)) {
				isAllDocsAvailable = true;
				break;
			}
		}*/
		
		/*if (!isAllDocsAvailable) {
			subExportDocuments = null;
			missType.addAll(atleastDocTypes);
		}
		
		if (subExportDocuments == null) {
			subMissingDocs.put(rObjectID, missType);
		}*/
		
		return missType;
	}
	
	private String getDocumentName(String fileName,String soruceFileLocation,String seq) {
		
		List<String> fileNames = readListOfFileInLocal(soruceFileLocation);
		String sequnceNumber = FileUtils.checkFileExists(fileNames, fileName, seq);
		if (sequnceNumber != null) {
			fileName = fileName + "-COPY"+sequnceNumber;
			sequnceNumber =	getDocumentName(fileName, soruceFileLocation, sequnceNumber);
		}
		return fileName;
			
		
	}
	
	
	/**
	 * Method used for reading the local file system.
	 * 
	 * @param sourceLocation
	 * @return
	 */
	private List<String> readListOfFileInLocal (String sourceLocation) {
		
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
	
	private int sendSuccessMail(DdpCommEmail ddpCommEmail,DdpDmsDocsDetail ddpDmsDocsDetail,File sourceLocation,String ruleType,StringBuffer mailBodyContent) {
		
		String	mailSubject =  null;	
		try {
			mailSubject  = env.getProperty("multiaedmail.subject."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
		} catch (Exception ex) {
			
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
		
		String mailBody = null;
		try {
			mailBody =	env.getProperty("multiaedmail.body."+ddpDmsDocsDetail.getDddCompanySource()+"."+ddpDmsDocsDetail.getDddControlDocType());
		} catch(Exception ex) {
			
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
		
		mailBody = mailBody.replaceAll("%%DETAILS%%", mailBodyContent.toString());
		String mailFrom=env.getProperty("mail.fromAddress");
		
		String smtpAddres = env.getProperty("mail.smtpAddress");
		logger.info("RuleJob.communication() : calling sendMail() ");
		int attachFlag = taskUtil.sendMailForMultiAed(ddpCommEmail, sourceLocation, smtpAddres, mailSubject, mailBody, mailFrom,ruleType,null);
		
		return attachFlag;
	}
	
	/**
     * Method used used for constructing the mail body.
     * 
     * @param ddpDmsDocsDetail
     * @param fileName
     * @return
     */
    private void constructMailBody(StringBuffer mailBody,String fileName,String jobNumber,String consignmentID,String docRef,String docType) {
    	
    	//Based on the company need to change the mail body.
    	
    	 mailBody.append("<tr><td>"+fileName+"</td>");
    	 mailBody.append("<td>"+jobNumber+"</td>");
    	 mailBody.append("<td>"+consignmentID+"</td>");
    	 mailBody.append("<td>"+docRef+"</td>");
    	 mailBody.append("<td>"+docType+"</td></tr>");
    	
    }
}
