/**
 * 
 */
package com.agility.ddp.core.components;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.DdpDFCClient;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.NamingConventionUtil;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpDocnameConv;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfLoginInfo;

/**
 * @author DGuntha
 *
 */
@Component
//@PropertySource("classpath:ddp.properties")
public class DdpDFCClientComponent {
	
	//Get the documentum Session
//    @Value( "${dms.docbaseName}" )
//	String docbaseName  ;
//   			
//	@Value( "${dms.userName}" )
//    String userName ;
//		
//	@Value( "${dms.password}" )
//    String password ;
		
	@Autowired
	private TaskUtil taskUtil;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private CommonUtil commonUtil;
	
	 private static final Logger logger = LoggerFactory.getLogger(DdpDFCClientComponent.class);
	 
	public IDfSession beginSession() {
		
		logger.debug("DdpDFCClientComponent.beginSession() invoked");
		IDfSession session = null;
        IDfSessionManager sMgr =null;
        
        try {
        	String userName = env.getProperty("dms.userName") ;
        	String password  = env.getProperty("dms.password");
      		String docbaseName = env.getProperty("dms.docbaseName");
      		
	        IDfClientX clientx = new DfClientX();
		    IDfClient  client = clientx.getLocalClient();
		    
		    sMgr = client.newSessionManager();
		    
		    IDfLoginInfo loginInfoObj = clientx.getLoginInfo();
		    
		    loginInfoObj.setUser(userName);
		    loginInfoObj.setPassword(password);
		    sMgr.setIdentity(docbaseName, loginInfoObj);
		    
		    logger.info("DdpDFCClientComponent.beginSession(): Export process started for creating session manager.");
		    
		    session = sMgr.getSession(docbaseName);
        }  catch(Exception ex)
	    {
	    	logger.error("DdpDFCClientComponent.beginSession() exception while creating session.",ex.getMessage());
	    	ex.printStackTrace();
	    	taskUtil.sendMailByDevelopers(ex.getMessage(), " DdpDFCClientComponent.beginSession() not able to create session.");
	    }
        logger.debug("DdpDFCClientComponent.beginSession() successfully executed");
	    return session;
	}
	
	/**
	 * Method to release the docbase session
	 * 
	 * @param dfSessionManager
	 *            IDfSessionManager
	 * @param dfSession
	 *            IDfSession
	 * 
	 */
	public  void releaseDfSession(IDfSessionManager dfSessionManager,
			IDfSession dfSession) {

		if (null != dfSession) {
			// release session
			try {
				dfSessionManager.release(dfSession);
			} catch (Exception e) {
				logger.error("DdpDFCClient-releaseDFSession() : Exception while releasing the docbase session..."
								+ e.getMessage(), e);
			}
		}
	}
	       
    public boolean downloadDocumentFromDFCClient(Integer catDtxId,String filePath,DdpDmsDocsDetail ddpDmsDocsDetail,IDfSession session) {
    	
    	boolean isFileDownload = false;
    	
	    try
	    {
	    	
	    	String strFileName = new DdpDFCClient().exportDocumentToLocal(catDtxId,ddpDmsDocsDetail.getDddRObjectId(), filePath, session);
	    	
	    	if (strFileName == null || strFileName.isEmpty())
	    		isFileDownload = false;
	    	else
	    		isFileDownload = true;
	    			//logger.debug("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) - File [{}] exported successfully.",strFileName);
	    	
	    }  catch(Exception ex)
	    {
	    	//logger.error("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) exception while iterating.",ex.getMessage());
	    	ex.printStackTrace();
	    }
	
	   // logger.debug("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) executed successfully.");
    	return isFileDownload;
    }
    
    public boolean downloadDocumentFromDFCWithOutCatID(String filePath,DdpDmsDocsDetail ddpDmsDocsDetail,IDfSession session,StringBuffer expdetails,List<String> fileNameList,
    		DdpDmsDocsDetail dmsDocsDetails,DdpDocnameConv convName,int typeOfService,List<DdpExportSuccessReport> exportedReports,DdpCategorizedDocs docs,Map<String, String> loadNumebrMap) {
    	
    	
    	boolean isFileDownload = false;
    	
	    try
	    {
	    	
	    	String fileName = "";
	    	String strObjName = "";
			IDfSysObject sysObj = null;
			
            sysObj = (IDfSysObject) session.getObject(new DfId(ddpDmsDocsDetail.getDddRObjectId()));
	            
            if (sysObj == null)
                throw new Exception("Cannot retrieve sysobject: " + ddpDmsDocsDetail.getDddRObjectId());

            if (sysObj.getContentSize() < 2) //checking for 2 bytes. This can be fine-tuned.
            	throw new Exception("No content/corrupted file");
	            
            if (convName != null) {
	 			if (convName.getDcvGenNamingConv() != null && !convName.getDcvGenNamingConv().isEmpty() && (convName.getDcvDupDocNamingConv() == null ||  convName.getDcvDupDocNamingConv().isEmpty())) {
	 				String extension = ((extension =FileUtils.getFileExtension(dmsDocsDetails.getDddObjectName())) == null ? ".pdf":extension);
	 				fileName = NamingConventionUtil.getExportNamingConvension(convName.getDcvGenNamingConv(), dmsDocsDetails, null, fileNameList,loadNumebrMap);
	 				fileName = fileName + extension;
	 			} else if (convName.getDcvDupDocNamingConv() != null && !convName.getDcvDupDocNamingConv().isEmpty()) {
	 				String extension = ((extension =FileUtils.getFileExtension(dmsDocsDetails.getDddObjectName())) == null ? ".pdf":extension);
	 				fileName = NamingConventionUtil.getExportDuplicateNamingConvension(convName.getDcvDupDocNamingConv(), dmsDocsDetails, "1", fileNameList,loadNumebrMap);
	 				fileName = fileName + extension;
	 			} else {
	 				fileName = sysObj.getObjectName();
	 			}
	 		} else {
            	fileName = sysObj.getObjectName();
	 		}
	            
				//Assumed always file will be exported. No FOLDER or Virtual doc will be exported.
            //FOLDER or Virtual doc should be treated separately
	            
            strObjName = sysObj.getFile(filePath + "/" + fileName);
	    	
	    	if (strObjName == null || strObjName.isEmpty())
	    		isFileDownload = false;
	    	else {
	    		isFileDownload = true;
	    		
	    		if (!fileNameList.contains(fileName)) {
		    		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
			    	expdetails.append(format.format(new Date())+","); 
					format.applyLocalizedPattern("HH:mm:ss");
					expdetails.append(format.format(new Date())+",receive,success,"+FileUtils.findSizeofFileInKB(filePath+"/"+fileName)+",00:00:01,"+dmsDocsDetails.getDddConsignmentId()+","+dmsDocsDetails.getDddJobNumber()+","+fileName+"\n");
					fileNameList.add(fileName);
					exportedReports.add(commonUtil.constructExportReportDomainObject(docs.getCatId(), docs.getCatRulId().getRulId(), dmsDocsDetails.getDddJobNumber(), 
			   				dmsDocsDetails.getDddConsignmentId(), "Rule By ClientID", fileName, (int)FileUtils.findSizeofFileInKB(filePath + "/" + fileName), typeOfService,
			   				dmsDocsDetails.getDddDocRef(), dmsDocsDetails.getDddMasterJobNumber(), docs.getCatCreatedDate()));
	    		}
	    	}
	    }  catch(Exception ex)  {
	    	logger.error("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) exception while iterating.",ex.getMessage());
	    	//ex.printStackTrace();
	    }
	
	   // logger.debug("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) executed successfully.");
    	return isFileDownload;
    }
    
    public boolean downloadDocumentBasedOnObjectIDFromDFClient(String objectID,String expPath,String destFileName,IDfSession session) {
    	
    	boolean isFileDownload = false;
    	String strObjName = "";
		//String strtypeName = "";
		IDfSysObject sysObj = null;
		
		try
		{
			if (session == null)
                throw new Exception("DFC Session is null");

            sysObj = (IDfSysObject) session.getObject(new DfId(objectID));
            
            if (sysObj == null)
                throw new Exception("Cannot retrieve sysobject: " + objectID);

            if (sysObj.getContentSize() < 2) //checking for 2 bytes. This can be fine-tuned.
            	throw new Exception("No content/corrupted file");
            
           // strObjName = sysObj.getObjectName();
           // strtypeName = sysObj.getTypeName();
            
			//Assumed always file will be exported. No FOLDER or Virtual doc will be exported.
            //FOLDER or Virtual doc should be treated separately
            
            strObjName = sysObj.getFile(expPath + "/" +destFileName);
            if (strObjName != null && !strObjName.isEmpty())
            	isFileDownload = true;
		}
		catch(Exception ex)
		{
			logger.error("DdpDFCClientComponent.downloadDocumentBasedObObjectIDFromDFClinet() - Unable to download the document object ID : "+objectID, ex);
			//ex.printStackTrace();
			
		}
		return isFileDownload;
    }
    
    public boolean downloadAllDocumentBasedOnVersions(String filePath,DdpDmsDocsDetail ddpDmsDocsDetail,IDfSession session,DdpDocnameConv convName,
    		String startDate,String endDate,StringBuffer exportDetails,List<String> fileNameList, int typeOfService,
    		List<DdpExportSuccessReport> exportedReports,DdpCategorizedDocs docs,Map<String,String> loadNumberMap) {
    	
    	 logger.debug("DdpDFCClientComponent.downloadAllDocumentBasedOnVersions() successfully executed");
    	boolean isAllFilesDownload = false;
    	try {
	    	DdpDFCClient client =  new DdpDFCClient();
	    	 Hashtable<String, String> dmsMetadata = client.begin(session,ddpDmsDocsDetail.getDddRObjectId());
	    	 logger.debug("DdpDFCClientComponent.downloadAllDocumentBasedOnVersions() : Chornicle id : "+dmsMetadata.get("i_chronicle_id"));
	
	 	    //String query = "select object_name, r_object_id,  r_version_label from agl_control_document (all) where i_chronicle_id= '"+dmsMetadata.get("i_chronicle_id")+"' and agl_creation_date between Date('"+startDate+"','dd/mm/yyyy HH:mi:ss') and Date('"+endDate+"','dd/mm/yyyy HH:mi:ss')";
	    	 String query = "select object_name, r_object_id,  r_version_label from agl_control_document (all) where i_chronicle_id= '"+dmsMetadata.get("i_chronicle_id")+"'";
	 	    IDfQuery dfQuery = new DfQuery();
	 		dfQuery.setDQL(query);
	 		IDfCollection dfCollection = dfQuery.execute(session,
	 				IDfQuery.DF_EXEC_QUERY);
	 		
	 		boolean isVersionNumberReq = false;
	 		String srcDocName = null;
	 		if (convName != null) {
	 			if (convName.getDcvGenNamingConv() != null && !convName.getDcvGenNamingConv().isEmpty() && (convName.getDcvDupDocNamingConv() == null ||  convName.getDcvDupDocNamingConv().isEmpty())) {
	 				isVersionNumberReq = true;
	 				srcDocName =NamingConventionUtil.getExportNamingConvension(convName.getDcvGenNamingConv(), ddpDmsDocsDetail, "1", fileNameList,loadNumberMap);
	 			} 
	 		}
	 		logger.info("DdpDFCClientComponent.downloadAllDocumentBaseonVersion() - Total number of number doucment : "+dfCollection.getAttrCount()+" : for date range  star date : "+startDate+ " : End Date : "+endDate);
 			int i = 1;
	 		while (dfCollection.next()) {
	 			boolean isAllVesionNumReq = false;
	 			String destFileName = null;
	 			//Naming Convention
	 			String ext ="."; 
 		   		if ( dfCollection.getString("object_name") != null && dfCollection.getString("object_name").contains(".")) {
 			   		String [] fileNames = ddpDmsDocsDetail.getDddObjectName().split("\\.");
 					 ext = ext +fileNames[1];
 		   		} else {
 		   			ext = ".pdf";
 		   		}
 		   		
	 			if (convName != null) {
	 				
	 		   		if (isVersionNumberReq) {
	 		   			destFileName = srcDocName + ext;
	 		   		} else if (convName.getDcvDupDocNamingConv() != null && !convName.getDcvDupDocNamingConv().isEmpty()) {
		 		   			srcDocName = NamingConventionUtil.getExportDuplicateNamingConvension(convName.getDcvDupDocNamingConv(), ddpDmsDocsDetail, i+"", fileNameList,loadNumberMap);
		 		   			destFileName = srcDocName + ext;
	 		   		} else {
	 		   			destFileName = dfCollection.getString("object_name");
	 		   			isAllVesionNumReq = true;
	 		   		}
	 			} else {
	 				destFileName = dfCollection.getString("object_name");
	 				isAllVesionNumReq	 = true;
	 			}
	 			// end of the naming convention
	 			
	 			logger.debug("DdpDFCClientComponent.downloadAllDocumentBasedOnVersions() : "+dfCollection.getString("object_name")+" Object name : "+ dfCollection.getString("r_object_id")+" Version : "+dfCollection.getString("r_version_label"));
	 			//downloading all documents.
	 			String fileName = client.exportDocumentToLocal( dfCollection.getString("r_version_label"), destFileName, dfCollection.getString("r_object_id"),filePath, session,isAllVesionNumReq);
	 			if (fileName != null && !fileNameList.contains(fileName)) {
	 				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
	 				exportDetails.append(format.format(new Date())+",");
					format.applyLocalizedPattern("HH:mm:ss");
					exportDetails.append(format.format(new Date())+",receive,success,"+FileUtils.findSizeofFileInKB(filePath+"/"+fileName)+",00:00:01,"+ddpDmsDocsDetail.getDddConsignmentId()+","+ddpDmsDocsDetail.getDddJobNumber()+","+fileName+"\n");
					fileNameList.add(fileName);
					exportedReports.add(commonUtil.constructExportReportDomainObject(docs.getCatId(), docs.getCatRulId().getRulId(), ddpDmsDocsDetail.getDddJobNumber(), 
							ddpDmsDocsDetail.getDddConsignmentId(), "Rule By ClientID", fileName, (int)FileUtils.findSizeofFileInKB(filePath + "/" + fileName), typeOfService,
			   				ddpDmsDocsDetail.getDddDocRef(), ddpDmsDocsDetail.getDddMasterJobNumber(), docs.getCatCreatedDate()));
	 			}
	 			i++;
	 		}
	 		isAllFilesDownload = true;
    	} catch (Exception ex) {
    		logger.error("DdpDFCClientComponent.downloadAllDocumentBasedOnVersions() Error occurred while downloading all version based documents",ex.getMessage());
    	}
    	 logger.debug("DdpDFCClientComponent.downloadAllDocumentBasedOnVersions() successfully executed");
    	return isAllFilesDownload;
    }

}
