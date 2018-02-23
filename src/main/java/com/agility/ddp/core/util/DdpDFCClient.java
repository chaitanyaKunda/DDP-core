package com.agility.ddp.core.util;

import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.PropertySource;
import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfLoginInfo;

@PropertySource("dfc.properties")
public class DdpDFCClient
{
//	@Value( "${dfc.docbaseName}" )
//  String docbaseName  ;
//	String docbaseName = "ZUSLogistics1UAT" ;
	
//	@Value( "${dfc.userName}" )
//  String userName ;
	
	// userName="dmadminuat" ;
//	@Value( "${dfc.password}" )
//  String password ;
	
	//String password ="agildcmnsy";
   // IDfClient  client;
    //IDfSession session;
	    
	     
    private static final Logger logger = LoggerFactory.getLogger(DdpDFCClient.class);
    
//	public Hashtable<String, String> begin(IDfSessionManager sessionMgr, String args,String docbaseName ) {
    public Hashtable<String, String> begin(IDfSession session, String args ) {
		  Hashtable<String, String>  metaData = new Hashtable<String, String>();
//		  IDfSessionManager sMgr =null;
	      try
	      {
	    	logger.info("process started for creating session manager.");
//	        session = sessionMgr.getSession(docbaseName);
	    	//  this.session = session;
	        DMSUtils my_ObjAttr = new com.agility.ddp.core.util.DMSUtils();
	        logger.info("object attributes received.");
	        metaData = my_ObjAttr.DDP_getObjAttributes(args,session);
	        logger.info("metaData received.");
	      }
	      catch(Exception ex)
	      {
	    	 logger.info(ex.toString());
	         ex.printStackTrace();
	      }
	      
		return metaData;
	}
	IDfSessionManager createSessionManager(String docbase, String user, String pass)   {
		IDfSessionManager sMgr =null;
		logger.info("CreateSessionManager: Document source = "+docbase+":User Name="+user+":Password="+pass);
		try{
			    IDfClientX clientx = new DfClientX();
			    IDfClient  client = clientx.getLocalClient();
			    sMgr = client.newSessionManager();
			    IDfLoginInfo loginInfoObj = clientx.getLoginInfo();
			    loginInfoObj.setUser(user);
			    loginInfoObj.setPassword(pass);
			    sMgr.setIdentity(docbase, loginInfoObj);
		}catch(Exception e){
			logger.error("Failed to create session");
			logger.error(e.toString());
			e.printStackTrace();
		}
	    return sMgr;
	}
	
	
	public String exportDocumentToLocal(Integer catDtxId, String strObjId, String expPath, IDfSession session)
	{
		String strObjName = "";
		String strtypeName = "";
		IDfSysObject sysObj = null;
		
		try
		{
			if (session == null)
                throw new Exception("DFC Session is null");

            sysObj = (IDfSysObject) session.getObject(new DfId(strObjId));
            
            if (sysObj == null)
                throw new Exception("Cannot retrieve sysobject: " + strObjId);

            if (sysObj.getContentSize() < 2) //checking for 2 bytes. This can be fine-tuned.
            	throw new Exception("No content/corrupted file");
            
            strObjName = sysObj.getObjectName();
            strtypeName = sysObj.getTypeName();
            
			//Assumed always file will be exported. No FOLDER or Virtual doc will be exported.
            //FOLDER or Virtual doc should be treated separately
            
            strObjName = sysObj.getFile(expPath + "/" +catDtxId+"_"+ strObjName);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		return strObjName;
	}
	
	
	/**
	 * Method used for exporting the all version from the DFC client.
	 * 
	 * @param version
	 * @param objectName
	 * @param strObjId
	 * @param expPath
	 * @param session
	 * @return
	 */
	public String exportDocumentToLocal(String version, String objectName, String strObjId, String expPath, IDfSession session,boolean isVersionNumRequired)
	{
		String strObjName = "";
		IDfSysObject sysObj = null;
		
		try
		{
			if (isVersionNumRequired) {			
				String [] fileNames = objectName.split("\\.");
				String ext = "."+fileNames[1];
				String fileName = objectName.substring(0,  objectName.lastIndexOf(ext));
				objectName = fileName+"_"+version+ext;
			}

			if (session == null)
                throw new Exception("DFC Session is null");

            sysObj = (IDfSysObject) session.getObject(new DfId(strObjId));
            
            if (sysObj == null)
                throw new Exception("Cannot retrieve sysobject: " + strObjId);

            if (sysObj.getContentSize() < 2) //checking for 2 bytes. This can be fine-tuned.
            	throw new Exception("No content/corrupted file");
            
            strObjName = sysObj.getObjectName();
            //strtypeName = sysObj.getTypeName();
            
			//Assumed always file will be exported. No FOLDER or Virtual doc will be exported.
            //FOLDER or Virtual doc should be treated separately
            
            strObjName = sysObj.getFile(expPath + "/" + objectName);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		if (strObjName != null)
			strObjName = objectName;
		
		return strObjName;
	}
	
	
//	public static void main(String[] args){
//		new Test_DDP().begin( args );
//	}
}