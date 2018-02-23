/**
 * 
 */
package com.agility.ddp.core.components;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfVirtualDocument;
import com.documentum.fc.client.IDfVirtualDocumentNode;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;

/**
 * @author dguntha
 *
 */
public class DdpVirtualDocBuilder {

	private static final Logger logger = LoggerFactory.getLogger(DdpVirtualDocBuilder.class);
	
	public String createVirtualDocumentObject(IDfSession dfSession,String docName,List<String> rObjectList,String vdLocation,String objType,String appName) {
		
		String sysRootObj = null;
		logger.debug("Inside the createVirtualDocumentObject()");
		try {
			IDfSysObject sysObject = (IDfSysObject)dfSession.newObject(objType);
			sysObject.setObjectName(docName);
			sysObject.setContentType("pdf");
			sysObject.link(vdLocation);
			sysObject.setIsVirtualDocument(true);
			sysObject.save();
			
			IDfVirtualDocument iDfVirtualDocument = sysObject.asVirtualDocument("CURRENT", false);
			IDfVirtualDocumentNode rootNode = iDfVirtualDocument.getRootNode();
			IDfVirtualDocumentNode insertAfterNode = null;
			
			sysObject.checkout();
			
			for (String rObjectID : rObjectList) {
				
				IDfSysObject sysChildObject = (IDfSysObject)dfSession.getObject(new DfId(rObjectID));
				IDfId childChronID = sysChildObject.getChronicleId();
				IDfVirtualDocumentNode childNode = iDfVirtualDocument.addNode(rootNode, insertAfterNode, childChronID, "CURRENT", false,true);
				insertAfterNode = childNode;
			}
			
			sysObject.setObjectName(docName);
			
			sysObject.save();
			sysRootObj = sysObject.getObjectId().toString();
			logger.info("AppName :"+appName+" DdpVirtualDocBuilder.createVirtualDocumentObject() - "+sysRootObj+" System root object for document : "+docName);
			
		} catch (Exception ex) {
			logger.error("AppName :"+appName+" DdpVirtualDocBuilder.createVirtualDocumentObject() - unable to create virtual object for document : "+docName, ex);
		}
		
		return sysRootObj;
		
	}
	
	
}
