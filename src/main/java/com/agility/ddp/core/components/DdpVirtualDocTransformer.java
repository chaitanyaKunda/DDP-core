/**
 * 
 */
package com.agility.ddp.core.components;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfTime;
import com.documentum.services.cts.df.profile.ICTSProfile;
import com.documentum.services.cts.df.profile.ICTSProfileFilter;
import com.documentum.services.dam.df.transform.ICTSService;
import com.documentum.services.dam.df.transform.IMediaProfile;
import com.documentum.services.dam.df.transform.IProfileFormat;
import com.documentum.services.dam.df.transform.IProfileParameter;
import com.documentum.services.dam.df.transform.IProfileService;
import com.documentum.services.dam.df.transform.ITransformRequest;

/**
 * @author dguntha
 *
 */
public class DdpVirtualDocTransformer {

	private static final Logger logger = LoggerFactory.getLogger(DdpVirtualDocTransformer.class);
	
	/**
	 * Method used for transforming the set documents into a single document.
	 * 
	 * @param session
	 * @param sysObjID
	 * @param objType
	 * @param vdLocation
	 * @return
	 */
	public String initiateCTSRequestSetup(IDfSession session,String sysObjID,String objType,String vdLocation,String appName,ApplicationProperties env) {
		
		String repositorySysObjID = null;
		try {
			IDfSysObject sysObject = (IDfSysObject)session.getObject(new DfId(sysObjID));
			String documentName = sysObject.getObjectName();
			IDfSysObject placeHolderObj = createNewDocbaseObject(session, objType, vdLocation, documentName);
			
			if (placeHolderObj == null) {
				logger.info("AppName :"+appName+" DdpVirtualDocTransformer.initiateCTSRequestSetup() - Unable to create new repository & placeholder is empty");
				return repositorySysObjID;
			}
			List<IMediaProfile> mediaProfiles = findProfile(session, sysObjID, "pdf", "pdf");
			
			if (mediaProfiles != null && mediaProfiles.size() > 0) {
				IMediaProfile mediaProfile = mediaProfiles.get(0);
				logger.info("AppName :"+appName+" DdpVirtual. Size of the IMediaProfile is "+mediaProfiles.size());
			//	for (IMediaProfile mediaProfile : mediaProfiles) {
					
					String sysObjectID = sysObject.getObjectId().toString();
					String placeHolderObjID = placeHolderObj.getObjectId().toString();
					logger.info("AppName :"+appName+" SysObjectID : "+sysObjectID+ " : placeHolderObjID : "+placeHolderObjID);
					//System.out.println(" Media Profile : in the virtual "+mediaProfile);
					//logger.info(" Media Profile : in the virtual "+mediaProfile);
					logger.info("AppName :"+appName+" MediaProfile label : "+mediaProfile.getProfileLabel());
					IProfileParameter[] profileParameters = mediaProfile.getParameters();
					
					ICTSService ctsService = (ICTSService) session.getClient().newService(ICTSService.class.getName(),session.getSessionManager());
					profileParameters = setProfileParameters(session, ctsService, profileParameters,appName,env);
					logger.info("AppName :"+appName+" Sending Request to create Transformation object using "+ mediaProfile.getObjectName());																								
					
					ITransformRequest transformRequest = createTransformRequest(session, sysObject, mediaProfile,	"pdf", profileParameters,placeHolderObj.getObjectId(), false);
					
					logger.info("AppName :"+appName+" TransformRequest: "+transformRequest.getObjectId());
					
					logger.info("AppName :"+appName+" Is Transform Request Valid: "+transformRequest.isValid());
					
					//notifyUser, synchronizeCmd, deleteOnCompletion
					ctsService.submitRequest(session, transformRequest, false, false, false);
																														
					logger.info("AppName :"+appName+" Request submitted to CTS server successfully...");
					
					//CTS response handling								
					String strReqId = transformRequest.getObjectId().toString();
					logger.info("AppName :"+appName+" RequestID: "+strReqId);
					
					logger.info("AppName :"+appName+" Request Name: "+transformRequest.getObjectName());
																													
					
					//sleep for one and half minute
					Thread.sleep(90000);
					
					logger.debug("Response Object Qualification: dm_cts_response where object_name = 'cts_response_merge_virtualdoc_adts.xml' and profile_name = 'merge_virtualdoc_adts' and source_object_id = '"+sysObjectID+"' and transformation_type = 'dm_transcode_content' and parameters like '%-profile_id=\""+mediaProfile.getObjectId().toString()+"\"%-related_object_id=\""+placeHolderObjID+"\" -arg_object_id=\""+strReqId+"\"' and (queueitem_signed_off_time is not nulldate or trans_completed_time is not nulldate) and job_status <> 'Pending' order by r_modify_date desc");
														
					IDfSysObject ctsResponseObj = (IDfSysObject) session.getObjectByQualification("dm_cts_response where object_name = 'cts_response_merge_virtualdoc_adts.xml' and profile_name = 'merge_virtualdoc_adts' and source_object_id = '"+sysObjectID+"' and transformation_type = 'dm_transcode_content' and parameters like '%-profile_id=\""+mediaProfile.getObjectId().toString()+"\"%-related_object_id=\""+placeHolderObjID+"\" -arg_object_id=\""+strReqId+"\"' and (queueitem_signed_off_time is not nulldate or trans_completed_time is not nulldate) and job_status <> 'Pending' order by r_modify_date desc");
					
					int intCount = 0;
					while(null == ctsResponseObj && intCount <= 5){
						logger.info("AppName :"+appName+" Waiting to retrieve the CTS response object...count: "+intCount);
						//sleep for one minute
						Thread.sleep(60000);
						ctsResponseObj = (IDfSysObject) session.getObjectByQualification("dm_cts_response where (object_name = 'cts_response_merge_virtualdoc_adts.xml' and profile_name = 'merge_virtualdoc_adts' and source_object_id = '"+sysObjectID+"' and transformation_type = 'dm_transcode_content' and parameters like '%-profile_id=\""+mediaProfile.getObjectId().toString()+"\"%-related_object_id=\""+placeHolderObjID+"\" -arg_object_id=\""+strReqId+"\"') and (queueitem_signed_off_time is not nulldate or trans_completed_time is not nulldate) and job_status <> 'Pending' order by r_modify_date desc");
						//CTSResponseObj = (IDfSysObject) dfSession.getObjectByQualification("dm_cts_response where (object_name = 'cts_response_merge_virtualdoc_adts.xml' and profile_name = 'merge_virtualdoc_adts' and source_object_id = '"+strSourceObjId+"' and transformation_type = 'synchronous' and parameters like '%-profile_id=\""+mediaProfile.getObjectId().toString()+"\"%%-related_object_id=\""+strPlaceholderObjId+"\"% %-arg_object_id=\""+strReqId+"\"%') and (queueitem_signed_off_time is not nulldate or trans_completed_time is not nulldate) and job_status <> 'Pending' order by r_modify_date desc");
						intCount = intCount+1;
					}
					
					if (ctsResponseObj != null) {
						
						ctsResponseObj.fetch(null);
						String ctsResponseObjID = ctsResponseObj.getObjectId().toString();
						logger.info("AppName :"+appName+" CTS Response object id : "+ctsResponseObjID);
						
						intCount = 0;
						String requestStatus = null;
						boolean status = true;
						
						while(status){
							
							ctsResponseObj.fetch(null);
							if (ctsResponseObj.getString("job_status").equalsIgnoreCase("Pending") || ctsResponseObj.getString("job_status").equalsIgnoreCase("InProgress") ) {
								
								logger.info("AppName :"+appName+" Waiting to retrieve the job status from CTS response object...count: "+intCount);												
								requestStatus = ctsResponseObj.getString("job_status");
								logger.info("AppName :"+appName+" Request Status: "+requestStatus);
								
								//sleep for one and half minute
								Thread.sleep(90000);																																
								
								intCount = intCount+1;
								
								if(intCount == 20){													
									status = false;
								}
							} else {
								requestStatus = ctsResponseObj.getString("job_status");
								logger.info("AppName :"+appName+" Request Status: "+requestStatus);
								status = false;
								
							}
							
						}
						
						String strJobErrorDetails = null;
						if(!ctsResponseObj.getString("job_error_details").equals("") && !ctsResponseObj.getString("job_error_details").equals(" ")){
							strJobErrorDetails = ctsResponseObj.getString("job_error_details");
							logger.error("Request Error Details: "+strJobErrorDetails);
						}
						
						IDfTime transformationCompletedTime = null;
						boolean isCompeted = true;
						while (isCompeted) {
							
							ctsResponseObj.fetch(null);
							transformationCompletedTime = ctsResponseObj.getTime("trans_completed_time");
							
							if (transformationCompletedTime == null) {
								transformationCompletedTime = ctsResponseObj.getTime("trans_completed_time");									
								System.out.println("Job Completed Time: "+transformationCompletedTime);
								
								//sleep for one minute
								Thread.sleep(60000);																																	
								
								intCount = intCount+1;
								if(intCount == 5){													
									isCompeted = false;
								}
							} else {
								transformationCompletedTime = ctsResponseObj.getTime("trans_completed_time");													
								logger.info("Job Completed Time: "+transformationCompletedTime);
								isCompeted = false;
							}
						}
						
					if(ctsResponseObj.getString("job_status") != null && ctsResponseObj.getString("job_status").equalsIgnoreCase("Completed")) {
						repositorySysObjID = placeHolderObj.getObjectId().toString();
					}
						
					}
					
					
				//}
			}
			
		} catch(Exception ex) {
			logger.info("DdpVirtual- Error : "+ ex.getMessage());
			logger.error("AppName : "+appName+". DdpVirtualDocTransformer.initiateCTSRequestSetup() - unable to merge the documents", ex);
			System.out.println("DdpVirtualDocs error : "+ex.getMessage());
			ex.printStackTrace();
		}
		return repositorySysObjID;
		
	}
	
	/**
	 * Method used for creating the new document base object.
	 * 
	 * @param session
	 * @param objType
	 * @param vdLocation
	 * @param documentName
	 * @return
	 */
	private IDfSysObject createNewDocbaseObject(IDfSession session,String objType,String vdLocation,String documentName) {
		
		IDfSysObject sysObject = null;
		try {
			sysObject =  (IDfSysObject)session.newObject(objType);
			sysObject.setObjectName(documentName);
			sysObject.setContentType("pdf");
			sysObject.link(vdLocation);
			sysObject.save();
		} catch (Exception ex) {
			logger.error("DdpVirtualDocTransformer-createNewDocbaseObject() - unable to create new System object", ex);
		}
		
		return sysObject;
		
	}
	
	/**
	 * Method used for finding the profiles.
	 * 
	 * @param dfSession
	 * @param strSrcObjectId
	 * @param strSrcFormat
	 * @param strTargetFormat
	 * @return
	 */
	private List<IMediaProfile> findProfile(IDfSession dfSession,
			String strSrcObjectId, String strSrcFormat, String strTargetFormat) {
		
		IDfClientX dfClientX = null;
		IDfClient dfClient = null;
		List<IMediaProfile> identifiedMediaProfileList = null;
		
		try {
			
			dfClientX = new DfClientX();
			dfClient = dfClientX.getLocalClient();
			IDfSessionManager sessionManager = dfSession.getSessionManager();
			IProfileService profileService = (IProfileService) dfClient	.newService(IProfileService.class.getName(), sessionManager);
			String[] categ = {IMediaProfile.CATEGORY_ADTS,IMediaProfile.CATEGORY_PUBLIC};
			IMediaProfile[] mediaProfiles = profileService.getProfiles(dfSession, strSrcFormat, strSrcObjectId,categ );
			
			if (mediaProfiles != null) {
				
				identifiedMediaProfileList = new ArrayList<IMediaProfile>();
				
				for (IMediaProfile mediaProfile : mediaProfiles) {
					
					IProfileFormat[] iProfileFormats = mediaProfile.getFormats();
					ICTSProfile ctsProfile = (ICTSProfile)mediaProfile;
					
					for (IProfileFormat iProfileFormat : iProfileFormats) {
						if ((!iProfileFormat.getSourceFormat().equals(strSrcFormat))
								|| (!iProfileFormat.getTargetFormat().equals(strTargetFormat)))
						{
							continue;
						}
						
						ICTSProfileFilter[] profileFilters = ctsProfile.getProfileFilters();							
						if (profileFilters != null) {
							for (ICTSProfileFilter ictsProfileFilter : profileFilters) {
								String filterName = ictsProfileFilter.getFilterName();
								String[] filterValues = ictsProfileFilter.getFilterValues();
								if (filterName != null && filterValues != null) {
									for (String filterValue : filterValues) {
										if ((filterName.equalsIgnoreCase("CTSProduct") 
												&& filterValue.equalsIgnoreCase(IMediaProfile.CATEGORY_ADTS))
												|| (filterName.equalsIgnoreCase("Visibility") 
												&& filterValue.equalsIgnoreCase(IMediaProfile.CATEGORY_PUBLIC))) 
										{
											String strCTSProfileName = ctsProfile.getObjectName();
											logger.debug("Profile found: "+ ctsProfile.getObjectName());
											
											if (null != strCTSProfileName && strCTSProfileName.equalsIgnoreCase("merge_virtualdoc_adts")) 
											{
												logger.info("Required Profile found: "+ ctsProfile.getObjectName());
												identifiedMediaProfileList.add(ctsProfile);
												break;
											}
										}
									}
									
								}
							}
							
						}
					}
					
				}
			}
			
		} catch (Exception ex) {
			logger.error("DdpVirtualDocTransformer.findProfile() - Unable to find the profiles", ex);
		}
		
		return identifiedMediaProfileList;
	}
	
	/**
	 * Method used for setting the profile parameters.
	 * 
	 * @param dfSession
	 * @param ctsService
	 * @param profileparameter
	 * @return
	 */
	private IProfileParameter[] setProfileParameters(IDfSession dfSession,ICTSService ctsService, IProfileParameter[] profileparameter,String appName,ApplicationProperties env) {
		
		String tableOfContent = env.getProperty("merge.tableofcontent.display."+(appName.contains(":") ? appName.split(":")[0] : appName));
		
		if (tableOfContent == null || tableOfContent.isEmpty()) {
			tableOfContent = env.getProperty("merge.tableofcontent.display");
		}
		//Table of contents
		setProfileParameter(ctsService, profileparameter, "doc_token_toc", (tableOfContent != null && !tableOfContent.isEmpty() ? tableOfContent.trim() : "Yes"));
		setProfileParameter(ctsService, profileparameter, "doc_token_showRelationship", "No");
		
		//PDF Default Settings 
		setProfileParameter(ctsService, profileparameter,"doc_token_changeDefaultSettings", "Yes");	
		
		//PDF settings
		setProfileParameter(ctsService, profileparameter, "doc_token_enableBookMarks", "No");
		setProfileParameter(ctsService, profileparameter, "doc_token_enableHyperlinks", "Yes");
		setProfileParameter(ctsService, profileparameter, "doc_token_pdfVersion", "PDFVersion15");		
		setProfileParameter(ctsService, profileparameter, "doc_token_resolution", "300");
		setProfileParameter(ctsService, profileparameter, "doc_token_optimize", "Yes");
		
        //Security settings       
		setProfileParameter(ctsService, profileparameter, "doc_token_enableSecurity", "No");
        setProfileParameter(ctsService, profileparameter, "doc_token_encryptionMode", "40bit");
        setProfileParameter(ctsService, profileparameter, "doc_token_secOpass", " ");
        setProfileParameter(ctsService, profileparameter, "doc_token_secCpass", " ");	
        setProfileParameter(ctsService, profileparameter, "doc_token_enableAccess", "Disabled");
        setProfileParameter(ctsService, profileparameter, "doc_token_allowCopy", "Disabled");
        setProfileParameter(ctsService, profileparameter, "doc_token_changesAllowed", "Disabled");
        setProfileParameter(ctsService, profileparameter, "doc_token_printing", "Disabled");
        setProfileParameter(ctsService, profileparameter, "doc_token_docAssembly", "Disabled");
        setProfileParameter(ctsService, profileparameter, "doc_token_formFieldFilling", "Disabled");
       	                                						    
		return profileparameter;

	}
	
	/**
	   * Method to set profile parameter
	   * 
	   * @param ctsService
	   *				ICTSService
	   * @param profileparameter
	   * 				IProfileParameter[]
	   * @param strTokenName
	   * 				String
	   * @param strTokenValue
	   * 				String
	   */
		private void setProfileParameter(ICTSService ctsService,IProfileParameter[] profileparameter, String strTokenName,String strTokenValue)
		{

			int i = 0;
			for (boolean flag = false; (i < profileparameter.length) && (!flag); i++)
				if (profileparameter[i].getParameterName().compareTo(strTokenName) == 0) {
					profileparameter[i].setParameterValue(strTokenValue);
					flag = true;
				}
		}
		
		
		
		/**
		 * Method to create transform request
		 * 
		 * @param dfSession
		 * 				IDfSession
		 * @param objectToTransform
		 * 				IDfSysObject
		 * @param iMediaProfile
		 * 				IMediaProfile
		 * @param strTargetFormat
		 * 				String
		 * @param aiprofileparameter
		 * 				IProfileParameter[]
		 * @param dfId
		 * 				IDfId
		 * @param flag
		 * 				boolean
		 * @param printWriter
		 * 				PrintWriter
		 * 
		 * @return ITransformRequest
		 * @throws Exception
		 */
		private ITransformRequest createTransformRequest(IDfSession dfSession, IDfSysObject objectToTransform,IMediaProfile iMediaProfile, String strTargetFormat,
				IProfileParameter[] aiprofileparameter, IDfId placeholderObjId, boolean flag) throws Exception {
			
			logger.info("Inside createRenditionTransformRequestNew method...");

			logger.info("Media Profile Notify Result Value: "+iMediaProfile.getNotifyResult());
			logger.info(">>>>> Source ObjectId: "+objectToTransform.getObjectId().toString());
			logger.info("Media ProfileId: "+iMediaProfile.getObjectId().toString());
		//	logger.info(">>>>> Placeholder ObjId: "+placeholderObjId.toString());
			
		//	String strDocbaseName = MergeDocsMain.prop.getProperty(STR_DOCBASE_NAME_PROPERTY_KEY);
			//logger.info("Docbase Name: " + strDocbaseName);
			
			ITransformRequest itransformrequest = (ITransformRequest) dfSession.newObject("dm_transform_request");
			
			itransformrequest.setSourceObjectId(objectToTransform.getObjectId().toString());
			itransformrequest.setMediaProfileId(iMediaProfile.getObjectId().toString());
			
			itransformrequest.setSourceFormat(objectToTransform.getContentType());
			itransformrequest.setTargetFormat(strTargetFormat); 
			
			if (placeholderObjId != null)
				itransformrequest.setRelatedObjectId(placeholderObjId.toString());
			else
				itransformrequest.setRelatedObjectId(null);
			
			itransformrequest.setLocale(Locale.getDefault());
			itransformrequest.setParameters(aiprofileparameter);
			itransformrequest.setMediaProfileName(iMediaProfile.getObjectName());
			itransformrequest.setMediaProfileLabel(iMediaProfile.getProfileLabel());
			itransformrequest.setNotifyResult(iMediaProfile.getNotifyResult());
			itransformrequest.setDefaultProxy(flag);
			itransformrequest.setPriority(1);
			itransformrequest.setSourcePageModifier("");
			itransformrequest.setTargetPageModifier("");
			itransformrequest.setSourcePage(0);
			itransformrequest.setTargetPage(0);
			
			//Docbase owner name		
			itransformrequest.setOwnerName(dfSession.getDocbaseOwnerName());
			
			itransformrequest.save();
			
			return itransformrequest;
		}
		
}
