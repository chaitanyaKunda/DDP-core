package com.agility.ddp.core.util;

import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.documentum.fc.client.*;
import com.documentum.fc.common.*;


public class DMSUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(DMSUtils.class);
	
	//IDfSession m_session = null;
    //IDfClientX m_clientX = null;
  //  IDfSysObject sysObject = null;
    
	public DMSUtils() {
	   // m_session = session;
	   // m_clientX = new DfClientX();
	}
	public  Hashtable<String, String> DDP_getObjAttributes(String objectID,IDfSession session ) throws Exception {
	logger.info("establishing connection for getting IdfSysObject started.");
	IDfSysObject sysObject = (IDfSysObject) session.getObject(new DfId(objectID));
	logger.info("IdfSysObject data received.");
	
	Hashtable<String, String> dms_attr = new Hashtable<String, String>();
	//Add key/value pairs into the hashtable
	dms_attr.put("r_object_id", objectID); 
	dms_attr.put("object_name", sysObject.getString("object_name")); 
	dms_attr.put("agl_job_numbers", sysObject.getString("agl_job_numbers"));
	
	dms_attr.put("agl_control_doc_type",sysObject.getString("agl_control_doc_type"));
	dms_attr.put("agl_consignment_id",sysObject.getString("agl_consignment_id"));
	dms_attr.put("agl_doc_ref",sysObject.getString("agl_doc_ref"));
	dms_attr.put("agl_global_doc_ref",sysObject.getString("agl_global_doc_ref"));
	dms_attr.put("agl_carrier_ref",sysObject.getString("agl_carrier_ref"));
	dms_attr.put("agl_company_source",sysObject.getString("agl_company_source"));
	dms_attr.put("agl_company_destination",sysObject.getString("agl_company_destination"));
	dms_attr.put("agl_branch_source",sysObject.getString("agl_branch_source"));
	dms_attr.put("agl_branch_destination",sysObject.getString("agl_branch_destination"));
	dms_attr.put("agl_dept_source",sysObject.getString("agl_dept_source"));
	dms_attr.put("agl_dept_destination",sysObject.getString("agl_dept_destination"));		
	dms_attr.put("agl_bill_of_lading_number",sysObject.getString("agl_bill_of_lading_number"));
	dms_attr.put("agl_master_airway_bill_num",sysObject.getString("agl_master_airway_bill_num"));
	dms_attr.put("agl_house_airway_bill_num",sysObject.getString("agl_house_airway_bill_num"));
	dms_attr.put("agl_master_job_number",sysObject.getString("agl_master_job_number"));
	dms_attr.put("agl_client_id",sysObject.getString("agl_client_id"));
	dms_attr.put("agl_shipper",sysObject.getString("agl_shipper"));
	dms_attr.put("agl_consignee",sysObject.getString("agl_consignee"));
	dms_attr.put("agl_notify_party",sysObject.getString("agl_notify_party"));
	dms_attr.put("agl_debits_back",sysObject.getString("agl_debits_back"));
	dms_attr.put("agl_debits_forward",sysObject.getString("agl_debits_forward"));
	dms_attr.put("agl_final_agent_id",sysObject.getString("agl_final_agent_id"));
	dms_attr.put("agl_generating_system",sysObject.getString("agl_generating_system"));
	dms_attr.put("agl_initial_agent",sysObject.getString("agl_initial_agent"));
	dms_attr.put("agl_intermediate_agent",sysObject.getString("agl_intermediate_agent"));
	dms_attr.put("agl_customs_entry_no",sysObject.getString("agl_customs_entry_no"));
	dms_attr.put("agl_is_rated",sysObject.getString("agl_is_rated"));
	dms_attr.put("agl_user_id",sysObject.getString("agl_user_id"));
	dms_attr.put("agl_user_reference",sysObject.getString("agl_user_reference"));
	dms_attr.put("agl_file_type",sysObject.getString("agl_file_type"));
	dms_attr.put("agl_note",sysObject.getString("agl_note"));
	dms_attr.put("a_content_type",sysObject.getString("a_content_type"));
	dms_attr.put("r_content_size",sysObject.getString("r_content_size"));
	dms_attr.put("i_chronicle_id",sysObject.getString("i_chronicle_id"));
	dms_attr.put("r_version_label", sysObject.getString("r_version_label"));
		  
	return dms_attr;
		
		
	}

}