package com.agility.ddp.core.util;


public class Constant 
{
	
	public static final String EXECUTION_STATUS_SUCCESS = "success";
	public static final String EXECUTION_STATUS_FAILED   = "failed";
	
	public static final String COMPRESSION_LEVEL_MERGE = "merge";
	/***************************************************************
	 *********************  Job Relevant SQLs **********************
	 ***************************************************************/
	
	/*********************  Querying Job Ref ID ***********************/
	public static final String DEF_SQL_SELECT_JOB_REF_HOLDER_ID		 			= "SELECT JRF_ID,JRF_JOB_ID,JRF_STATUS,JRF_CREATED_DATE,JRF_MODIFIED_DATE FROM DDP_JOB_REF_HOLDER WHERE JRF_JOB_ID=?";
	
	public static final String SELECT_JOB_REF_HOLDER_BY_ID		 			= "SELECT JRF_ID,JRF_JOB_ID,JRF_STATUS,JRF_CREATED_DATE,JRF_MODIFIED_DATE FROM DDP_JOB_REF_HOLDER WHERE JRF_ID=? AND JRF_JOB_ID=?";
	
	
	 
	/*********************  Querying Max(SYN_ID) for DDP_DMS_DOCS_HOLDER ***********************/
	public static final String DEF_SQL_SELECT_DDP_DMS_DOCS_HOLDER_MAX_SYN_ID		=  "SELECT MAX(THL_SYN_ID) THL_SYN_ID FROM DDP_DMS_DOCS_HOLDER";
	
	
	
	/*********************  Querying Max(CAT_ID) for DDP_CATEGORIZED_DOCS ***********************/
	public static final String DEF_SQL_SELECT_DDP_CATEGORIZED_DOCS_MAX_CAT_ID		=  "select Max(CAT_ID) CAT_ID from DDP_CATEGORIZED_DOCS;";
	
	
	/*********************  Querying Max(CHL_ID) for DDP_CATEGORIZATION_HOLDER ***********************/
	public static final String DEF_SQL_SELECT_DDP_CATEGORIZATION_HOLDER_MAX_CHL_ID	=  "select Max(CHL_ID) CHL_ID from DDP_CATEGORIZATION_HOLDER";
	
	/*********************  Querying Max(SYN_ID) for DDP_DMS_DOCS_HOLDER where less than the current Failed OR In Progress SYN_ID ***********************/
	public static final String DEF_SQL_SELECT_DDP_DMS_DOCS_HOLDER_PREV_MAX_SYN_ID	="SELECT MAX(THL_SYN_ID) THL_SYN_ID FROM DDP_DMS_DOCS_HOLDER WHERE THL_SYN_ID < ?";
	
	/*********************  Querying Max(CHL_ID) for DDP_CATEGORIZATION_HOLDER where less than the current Failed OR In Progress CHL_ID ***********************/
	public static final String DEF_SQL_SELECT_DDP_CATEGORIZATION_HOLDER_PREV_MAX_CHL_ID	="SELECT MAX(CHL_ID) CHL_ID FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_ID <  ? ";
	
	/*********************  Querying Max(CAT_ID) for DDP_CATEGORIZATION_DOCS where less than the current Failed OR In Progress CAT_ID ***********************/
	public static final String DEF_SQL_SELECT_DDP_CATEGORIZED_DOCS_PREV_MAX_CAT_ID	="SELECT MAX(CAT_ID) CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_ID < ?";
		
	
	/*********************  Querying DMS_DDP_SYN table for greater than Max(SYN_ID) ***********************/
	public static final String DEF_SQL_SELECT_DMS_DDP_SYN_GREATER_THAN_MAX_SYN_ID	="SELECT * FROM DMS_DDP_SYN WHERE SYN_ID > ?";
	
	
	/*********************  Querying CHL_ID Categorization table for greater than Max(CHL_ID) ***********************/
	public static final String DEF_SQL_SELECT_CATEGORIZATION_THAN_MAX_CHL_ID	="SELECT * FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_ID > ?";
	
	/*********************  Querying CHL_ID Categorizeddocs table for greater than Max(CAT_ID) ***********************/
	public static final String DEF_SQL_SELECT_CATEGORIZED_DOCS_GREATER_THAN_MAX_CAT_ID	="SELECT * FROM DDP_CATEGORIZED_DOCS  WHERE CAT_ID > ?";
	
	
	/*********************  Categorization Job Queries - to Sync DmsDdpSynAndDdpDmsDocsHolder***********************/
	public static final String DEF_SQL_INSERT_DDP_DMS_DOCS_HOLDER					="INSERT INTO DDP_DMS_DOCS_HOLDER (THL_SYN_ID, THL_R_OBJECT_ID, THL_IS_PROCESS_REQ, THL_TBO_NAME) VALUES(?, ?, ?, ?)";
	
	/*********************  Categorization Job Queries - to insert records into DdpCategorizationHolder ***********************/
	public static final String DEF_SQL_INSERT_DDP_CATEGORIZATION_HOLDER				="INSERT INTO DDP_CATEGORIZATION_HOLDER (CHL_SYN_ID, CHL_DTX_ID, CHL_RUL_ID) VALUES(?, ?, ?)";
	
	/*********************  Querying DDP_CATEGORIZATION_HOLDER for Distinct DTX_ID ***********************/
	public static final String DEF_SQL_GET_DISTINCT_DDP_DMS_DTX						="SELECT DISTINCT CHL_DTX_ID FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_ID IN (?,?)";
	
	/*********************  Querying DDP_CATEGORIZATION_HOLDER for No.Of Rules by DTX_ID ***********************/
	public static final String DEF_SQL_GET_RULE_COUNT_BY_DTX_ID						="SELECT COUNT(*)RULESCOUNT FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_DTX_ID=?";
	
	
	/***************************************************************
	 *********************  Check Rules SQLs **********************
	 ***************************************************************/
	
	/*********************  Query to check AED rules for all status as active (RDT_STATUS, AED_STATUS, RUL_STATUS = 0)***********************/
	public static final String DEF_SQL_SELECT_AED_RULE								="SELECT DETAILS.Rdt_id FROM DDP_RULE RULES, DDP_AED_RULE AED_RULES, DDP_RULE_DETAIL DETAILS, DDP_PARTY PARTY "+
																						" WHERE AED_RULES.AED_RULE_ID = RULES.RUL_ID AND DETAILS.RDT_RULE_ID = RULES.RUL_ID AND "+
																						" PARTY.PTY_PARTY_CODE = DETAILS.RDT_PARTY_CODE AND DETAILS.RDT_RULE_TYPE = 'AED_RULE' AND DETAILS.RDT_STATUS = 0 "+
																						" AND AED_RULES.AED_STATUS = 0 AND RULES.RUL_STATUS = 0";

	public static final String DMSTEAMEmailId 										= "Rnagarathinam@agility.com";

	//Get Login User Name
	public static  String USERLOGINIDCORE											= "admin";
	
	public static String DEF_SQL_SELECT_DOCS_DETAIL									=  "select * from DDP_DMS_DOCS_DETAIL where DDD_DTX_ID = ?";
	
	public static String DEF_AQL_SELECT_DTX_ID										=	"select distinct CHL_DTX_ID,* from DDP_CATEGORIZATION_HOLDER" ;
	
	public static String DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY                        = "SELECT ddd.DDD_USER_ID,cat.CAT_MODIFIED_DATE FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd"
																					+ " WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID and cat.CAT_RULE_TYPE = 'AED_RULE' and  cat.CAT_STATUS = 1 "
																					+ "and ddd.DDD_COMPANY_SOURCE = ? and ddd.DDD_CONTROL_DOC_TYPE = ?  and ddd.DDD_DOC_REF =? and"
																					+ " ddd.DDD_CONSIGNMENT_ID = ? and ddd.DDD_JOB_NUMBER =? and cat.CAT_RUL_ID =? and cat.CAT_ID != ? and cat.CAT_CREATED_DATE >= ? ";
	public static String DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY_WITHOUT_TIME           = "SELECT ddd.DDD_USER_ID,cat.CAT_MODIFIED_DATE FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd"
																					+ " WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID and cat.CAT_RULE_TYPE = 'AED_RULE' and  cat.CAT_STATUS = 1 "
																					+ "and ddd.DDD_COMPANY_SOURCE = ? and ddd.DDD_CONTROL_DOC_TYPE = ?  and ddd.DDD_DOC_REF =? and"
																					+ " ddd.DDD_CONSIGNMENT_ID = ? and ddd.DDD_JOB_NUMBER =? and cat.CAT_RUL_ID =? and cat.CAT_ID != ?";
																					
	public static String DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY_WITH_OUT_CAT_STATUS   = "SELECT ddd.DDD_USER_ID,cat.CAT_MODIFIED_DATE FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd"
																					+ " WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID and cat.CAT_RULE_TYPE = 'AED_RULE' "
																					+ "and ddd.DDD_COMPANY_SOURCE = ? and ddd.DDD_CONTROL_DOC_TYPE = ?  and ddd.DDD_DOC_REF =? and"
																					+ " ddd.DDD_CONSIGNMENT_ID = ? and ddd.DDD_JOB_NUMBER =? and cat.CAT_RUL_ID =? and cat.CAT_ID != ? and cat.CAT_CREATED_DATE >= ? ";
	
	public static String DEF_SQL_AED_ALREADY_MAIL_SEND_QUERY_COUNT				   = "SELECT ddd.DDD_USER_ID,cat.CAT_MODIFIED_DATE FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd"
																					+ " WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID and cat.CAT_RULE_TYPE = 'AED_RULE' "
																					+ "and ddd.DDD_COMPANY_SOURCE = ? and ddd.DDD_CONTROL_DOC_TYPE = ?  and ddd.DDD_DOC_REF =? and"
																					+ " ddd.DDD_CONSIGNMENT_ID = ? and ddd.DDD_JOB_NUMBER =? and cat.CAT_RUL_ID =? and cat.CAT_ID != ? and cat.CAT_CREATED_DATE >= ? ";
																																						
	/*********************  Querying Last Processed CHL DTX ID from DDP_CATEGORIZATION_HOLDER ***********************/
	public static final String DEF_SQL_SELECT_LAST_PROCESSED_CAT_HOLDER_ID			= 	"SELECT CHL_ID, CHL_SYN_ID, CHL_DTX_ID, CHL_RUL_ID FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_DTX_ID =? AND CHL_RUL_ID=?";
	
	
	/*********************  Querying Last Processed CAT ID from DDP_CATEGORIZED_DOCS ***********************/
	public static final String DEF_SQL_SELECT_LAST_PROCESSED_CAT_DOCS_ID			= 	"SELECT MIN(CAT_ID) from DDP_CATEGORIZED_DOCS WHERE CAT_STATUS=0";
	
	
	/*********************  Query to check Export rules for all status as active (RDT_STATUS, AED_STATUS, RUL_STATUS = 0)***********************/
	public static final String DEF_SQL_SELECT_EXPORT_RULE								="SELECT DETAILS.RDT_ID FROM DDP_RULE RULES, DDP_EXPORT_RULE EXPORT_RULES, DDP_RULE_DETAIL DETAILS, DDP_PARTY PARTY "+
																						 "WHERE EXPORT_RULES.EXP_RULE_ID = RULES.RUL_ID AND DETAILS.RDT_RULE_ID = RULES.RUL_ID AND "+
																						 "PARTY.PTY_PARTY_CODE = DETAILS.RDT_PARTY_CODE AND DETAILS.RDT_RULE_TYPE = 'EXPORT_RULE' AND DETAILS.RDT_STATUS = 0 "+
																						 "AND EXPORT_RULES.EXP_STATUS = 0 AND RULES.RUL_STATUS = 0";
	
	/*********************  Query to fetch matched Scheduler ID and Rule Detail ID for Export rules for all status as active (SCH_STATUS, RDT_STATUS, EXP_STATUS, RUL_STATUS = 0)***********************/
	public static final String DEF_SQL_SELECT_MATCHED_SCH_RULE_DET_EXPORT				="SELECT RULE_DET.RDT_ID FROM DDP_SCHEDULER SCHED, DDP_EXPORT_RULE EXP_RULE, DDP_RULE_DETAIL RULE_DET, DDP_RULE RUL "+
																						 "WHERE EXP_RULE.EXP_SCHEDULER_ID = SCHED.SCH_ID AND RULE_DET.RDT_RULE_ID = EXP_RULE.EXP_RULE_ID AND RUL.RUL_ID = EXP_RULE.EXP_RULE_ID "+
																						 "AND SCHED.SCH_STATUS = 0 AND EXP_RULE.EXP_STATUS = 0 AND RULE_DET.RDT_STATUS = 0 AND RUL.RUL_STATUS = 0 AND SCHED.SCH_ID = ?";
	
	/*********************  Query to fetch matched Scheduler ID and Rule Detail ID for Export rules for all status as active (SCH_STATUS, RDT_STATUS, EXP_STATUS, RUL_STATUS = 0)***********************/
	public static final String DEF_SQL_SELECT_MATCHED_CAT_DOCS							="SELECT CAT_DOCS.CAT_ID FROM DDP_CATEGORIZED_DOCS CAT_DOCS WHERE CAT_DOCS.CAT_RDT_ID = ? AND (CAT_DOCS.CAT_CREATED_DATE BETWEEN ? and ?)";
	
	public static final String DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_JOB_NUMBER   			="SELECT cat.CAT_ID FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID  and ddd.DDD_JOB_NUMBER in (dynamiccondition) and cat.CAT_RDT_ID = ? AND (cat.CAT_CREATED_DATE BETWEEN ? and ?)";
	
	public static final String DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_CONSIGNMENT_ID        	="SELECT cat.CAT_ID FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID  and ddd.DDD_CONSIGNMENT_ID in (dynamiccondition) and cat.CAT_RDT_ID = ? AND (cat.CAT_CREATED_DATE BETWEEN ? and ?)";
	
	public static final String DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_DOC_REFS               ="SELECT cat.CAT_ID FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd WHERE cat.CAT_DTX_ID = ddd.DDD_DTX_ID  and ddd.DDD_DOC_REF in (dynamiccondition) and cat.CAT_RDT_ID = ? AND (cat.CAT_CREATED_DATE BETWEEN ? and ?)";
	
	/********************** Used in DdpRuleSchedulerJob ************************************************/
	public static final String DEF_SQL_SELECT_MATCHED_EXPORT_RULE_COMM_ID            = "SELECT FTP.CFT_FTP_ID FROM DDP_COMM_FTP FTP, DDP_COMMUNICATION_SETUP COMS,DDP_EXPORT_RULE  EXP_RULE WHERE FTP.CFT_FTP_ID = COMS.CMS_PROTOCOL_SETTINGS_ID AND COMS.CMS_COMMUNICATION_ID = EXP_RULE.EXP_COMMUNICATION_ID and EXP_RULE.EXP_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_EXPORT_RULE_UNC_COMM_ID        = "SELECT UNC.CUN_UNC_ID FROM DDP_COMM_UNC UNC, DDP_COMMUNICATION_SETUP COMS,DDP_EXPORT_RULE  EXP_RULE WHERE UNC.CUN_UNC_ID = COMS.CMS_PROTOCOL_SETTINGS_ID AND COMS.CMS_COMMUNICATION_ID = EXP_RULE.EXP_COMMUNICATION_ID and EXP_RULE.EXP_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_DMS_DOCS_DETAILS               = "SELECT DDD_ID FROM  DDP_DMS_DOCS_DETAIL WHERE DDD_DTX_ID  = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_COMMUNICATION_SET_ID           = "SELECT COMS.CMS_COMMUNICATION_ID FROM  DDP_COMMUNICATION_SETUP COMS,DDP_RULE_DETAIL DETAIL where COMS.CMS_COMMUNICATION_ID = DETAIL.RDT_COMMUNICATION_ID and DETAIL.RDT_ID = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_CONV_ID                       = "SELECT EXP_DOCNAME_CONV_ID from DDP_EXPORT_RULE WHERE EXP_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_SCHEDULER_ID                  = "SELECT EXP_SCHEDULER_ID FROM DDP_EXPORT_RULE WHERE EXP_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_MACTED_EXPORT_RULE_ID                  = "SELECT EXP_RULE_ID FROM DDP_EXPORT_RULE WHERE EXP_SCHEDULER_ID = ?";
	
	public static final String DEF_SQL_SELECT_MACTHED_VERSION_SETUP_ID              = "SELECT EVS_OPTION FROM DDP_EXPORT_VERSION_SETUP WHERE EVS_RDT_ID = ?";
	
	public static final String DEF_SQL_SELECT_MACHED_EXPORT_SCH_CAT_ID               = "SELECT CAT_DOCS.CAT_ID  FROM DDP_SCHEDULER SCHED, DDP_EXPORT_RULE EXP_RULE, DDP_CATEGORIZED_DOCS CAT_DOCS, DDP_RULE RUL "
																						+ "WHERE EXP_RULE.EXP_SCHEDULER_ID = SCHED.SCH_ID AND  CAT_DOCS.CAT_RUL_ID = EXP_RULE.EXP_RULE_ID  AND"
																						+ "  RUL.RUL_ID = EXP_RULE.EXP_RULE_ID AND SCHED.SCH_STATUS = 0"
																						+ " AND EXP_RULE.EXP_STATUS = 0  AND RUL.RUL_STATUS = 0 AND SCHED.SCH_ID = ? and CAT_DOCS.CAT_CREATED_DATE BETWEEN  ? and ?";

	public static final String DEF_SQL_SELECT_MATCHED_MISSING_DOCS_APP_STATUS      = "SELECT MIS_ID FROM DDP_EXPORT_MISSING_DOCS miss,DDP_EXPORT_RULE export WHERE   miss.MIS_APP_NAME = ? AND (miss.MIS_CREATED_DATE BETWEEN ? and ?) AND miss.MIS_STATUS = 0 and export.exp_status = 0 and  export.EXP_RULE_ID = miss.MIS_EXP_RULE_ID";
	
	/******************************************** AED Rule Checking ****************************************/
	/** public static final String DEF_SQL_SELECT_MATCHED_RULE_DETAIL_ID               = "SELECT ruledetail.RDT_ID FROM DDP_RULE_DETAIL ruledetail,DDP_GEN_SOURCE_SETUP gensetup WHERE (gensetup.GSS_OPTION = CASE WHEN 'Control' = ? THEN  'Control' ELSE  '3rd Party' End or gensetup.GSS_OPTION = 'Any') AND gensetup.GSS_RDT_ID  = ruledetail.RDT_ID AND ruledetail.RDT_DOC_TYPE = ? AND (ruledetail.RDT_BRANCH = 'All' OR ruledetail.RDT_BRANCH = ?) AND ruledetail.RDT_COMPANY = ? AND ruledetail.RDT_ACTIVATION_DATE <= ? and ruledetail.RDT_STATUS=0 and ruledetail.RDT_RULE_TYPE = 'AED_RULE'"; ***/
	public static final String DEF_SQL_SELECT_MATCHED_RULE_DETAIL_ID_1               = "SELECT ruledetail.RDT_ID FROM DDP_RULE_DETAIL ruledetail,DDP_GEN_SOURCE_SETUP gensetup WHERE (gensetup.GSS_OPTION = CASE WHEN 'Control' = ? THEN  'Control' ELSE  '3rd Party' End or gensetup.GSS_OPTION = 'Any') AND gensetup.GSS_RDT_ID  = ruledetail.RDT_ID AND ruledetail.RDT_DOC_TYPE = ? AND (ruledetail.RDT_BRANCH = 'All' OR ruledetail.RDT_BRANCH = ?) AND (ruledetail.RDT_COMPANY = ? OR ruledetail.RDT_COMPANY = 'GIL') AND ruledetail.RDT_ACTIVATION_DATE <= ? and ruledetail.RDT_STATUS=0 and ruledetail.RDT_RULE_TYPE = 'AED_RULE'";
	public static final String DEF_SQL_SELECT_MATCHED_RULE_DETAIL_ID	    	   = "SELECT ruleDetail.RDT_ID, ruleDetail.RDT_RULE_ID, ruleDetail.RDT_COMPANY, ruleDetail.RDT_BRANCH, ruleDetail.RDT_DOC_TYPE, ruleDetail.RDT_PARTY_CODE, ruleDetail.RDT_PARTY_ID, ruleDetail.RDT_DEPARTMENT FROM ddp_rule_detail ruleDetail WHERE ruledetail.RDT_ACTIVATION_DATE <= ? AND RDT_STATUS = 0 AND RDT_RULE_TYPE = 'AED_RULE'";
	
	//public static final String DEF_SQL_SELECT_MATCHED_MULTI_RULE_DETAIL_ID         = "SELECT ruledetail.RDT_ID FROM DDP_RULE_DETAIL ruledetail,DDP_GEN_SOURCE_SETUP gensetup WHERE (gensetup.GSS_OPTION = CASE WHEN 'Control' = ? THEN  'Control' ELSE  '3rd Party' End or gensetup.GSS_OPTION = 'Any') AND gensetup.GSS_RDT_ID  = ruledetail.RDT_ID AND ruledetail.RDT_DOC_TYPE = ? AND (ruledetail.RDT_BRANCH = 'All' OR ruledetail.RDT_BRANCH = ?) AND ruledetail.RDT_COMPANY = ? AND ruledetail.RDT_ACTIVATION_DATE <= ? and ruledetail.RDT_STATUS=0 and ruledetail.RDT_RELAVANT_TYPE = 1 and ruledetail.RDT_RULE_TYPE = 'MULTI_AED_RULE'";
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_RULE_DETAIL_ID         = "SELECT ruledetail.RDT_ID, ruledetail.RDT_RULE_ID, ruledetail.RDT_COMPANY, ruledetail.RDT_BRANCH, ruledetail.RDT_DOC_TYPE, ruledetail.RDT_PARTY_CODE, ruledetail.RDT_PARTY_ID, ruledetail.RDT_DEPARTMENT FROM DDP_RULE_DETAIL ruledetail WHERE ruledetail.RDT_ACTIVATION_DATE <= ? and ruledetail.RDT_STATUS=0 and ruledetail.RDT_RELAVANT_TYPE = 1 and ruledetail.RDT_RULE_TYPE = 'MULTI_AED_RULE'";
	//public static final String DEF_SQL_SELECT_MATCHED_EXPORT_RULE_ID	           = "SELECT ruledetail.RDT_ID FROM DDP_RULE_DETAIL ruledetail,DDP_GEN_SOURCE_SETUP gensetup WHERE (gensetup.GSS_OPTION = CASE WHEN 'Control' = ? THEN  'Control' ELSE  '3rd Party' End or gensetup.GSS_OPTION = 'Any') AND gensetup.GSS_RDT_ID  = ruledetail.RDT_ID AND ruledetail.RDT_DOC_TYPE = ? AND (ruledetail.RDT_BRANCH = 'All' OR ruledetail.RDT_BRANCH = ?) AND ruledetail.RDT_COMPANY = ? AND ruledetail.RDT_ACTIVATION_DATE <= ? and ruledetail.RDT_STATUS=0 and ruledetail.RDT_RULE_TYPE = 'EXPORT_RULE'";
	public static final String DEF_SQL_SELECT_MATCHED_EXPORT_RULE_ID	           = "SELECT ruledetail.RDT_ID, ruledetail.RDT_RULE_ID, ruledetail.RDT_COMPANY, ruledetail.RDT_BRANCH, ruledetail.RDT_DOC_TYPE, ruledetail.RDT_PARTY_CODE, ruledetail.RDT_PARTY_ID, ruledetail.RDT_DEPARTMENT FROM DDP_RULE_DETAIL ruledetail WHERE ruledetail.RDT_ACTIVATION_DATE <= ? and ruledetail.RDT_STATUS=0 and ruledetail.RDT_RULE_TYPE = 'EXPORT_RULE'";
	public static final String DEF_SQL_SELECT_MATCHED_EMAIL_TIGGER_ID              = "SELECT ETR_ID, ETR_TRIGGER_NAME,ETR_DOC_TYPES, ETR_DOC_SELECTION,ETR_RETRIES,ETR_INCLUDE FROM DDP_EMAIL_TRIGGER_SETUP WHERE ETR_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_AED_RULE_DETAIL_ID     = "SELECT RDT_ID FROM DDP_RULE_DETAIL WHERE RDT_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_AED_RULE_WITH_DATE_DETAIL_ID     = "SELECT RDT_ID FROM DDP_RULE_DETAIL WHERE RDT_RULE_ID = ? AND RDT_ACTIVATION_DATE <= ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS           = "SELECT CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_RUL_ID = ? AND CAT_RDT_ID = ? AND CAT_STATUS = 0  ORDER BY CAT_ID desc";
	
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS_REPROCESS = "SELECT CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_RUL_ID = ? AND CAT_RDT_ID = ? AND (CAT_STATUS = -1 OR CAT_STATUS = 3)  ORDER BY CAT_ID desc";
	
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS_WITH_DATES = "SELECT CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_RUL_ID = ? AND CAT_RDT_ID = ? AND CAT_STATUS = 0 AND CAT_CREATED_DATE BETWEEN  ? and ?  ORDER BY CAT_ID desc";
	
	public static final String DEF_SQL_SELECT_MATCHED_MULTI_AED_CAT_DOCS_ID        = "SELECT CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_RUL_ID = ? AND CAT_RDT_ID = ? AND CAT_STATUS = 0 AND  CAT_ID < ?";
	
	public static final String DEF_SQL_SELECT_MATCHED_TIGGER_POINT_SCHEDULAR_ID    = "SELECT tiggersetup.ETR_ID,tiggersetup.ETR_RULE_ID,tiggersetup.ETR_CRON_EXPRESSION,tiggersetup.ETR_TRIGGER_NAME, tiggersetup.ETR_DOC_TYPES,tiggersetup.ETR_DOC_SELECTION from DDP_EMAIL_TRIGGER_SETUP tiggersetup, DDP_RULE ddpRule WHERE tiggersetup.ETR_TRIGGER_NAME = ? AND tiggersetup.ETR_RULE_ID = ddpRule.RUL_ID AND ddpRule.RUL_STATUS = 0";
	
	public static final String DEF_SQL_SELECT_INITITATE_PROCESS_FETCH              = "SELECT CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_STATUS IN (100,-1,0) AND CAT_CREATED_DATE >= ?";
	
	public static final String DEF_SQL_SELECT_UPDATE_METADATA_FETCH				   = "SELECT CHL_ID FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_DTX_ID NOT IN (SELECT DDD_DTX_ID FROM DDP_DMS_DOCS_DETAIL)";
	
	public static final String DEF_SQL_SELECT_MATCHED_GENERATING_SOURCE            = "SELECT GSS_OPTION FROM DDP_GEN_SOURCE_SETUP WHERE GSS_RDT_ID = ?";
	/******************************************** Scheduler Queries *****************************************/
	public static final String DEF_SQL_SELECT_JOB_NUMBEER_MULTI_AED               = "SELECT cat.CAT_ID FROM DDP_CATEGORIZED_DOCS cat,DDP_DMS_DOCS_DETAIL ddd  WHERE   cat.CAT_DTX_ID = ddd.DDD_DTX_ID and ddd.DDD_JOB_NUMBER = ?  and ddd.DDD_CONTROL_DOC_TYPE != ? and cat.CAT_RUL_ID = ? and cat.CAT_RULE_TYPE= 'MULTI_AED_RULE' ORDER BY cat.CAT_ID DESC";
	
	public static final String DEF_SQL_SELECT_REPROCESS_MULTI_AED                 = "SELECT ETR_ID FROM DDP_EMAIL_TRIGGER_SETUP WHERE ETR_RULE_ID IN (select distinct cat.CAT_RUL_ID from DDP_RULE_DETAIL detail,DDP_MULTI_AED_RULE multi,DDP_CATEGORIZED_DOCS cat where   cat.CAT_STATUS = 0 and cat.CAT_RDT_ID = detail.RDT_ID and cat.CAT_RUL_ID = multi.MAED_RULE_ID and detail.RDT_RELAVANT_TYPE = 1  and detail.RDT_RULE_ID  = multi.MAED_RULE_ID  and multi.MAED_STATUS = 0)";
	/*********************************************   Trigger names    ****************************************/
	public static final String TRIGGER_NAME_IMMEDIATE                              = "Immediately";
	public static final String TRIGGER_NAME_SPECIFIC_DOCS                          = "Specific Docs";
	public static final String TRIGGER_NAME_ALL_OR_MINIMUM                         = "All/Minimum";
	public static final String TRIGGER_NAME_SPECIFIC_TIME                          = "Specific Time";
	
	public static final String ZIP_COMPRESSION                                     = "zip";
	public static final String NO_COMPRESSION                                      = "nonzip";
	public static final String MAED_MERGE                                          = "merge";
	
	/******************************** Queries for SLA Schedulers ***********************************************/
	public static final String DDP_SQL_SELECT_AED_DAILY_SCH 					   = "SELECT RDT_ID FROM DDP_RULE_DETAIL WHERE RDT_SLA_FREQ = 'day' AND RDT_STATUS = 0";
	public static final String DDP_SQL_SELECT_AED_WEEKLY_SCH 					   = "SELECT RDT_ID FROM DDP_RULE_DETAIL WHERE RDT_SLA_FREQ = 'week' AND RDT_STATUS = 0";
	public static final String DDP_SQL_SELECT_AED_MONTHLY_SCH 					   = "SELECT RDT_ID FROM DDP_RULE_DETAIL WHERE RDT_SLA_FREQ = 'month' AND RDT_STATUS = 0";
	
	public static final String DDP_SQL_SELECT_MATCHED_CATEGORIZED_DOCS_FOR_SLA     = "SELECT CAT_ID FROM DDP_CATEGORIZED_DOCS WHERE CAT_RDT_ID in (?) AND CAT_CREATED_DATE >= ? AND CAT_RULE_TYPE = 'AED_RULE'";
			
	/********************************** Export report generation ***********************************************/
	public static final String DDP_SQL_SELECT_EXPORT_REPORTS_BASED_DATE_RANGE		= "SELECT ESR_ID FROM DDP_EXPORT_SUCCESS_REPORT WHERE ESR_RULE_ID = ? AND ESR_TYPE_OF_SERVICE in ('1','3') AND ESR_CREATION_DATE BETWEEN ? and ?";
	public static final String DDP_SQL_SELECT_EXPORT_REPORTS_BASED_DATE_RANGE_FAILED= "SELECT ESR_ID FROM DDP_EXPORT_SUCCESS_REPORT WHERE ESR_RULE_ID = ? AND ESR_TYPE_OF_SERVICE in ('-1','0') AND ESR_CREATION_DATE BETWEEN ? and ?";
	
	public static final String DDP_SQL_SELECT_EXPORT_REPORTS_CATEGORIZED_DOCS		= "SELECT CAT_ID from ddp_categorized_docs WHERE CAT_RUL_ID = ? AND CAT_STATUS in ('-1','0') AND CAT_RULE_TYPE = 'EXPORT_RULE' AND CAT_CREATED_DATE BETWEEN ? and ?";
	public static final String DDP_SQL_SELECT_MIS_EXPORT_MISSING_DOCS 				= "SELECT MIS_ID from DDP_EXPORT_MISSING_DOCS WhERE MIS_EXP_RULE_ID = ? AND MIS_STATUS in ('-1','0') AND MIS_CREATED_DATE BETWEEN ? and ?";
	
	/*********** Query for Export Query UIs by Export Rule ID ***************************/
	public static final String DEF_SQL_SELECT_EXPORT_QUERY_UI_BY_RUL_ID = "SELECT EQI_ID FROM DDP_EXPORT_QUERY_UI WHERE EQI_STATUS=0 AND EQI_EXP_RULE_ID = ?";
	
	/*********** Query for Export Query  by Export Rule ID ***************************/
	public static final String DEF_SQL_SELECT_EXPORT_QUERY_BY_RUL_ID = "SELECT EXQ_ID FROM DDP_EXPORT_QUERY WHERE EXQ_STATUS=0 AND EXQ_EXP_RULE_ID = ?";
	
	public static final String DEF_SQL_SELECT_RATE_SETUP = "SELECT RTS_ID, RTS_RDT_ID, RTS_OPTION FROM DDP_RATE_SETUP WHERE RTS_RDT_ID = ?";
}

