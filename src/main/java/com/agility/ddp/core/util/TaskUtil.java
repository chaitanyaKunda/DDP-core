package com.agility.ddp.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.MimeHeaders;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPFault;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.data.domain.DdpBranch;
import com.agility.ddp.data.domain.DdpBranchService;
import com.agility.ddp.data.domain.DdpCategorizationHolder;
import com.agility.ddp.data.domain.DdpCategorizationHolderService;
import com.agility.ddp.data.domain.DdpCategorizedDetail;
import com.agility.ddp.data.domain.DdpCategorizedDetailService;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCompany;
import com.agility.ddp.data.domain.DdpCompanyService;
import com.agility.ddp.data.domain.DdpDmsDocsDetail;
import com.agility.ddp.data.domain.DdpDmsDocsDetailService;
import com.agility.ddp.data.domain.DdpDmsDocsHolder;
import com.agility.ddp.data.domain.DdpDmsDocsHolderService;
import com.agility.ddp.data.domain.DdpDmsDocsTxn;
import com.agility.ddp.data.domain.DdpDmsDocsTxnRepository;
import com.agility.ddp.data.domain.DdpDmsDocsTxnService;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.agility.ddp.data.domain.DdpJobRefHolderPK;
import com.agility.ddp.data.domain.DdpJobRefHolderService;
import com.agility.ddp.data.domain.DdpMultiEmails;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpRuleService;
import com.agility.ddp.data.domain.DmsDdpSyn;
import com.agility.ddp.data.domain.DmsDdpSynService;
import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.common.IDfLoginInfo;


//@PropertySource({"file:///E:/DDPConfig/ddp.properties","file:///E:/DDPConfig/mail.properties"})
public class TaskUtil 
{
	private static final Logger logger = LoggerFactory.getLogger(TaskUtil.class);
	
		
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
    private DdpJobRefHolderService ddpJobRefHolderService;
	
	@Autowired
    private DdpDmsDocsHolderService ddpDmsDocsHolderService;
	
	@Autowired
	private DdpDmsDocsTxnService ddpDmsDocsTxnService;
	
	@Autowired
    private DdpDmsDocsTxnRepository ddpDmsDocsTxnRepository;
	
	@Autowired
	private DdpRuleService ddpRuleService;
	
	@Autowired
	private DdpRuleDetailService ddpRuleDetailService;
	
	@Autowired
	private DmsDdpSynService dmsDdpSynService;
	
	@Autowired
	private DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	private DdpDmsDocsDetailService ddpDmsDocsDetailService;
	
	@Autowired
	private DdpCategorizedDetailService ddpCategorizedDetailService;
	
	@Autowired
	private DdpCategorizationHolderService ddpCategorizationHolderService;
	
	@Autowired
	private DdpCompanyService ddpCompanyService;
	
	@Autowired
	private DdpBranchService ddpBranchService;
	
	Date now = new Date();
	
	Calendar catCreatedDate = Calendar.getInstance();
	
	public DdpJobRefHolder getDdpJobRefHolder(String jobName) throws Exception
	{
		logger.debug("TaskUtil.getDdpJobRefHolder(String jobName) method invoked.");
		DdpJobRefHolder jobHolder = null;
		
		try
		{
			
			List<DdpJobRefHolder> jobHolderlist  = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_JOB_REF_HOLDER_ID, new Object[] { jobName }, new RowMapper<DdpJobRefHolder>() {
		
				public DdpJobRefHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					String strJrfId = rs.getString("JRF_ID");
					String strJrfJobId = rs.getString("JRF_JOB_ID");
					Integer intJrfStatus = rs.getInt("JRF_STATUS");
					Calendar calJrfCreateDate = Calendar.getInstance();
					calJrfCreateDate.setTime(rs.getTimestamp("JRF_CREATED_DATE"));
					Calendar calJrfModifiedDate = Calendar.getInstance();
					calJrfModifiedDate.setTime(rs.getTimestamp("JRF_MODIFIED_DATE"));
					
					DdpJobRefHolderPK ddpJobRefHolderKey = new DdpJobRefHolderPK(strJrfId, strJrfJobId, intJrfStatus, calJrfCreateDate, calJrfModifiedDate);
					DdpJobRefHolder ddpJobRefHolder = new DdpJobRefHolder();
					ddpJobRefHolder.setId(ddpJobRefHolderKey);
	
					return ddpJobRefHolder;
		           }
			});
	
	
			if(jobHolderlist.isEmpty())
			{
				logger.info("TaskUtil.getDdpJobRefHolder(String jobName) - No record found for Job [{}].",jobName);
			}
			else if(jobHolderlist.size() == 1)
			{
				jobHolder = jobHolderlist.get(0);
				logger.debug("TaskUtil.getDdpJobRefHolder(String jobName) - One record found for Job [{}] => [jobHolder.JrfId : {}, jobHolder.JrfJobId : {} ].",jobName,jobHolder.getId().getJrfId(),jobHolder.getId().getJrfJobId());
			}
			else
			{
				//If more than one record exist, fetch the last record
				jobHolder = jobHolderlist.get(jobHolderlist.size()-1);
				//Notification should be set to remove un-wanted records 
				logger.info("TaskUtil.getDdpJobRefHolder(String jobName) - More than one record found for Job [{}].",jobName);
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDdpJobRefHolder(String jobName) - Exception while accessing DdpJobRefHolder for Job [{}] : "+jobName,ex);
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getDdpJobRefHolder(String jobName) executed successfully.");
		return jobHolder;
	}
	
	
	public DdpDmsDocsHolder getMaxSynIdDdpDmsDocsHolder() throws Exception
	{
		
		logger.debug("TaskUtil.getMaxSynIdDdpDmsDocsHolder() method invoked.");
		DdpDmsDocsHolder ddpDmsDocsHolder = null;
		
		try
		{			
			Integer intThlSynId  = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_DMS_DOCS_HOLDER_MAX_SYN_ID, Integer.class);
			
			if (intThlSynId != null)
				ddpDmsDocsHolder = ddpDmsDocsHolderService.findDdpDmsDocsHolder(intThlSynId);
		    			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getMaxSynIdDdpDmsDocsHolder() - Exception while accessing Max(SYN_ID) for DdpDmsDocsHolder.",ex);
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getMaxSynIdDdpDmsDocsHolder() executed successfully.");
		return ddpDmsDocsHolder;
	}
	//for metadata 
	public DdpCategorizationHolder getMaxChlIdDdpCategorizationHolder()
	{
		
		logger.debug("TaskUtil. getMaxChlIdDdpCategorizationHolder() method invoked.");
		DdpCategorizationHolder ddpCategorizationHolder = null;
		
		try
		{			
			Integer intChlId  = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_CATEGORIZATION_HOLDER_MAX_CHL_ID, Integer.class);
			ddpCategorizationHolder = ddpCategorizationHolderService.findDdpCategorizationHolder(intChlId);
					    			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil. getMaxChlIdDdpCategorizationHolder() - Exception while accessing Max(CHL_ID) for DdpCategorizationHolder.");
			ex.printStackTrace();
		}
		
		logger.debug("TaskUtil.getMaxChlIdDdpCategorizationHolder() executed successfully.");
		return ddpCategorizationHolder;
	}
	//for init job

	public DdpCategorizedDocs getMaxCatIdDdpCategorizedDocs()
	{
		
		logger.debug("TaskUtil.getMaxCatIdDdpCategorizedDocs() method invoked.");
		DdpCategorizedDocs ddpCategorizedDocs = null;
		
		try
		{			
			Integer intCatId  = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_CATEGORIZED_DOCS_MAX_CAT_ID, Integer.class);
			
			ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(intCatId);
		    			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getMaxCatIdDdpCategorizedDocs() - Exception while accessing Max(Cat_Id) for DdpCategorizedDocs.");
		}
		
		logger.debug("TaskUtil.getMaxCatIdDdpCategorizedDocs() executed successfully.");
		return ddpCategorizedDocs;
	}
	
	
	
	public DdpCategorizationHolder getMaxChlIdDdpCategarizationHolder() throws Exception
	{
		
		logger.debug("TaskUtil.getMaxChlIdDdpCategarizationHolder() method invoked.");
		DdpCategorizationHolder ddpCategorizationHolder = null;
		
		try
		{			
			Integer intChlId  = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_CATEGORIZATION_HOLDER_MAX_CHL_ID, Integer.class);
			
			ddpCategorizationHolder = ddpCategorizationHolderService.findDdpCategorizationHolder(intChlId);
		    			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getMaxChlIdDdpCategarizationHolder() - Exception while accessing Max(CHL_ID) for DdpCategarizationHolder.");
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getMaxChlIdDdpCategarizationHolder() executed successfully.");
		return ddpCategorizationHolder;
	}
	
	
	
	public List<DdpCategorizationHolder> getDdpCategorizationHoldersGreaterThanMaxCHLID(DdpCategorizationHolder ddpCategorizationHolder) throws Exception
	{
		logger.debug("TaskUtil.getDdpCategorizationHoldersGreaterThanMaxCHLID(DdpCategorizationHolder ddpCategorizationHolder) method invoked.");
		List<DdpCategorizationHolder> ddpCategorizationHolders = null;
		
		try
		{
			ddpCategorizationHolders = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_CATEGORIZATION_THAN_MAX_CHL_ID, new Object[] { ddpCategorizationHolder.getChlId() }, new RowMapper<DdpCategorizationHolder>() {
							
				public DdpCategorizationHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpCategorizationHolder categorizationHolder = new DdpCategorizationHolder();
					
					categorizationHolder.setChlDtxId(rs.getInt("CHL_DTX_ID"));
					categorizationHolder.setChlId(rs.getInt("CHL_ID"));
					categorizationHolder.setChlRulId(rs.getInt("CHL_RUL_ID"));
					categorizationHolder.setChlSynId(rs.getInt("CHL_SYN_ID"));
					
					
					return categorizationHolder;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.ggetDdpCategorizationHoldersGreaterThanMaxCHLID(DdpCategorizationHolder ddpCategorizationHolder) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxSynID [{}] records.ddpCategorizationHolder",ddpCategorizationHolder.getChlId());
			ex.printStackTrace();
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getDdpCategorizationHoldersGreaterThanMaxCHLID(DdpCategorizationHolder ddpCategorizationHolder) executed successfully.");
		return ddpCategorizationHolders;
	}
	
	public List<DdpCategorizedDocs> getDdpCategorizedDocsGreaterThanMaxCatId(DdpCategorizedDocs ddpCategorizedDoc)
	{
		logger.debug("TaskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(DdpDmsDocsHolder ddpDmsDocsHolder) method invoked.");
		List<DdpCategorizedDocs> ddpCategorizationdocsList = null;
		
		try
		{
			ddpCategorizationdocsList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_CATEGORIZED_DOCS_GREATER_THAN_MAX_CAT_ID, new Object[] { ddpCategorizedDoc.getCatId() }, new RowMapper<DdpCategorizedDocs>() {
							
				public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
					
					return ddpCategorizedDocs;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(DdpCategorizedDocs ddpCategorizedDoc) - Exception while accessing DdpCategorizedDocs retrive GreaterThanMaxSynID [{}] records.ddpCategorizedDoc", ddpCategorizedDoc.getCatId());
			System.out.println(ex);
			//ex.printStackTrace();
		}
		
		logger.debug("TaskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(DdpCategorizedDocs ddpCategorizedDoc) executed successfully.");
		return ddpCategorizationdocsList;
	}
	public List<DdpCategorizedDocs> getCategorizedDocsInRange(int minCatID, int maxCatID)
	{
		List<DdpCategorizedDocs> ddpCategorizationdocsList = null;
		ddpCategorizationdocsList = this.jdbcTemplate.query("SELECT * FROM DDP_CATEGORIZED_DOCS WHERE CAT_ID BETWEEN ? AND ?", new Object[] { minCatID, maxCatID }, new RowMapper<DdpCategorizedDocs>() {
			
			public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
				
				return ddpCategorizedDocs;
	           }
		});
		return ddpCategorizationdocsList;
	}
	
	
	
	public List<DmsDdpSyn> getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) throws Exception
	{
		logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) method invoked.");
		List<DmsDdpSyn> dmsDdpSynList = null;
		
		try
		{
			dmsDdpSynList = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_DMS_DDP_SYN_GREATER_THAN_MAX_SYN_ID, new Object[] { ddpDmsDocsHolder.getThlSynId() }, new RowMapper<DmsDdpSyn>() {
							
				public DmsDdpSyn mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DmsDdpSyn dmsDdpSyn = new DmsDdpSyn();
					
					dmsDdpSyn.setSynId(rs.getInt("SYN_ID"));
					dmsDdpSyn.setSynRObjectId(rs.getString("SYN_R_OBJECT_ID"));
					dmsDdpSyn.setSynGenSystem(rs.getString("SYN_GEN_SYSTEM"));
					dmsDdpSyn.setSynCompanySource(rs.getString("SYN_COMPANY_SOURCE"));
					dmsDdpSyn.setSynBranchSource(rs.getString("SYN_BRANCH_SOURCE")); 
					dmsDdpSyn.setSynDeptSource(rs.getString("SYN_DEPT_SOURCE")); 
					dmsDdpSyn.setSynDocType(rs.getString("SYN_DOC_TYPE")); 
					dmsDdpSyn.setSynClientId(rs.getString("SYN_CLIENT_ID")) ;
					dmsDdpSyn.setSynShipper(rs.getString("SYN_SHIPPER")); 
					dmsDdpSyn.setSynConsignee(rs.getString("SYN_CONSIGNEE")); 
					dmsDdpSyn.setSynNotifyParty(rs.getString("SYN_NOTIFY_PARTY")); 
					dmsDdpSyn.setSynDebitsForward(rs.getString("SYN_DEBITS_FORWARD")); 
					dmsDdpSyn.setSynDebitsBack(rs.getString("SYN_DEBITS_BACK")); 
					dmsDdpSyn.setSynInitialAgent(rs.getString("SYN_INITIAL_AGENT")); 
					dmsDdpSyn.setSynIntermediateAgent(rs.getString("SYN_INTERMEDIATE_AGENT"));
					dmsDdpSyn.setSynFinalAgentId(rs.getString("SYN_FINAL_AGENT_ID"));
					dmsDdpSyn.setSynIsRated(rs.getInt("SYN_IS_RATED"));
					
					if(rs.getTimestamp("SYN_CREATED_DATE") == null){ dmsDdpSyn.setSynCreatedDate(null);}
					else{
						Calendar calDdsCreateDate = Calendar.getInstance();
						calDdsCreateDate.setTime(rs.getTimestamp("SYN_CREATED_DATE"));
						dmsDdpSyn.setSynCreatedDate(calDdsCreateDate);
					}
					dmsDdpSyn.setSynTboName(rs.getString("SYN_TBO_NAME"));
					
					return dmsDdpSyn;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxSynID [{}] records.",ddpDmsDocsHolder.getThlSynId());
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) executed successfully.");
		return dmsDdpSynList;
	}
	
	public List<DmsDdpSyn> getDmsDdpSynBeweenIDs(int minSynID,int maxSynID) throws Exception
	{
		List<DmsDdpSyn> dmsDdpSynList = null;
		try
		{
			dmsDdpSynList = this.jdbcTemplate.query("SELECT * FROM DMS_DDP_SYN WHERE SYN_ID BETWEEN ? AND ?", new Object[] { minSynID,maxSynID }, new RowMapper<DmsDdpSyn>() {
							
				public DmsDdpSyn mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DmsDdpSyn dmsDdpSyn = new DmsDdpSyn();
					
					dmsDdpSyn.setSynId(rs.getInt("SYN_ID"));
					dmsDdpSyn.setSynRObjectId(rs.getString("SYN_R_OBJECT_ID"));
					dmsDdpSyn.setSynGenSystem(rs.getString("SYN_GEN_SYSTEM"));
					dmsDdpSyn.setSynCompanySource(rs.getString("SYN_COMPANY_SOURCE"));
					dmsDdpSyn.setSynBranchSource(rs.getString("SYN_BRANCH_SOURCE")); 
					dmsDdpSyn.setSynDeptSource(rs.getString("SYN_DEPT_SOURCE")); 
					dmsDdpSyn.setSynDocType(rs.getString("SYN_DOC_TYPE")); 
					dmsDdpSyn.setSynClientId(rs.getString("SYN_CLIENT_ID")) ;
					dmsDdpSyn.setSynShipper(rs.getString("SYN_SHIPPER")); 
					dmsDdpSyn.setSynConsignee(rs.getString("SYN_CONSIGNEE")); 
					dmsDdpSyn.setSynNotifyParty(rs.getString("SYN_NOTIFY_PARTY")); 
					dmsDdpSyn.setSynDebitsForward(rs.getString("SYN_DEBITS_FORWARD")); 
					dmsDdpSyn.setSynDebitsBack(rs.getString("SYN_DEBITS_BACK")); 
					dmsDdpSyn.setSynInitialAgent(rs.getString("SYN_INITIAL_AGENT")); 
					dmsDdpSyn.setSynIntermediateAgent(rs.getString("SYN_INTERMEDIATE_AGENT"));
					dmsDdpSyn.setSynFinalAgentId(rs.getString("SYN_FINAL_AGENT_ID"));
					dmsDdpSyn.setSynIsRated(rs.getInt("SYN_IS_RATED"));
					
					if(rs.getTimestamp("SYN_CREATED_DATE") == null){ dmsDdpSyn.setSynCreatedDate(null);}
					else{
						Calendar calDdsCreateDate = Calendar.getInstance();
						calDdsCreateDate.setTime(rs.getTimestamp("SYN_CREATED_DATE"));
						dmsDdpSyn.setSynCreatedDate(calDdsCreateDate);
					}
					dmsDdpSyn.setSynTboName(rs.getString("SYN_TBO_NAME"));
					
					return dmsDdpSyn;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		logger.info("TaskUtil.getDmsDdpSynBeweenIDs() : reprocess start for syn IDs between "+minSynID+" and "+maxSynID+" and Total Syn Records to be processed : "+dmsDdpSynList.size());
		return dmsDdpSynList;
	}
	public List<DmsDdpSyn> getDmsDdpSynsGreaterThanMaxSynID(DdpJobRefHolder jobHolder) throws Exception
	{
		logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpJobRefHolder jobHolder) method invoked.");
		List<DmsDdpSyn> dmsDdpSynList = null;
		String strJrfId = null;
		try
		{
			strJrfId = jobHolder.getId().getJrfId();
			Integer intJrfStatus = jobHolder.getId().getJrfStatus(); //'-1' - Failed, '0' - In Progress, '1' - Success
			//Integer intQueryId = Integer.getInteger(strJrfId);
			Integer intQueryId = Integer.parseInt(strJrfId);
			//If current Job Ref Id is failed or still In Progress, fetch DmsDdpSyn including this record (Assumed that last update was failed). 
			if( intJrfStatus < 1 )
			{
				//intQueryId--; This logic may not return the correct id. 
				//Changed to use the SQL to fetch previous max syn_id				
				Integer queryId = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_DMS_DOCS_HOLDER_PREV_MAX_SYN_ID, new Object[] { intQueryId }, Integer.class);
				if(queryId != null){ intQueryId =  queryId;}		
			}
			//this needs to be split to ensure "the intQueryId--" returns record when calling ddpDmsDocsHolderService.findDdpDmsDocsHolder(intQueryId)
			dmsDdpSynList = getDmsDdpSynsGreaterThanMaxSynID(ddpDmsDocsHolderService.findDdpDmsDocsHolder(intQueryId));
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpJobRefHolder jobHolder) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxSynID [{}] records."+strJrfId,ex);
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		logger.debug("DdpJobRefHolder.getDmsDdpSynsGreaterThanMaxSynID(DdpJobRefHolder jobHolder) executed successfully.");
		return dmsDdpSynList;
	}
	
	
	/**
	 * 
	 * @param jobHolder
	 * @param jobName
	 * @return
	 */
	public List<DdpCategorizationHolder> getDmsDdpCatGreaterThanMaxJobRefID(DdpJobRefHolder jobHolder, String jobName) throws Exception
	{
		logger.debug("TaskUtil.getDmsDdpCatGreaterThanMaxJobRefID(DdpJobRefHolder jobHolder, String jobName) method invoked.");
		List<DdpCategorizationHolder> ddpCategorizationHolderList = null;
		String strJrfId = null;
		try
		{
			for(DdpJobRefHolder ddpJobRefHolder : ddpJobRefHolderService.findAllDdpJobRefHolders() ){
				if(ddpJobRefHolder.getId().getJrfJobId().equalsIgnoreCase(jobName)){
					strJrfId = ddpJobRefHolder.getId().getJrfId();
					Integer intJrfStatus = ddpJobRefHolder.getId().getJrfStatus(); //'-1' - Failed, '0' - In Progress, '1' - Success
					//Integer intQueryId = Integer.getInteger(strJrfId);
					Integer intQueryId = Integer.parseInt(strJrfId);
					//If current Job Ref Id is failed or still In Progress, fetch DmsDdpSyn including this record (Assumed that last update was failed). 
					if( intJrfStatus < 1 )
					{
						//intQueryId--; This logic may not return the correct id. 
						//Changed to use the SQL to fetch previous max syn_id				
						Integer queryId = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_CATEGORIZATION_HOLDER_PREV_MAX_CHL_ID, new Object[] { intQueryId }, Integer.class);
						if(queryId != null){ intQueryId =  queryId;}		
					}
					//this needs to be split to ensure "the intQueryId--" returns record when calling ddpDmsDocsHolderService.findDdpDmsDocsHolder(intQueryId)
					ddpCategorizationHolderList = getDdpCategorizationHoldersGreaterThanMaxCHLID(ddpCategorizationHolderService.findDdpCategorizationHolder(intQueryId));
				}
				continue;
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDmsDdpCatGreaterThanMaxJobRefID(DdpJobRefHolder jobHolder, String jobName) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxChlID [{}] records.",strJrfId);
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		logger.debug("DdpJobRefHolder.getDmsDdpCatGreaterThanMaxJobRefID(DdpJobRefHolder jobHolder, String jobName) executed successfully.");
		return ddpCategorizationHolderList;
	}
	
	public List<DdpCategorizationHolder> getCategorizationHoldersInRange(int minChlID, int maxChlID)
	{
		List<DdpCategorizationHolder> categorizationHolders = null;
		categorizationHolders = this.jdbcTemplate.query("SELECT * FROM ddp_categorization_holder WHERE chl_id BETWEEN ? AND ?", new Object[]{ minChlID, maxChlID}, new RowMapper<DdpCategorizationHolder>(){
			@Override
			public DdpCategorizationHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
				DdpCategorizationHolder categorizationHolder = new DdpCategorizationHolder();
				categorizationHolder.setChlId(rs.getInt("CHL_ID"));
				categorizationHolder.setChlSynId(rs.getInt("CHL_SYN_ID"));
				categorizationHolder.setChlDtxId(rs.getInt("CHL_DTX_ID"));
				categorizationHolder.setChlRulId(rs.getInt("CHL_RUL_ID"));
				return categorizationHolder;
			}
		});
		return categorizationHolders;
	}
 
	/**
	 * 
	 * @param jobHolder
	 * @param jobName
	 * @return
	 */
	public List<DdpCategorizedDocs> getDdpCategorizedDocsGreaterThanMaxCatId(DdpJobRefHolder jobHolder, String jobName)
	{
		logger.debug("TaskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(DdpJobRefHolder jobHolder) method invoked.");
		List<DdpCategorizedDocs> ddpCategorizationDocsList = null;
		String strJrfId = null;
		try
		{
			for(DdpJobRefHolder ddpJobRefHolder : ddpJobRefHolderService.findAllDdpJobRefHolders() ){
				if(ddpJobRefHolder.getId().getJrfJobId().equalsIgnoreCase(jobName)){
					strJrfId = jobHolder.getId().getJrfId();
					Integer intJrfStatus = jobHolder.getId().getJrfStatus(); //'-1' - Failed, '0' - In Progress, '1' - Success
					//Integer intQueryId = Integer.getInteger(strJrfId);
					Integer intQueryId = Integer.parseInt(strJrfId);
					//If current Job Ref Id is failed or still In Progress, fetch DmsDdpSyn including this record (Assumed that last update was failed). 
					if( intJrfStatus < 1 )
					{
						//intQueryId--; This logic may not return the correct id. 
						//Changed to use the SQL to fetch previous max syn_id				
						Integer queryId = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_DDP_CATEGORIZED_DOCS_PREV_MAX_CAT_ID, new Object[] { intQueryId }, Integer.class);
						if(queryId != null){ intQueryId =  queryId;}		
					}
					//this needs to be split to ensure "the intQueryId--" returns record when calling ddpDmsDocsHolderService.findDdpDmsDocsHolder(intQueryId)
					ddpCategorizationDocsList = getDdpCategorizedDocsGreaterThanMaxCatId(ddpCategorizedDocsService.findDdpCategorizedDocs(intQueryId));
				}
				continue;
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(DdpJobRefHolder jobHolder, String jobName) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxCatID [{}] records.",strJrfId);
			//System.out.println(ex);
			//ex.printStackTrace();
		}
		logger.debug("DdpJobRefHolder.getDdpCategorizedDocsGreaterThanMaxCatId(DdpJobRefHolder jobHolder, String jobName) executed successfully.");
		return ddpCategorizationDocsList;
	}
	
	
	
	/**
	 * 
	 * @param strSQL
	 * @param params
	 * @return
	 */
	public List<DdpRuleDetail> getDdpRuleDetail(String strSQL, Object[] params)
	{
		//logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) method invoked.");
		List<DdpRuleDetail> ddpRuleDetailList = null;
		
		try
		{
			ddpRuleDetailList = this.jdbcTemplate.query(strSQL , params , new RowMapper<DdpRuleDetail>() {
							
				public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpRuleDetail ddpRuleDetail = new DdpRuleDetail();
					
					// TODO Stopped due to IMT
					
					return ddpRuleDetail;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			//logger.error("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxSynID [{}] records.",ddpDmsDocsHolder.getThlSynId());
			ex.printStackTrace();
		}
		
		//logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) executed successfully.");
		return ddpRuleDetailList;
	}
	
	/************************************************
	 * 
	 * This method sort dmsDdpSynList into ascending order.
	 * 
	 *  
	 * 
	 * 
	 * @param dmsDdpSynList
	 * @return dmsDdpSynList
	 *************************************************/
	public List<DmsDdpSyn> sortDmsDdpSynList(List<DmsDdpSyn> dmsDdpSynList) throws Exception
	{
		logger.debug("TaskUtil.sortDmsDdpSynList(List<DmsDdpSyn> dmsDdpSynList) method invoked.");
		List<DmsDdpSyn> sortedDmsDdpSynList = null;
		
		try
		{
			if(dmsDdpSynList != null && !dmsDdpSynList.isEmpty())
			{
				sortedDmsDdpSynList = new ArrayList<DmsDdpSyn>();
				
				Collections.sort(dmsDdpSynList, new Comparator<DmsDdpSyn>() {
						
					@Override
					public int compare(DmsDdpSyn dmsDdpSyn1, DmsDdpSyn dmsDdpSyn2)
					{
						return dmsDdpSyn1.getSynId().compareTo(dmsDdpSyn2.getSynId());
					}
					
				});
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.sortDmsDdpSynList(List<DmsDdpSyn> dmsDdpSynList) - Exception while accessing sorting dmsDdpSynList.",ex);
			//ex.printStackTrace();
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.sortDmsDdpSynList(List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
		return dmsDdpSynList;
	}
	
	/************************************************
	 * 
	 * This method sort DdpCategorizedDocs into ascending order.
	 * 
	 *  
	 * 
	 * 
	 * @param ddpCategorizedDocsList
	 * @return ddpCategorizedDocstList
	 *************************************************/
	public List<DdpCategorizedDocs> sortDdpCategorizedDocsList(List<DdpCategorizedDocs> categorizedDocsList) throws Exception
	{
		logger.debug("TaskUtil.sortDdpCategorizedDocsList(List<DdpCategorizedDocs> ddpCategorizedDocs) method invoked.");
		List<DdpCategorizedDocs> sortedDdpCategorizedDocsList = null;
		
		try
		{
			if(categorizedDocsList != null && !categorizedDocsList.isEmpty())
			{
				sortedDdpCategorizedDocsList = new ArrayList<DdpCategorizedDocs>();
				
				Collections.sort(categorizedDocsList, new Comparator<DdpCategorizedDocs>() {
						
					@Override
					public int compare(DdpCategorizedDocs categorizedDocs1, DdpCategorizedDocs categorizedDocs2)
					{
						return categorizedDocs1.getCatId().compareTo(categorizedDocs2.getCatId());
					}
					
				});
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.sortDdpCategorizedDocsList(List<DdpCategorizedDocs> ddpCategorizedDocsList) - Exception while accessing sorting ddpCategorizedDocsList.");
			//ex.printStackTrace();
			throw new Exception(ex);
			
		}
		
		logger.debug("TaskUtil.sortDdpCategorizedDocsList(List<DdpCategorizedDocs> ddpCategorizedDocsList) executed successfully.");
		return categorizedDocsList;
	}
	
	
	/************************************************
	 * 
	 * This method sort DdpCategorizationHolderList into ascending order.
	 * 
	 *  
	 * 
	 * 
	 * @param DdpCategorizationHolderList
	 * @return DdpCategorizationHolderList
	 *************************************************/
	public List<DdpCategorizationHolder> sortDdpCategorizationList(List<DdpCategorizationHolder> ddpCategorizationHolders)
	{
		logger.debug("TaskUtil.sortDdpCategorizationList(List<DdpCategorizationHolder> ddpCategorizationHolders) method invoked.");
		List<DdpCategorizationHolder> categorizationHolders = null;
		
		try
		{
			if(ddpCategorizationHolders != null && !ddpCategorizationHolders.isEmpty())
			{
				categorizationHolders = new ArrayList<DdpCategorizationHolder>();
				
				Collections.sort(ddpCategorizationHolders, new Comparator<DdpCategorizationHolder>() {
						
					@Override
					public int compare(DdpCategorizationHolder ddpCategorizationHolder1, DdpCategorizationHolder ddpCategorizationHolder2)
					{
						return ddpCategorizationHolder1.getChlId().compareTo(ddpCategorizationHolder2.getChlId());
					}
					
				});
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.sortDdpCategorizationList(List<DdpCategorizationHolder> ddpCategorizationHolders) - Exception while accessing sorting ddpCategorizationHoldersList.");
			ex.printStackTrace();
		}
		
		logger.debug("TaskUtil.sortDdpCategorizationList(List<DdpCategorizationHolder> ddpCategorizationHolders) executed successfully.");
		return ddpCategorizationHolders;
	}
	
	
	/************************************************
	 * 
	 * This method checks that any record exists for (JRF_ID i.e. SYN_ID and JRF_JOB_ID i.e. jobName passed to 'execute(String jobName)' method)
	 * and if found updates the JRF_ID otherwise insert new record for given jobName.
	 * 
	 *  
	 * 
	 * 
	 * @param dmsDdpSynId, jobHolder
	 * @return boolean
	 * @throws Exception 
	 *************************************************/
	public boolean updateJobRefHolder(String ddpJrfId, DdpJobRefHolder jobHolder, String jobName, Integer iStatus)
	{
		
		logger.debug("TaskUtil.updateJobRefHolder(String dmsDdpSynId, DdpJobRefHolder jobHolder) method invoked.");
		
		try
		{
			Date now = new Date();
			
			String strJrfId = ddpJrfId;
			Integer intJrfStatus = iStatus;
			Calendar calJrfModifiedDate = Calendar.getInstance();
			calJrfModifiedDate.setTime(now);
			if(jobHolder == null)//No record exists in DDP_JOB_REF_HOLDER for job (first execution)
			{
				//jobHolder is null can't access its id
				//logger.debug("TaskUtil.updateJobRefHolder(String dmsDdpSynId, DdpJobRefHolder jobHolder) - No record exists in DDP_JOB_REF_HOLDER for job [{}]. Inserting new record in DDP_JOB_REF_HOLDER",jobHolder.getId().getJrfJobId());
				String strJrfJobId = jobName;
				Calendar calJrfCreateDate = Calendar.getInstance();
				calJrfCreateDate.setTime(now);
				
				DdpJobRefHolderPK ddpJobRefHolderKey = new DdpJobRefHolderPK(strJrfId, strJrfJobId, intJrfStatus, calJrfCreateDate, calJrfModifiedDate);
				DdpJobRefHolder ddpJobRefHolder = new DdpJobRefHolder();
				ddpJobRefHolder.setId(ddpJobRefHolderKey);
				
				ddpJobRefHolderService.saveDdpJobRefHolder(ddpJobRefHolder);
			}
			else//Record exists in DDP_JOB_REF_HOLDER for job
			{
				logger.debug("TaskUtil.updateJobRefHolder(String dmsDdpSynId, DdpJobRefHolder jobHolder) - Record exists in DDP_JOB_REF_HOLDER for job [{}]. Updating the old SYN_ID [{}] with new SYN_ID [{}]",jobHolder.getId().getJrfJobId(),jobHolder.getId().getJrfId(),ddpJrfId);
				
				//&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
				//&&&&&&&& SHOULD BE FINE-TUNED to update instead delete and insert &&&&&&&&&&&&&
				//&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
				
				DdpJobRefHolderPK ddpJobRefHolderKeyOld = jobHolder.getId();//old key
								
				String strJrfJobId = jobHolder.getId().getJrfJobId();
				Calendar calJrfCreateDate = jobHolder.getId().getJrfCreatedDate();
				//calJrfCreateDate.setTime(now);
				
				
				//jobHolder.setId(ddpJobRefHolderKeyNew); //This is to update instead delete and insert
						
				//first delete the existing record
				ddpJobRefHolderService.deleteDdpJobRefHolder(jobHolder);
				//insert the new record with same jobHolder but replacing the Key with ddpJobRefHolderKeyNew
				calJrfCreateDate.setTime(now);
				DdpJobRefHolderPK ddpJobRefHolderKeyNew = new DdpJobRefHolderPK(strJrfId, strJrfJobId, intJrfStatus, calJrfCreateDate, calJrfModifiedDate);
				jobHolder.setId(ddpJobRefHolderKeyNew);
				ddpJobRefHolderService.updateDdpJobRefHolder(jobHolder);
			}
			
		}
		catch(Exception ex)
		{
			logger.error("DdpCategorizationTask.updateJobRefHolder(DmsDdpSyn dmsDdpSyn, String jobName) - Exception while updating DDP_JOB_REF_HOLDER table.");
			ex.printStackTrace();
			return false;
		}
		logger.debug("DdpCategorizationTask.updateJobRefHolder(DmsDdpSyn dmsDdpSyn, String jobName) executed successfully.");
		return true;
	}
	
	
	/************************************************
	 * 
	 * This method will check for existing record in DDP_DMS_DOCS_TXN table for given DTX_R_OBJECT_ID and 
	 * if DTX_R_OBJECT_ID exists, it will update the record and if DTX_R_OBJECT_ID does not exist, 
	 * it will insert a new record. After successful insert/update a record, it will return 
	 * auto generated DTX_ID to caller method.
	 *  
	 * 
	 * 
	 * @param dmsDdpSyn, strIsProcessReq
	 * @return Integer
	 *************************************************/
	public Integer updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq)
	{
		logger.debug("TaskUtil.updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq) method invoked.");
		
		Date now = new Date();
		Integer iDtxId = null;
		
		try
		{	
			Integer iSynId = dmsDdpSyn.getSynId();
			
			/***********************************************************************************************************************
			 *
			 * When job was forcefully killed, which may inserted few/all records into DDP_DMS_DOCS_TXN table but not into 
			 * DDP_DMS_DOCS_HOLDER and DDP_CATEGORIZATION_HOLDER tables. This will also keep the STATUS field as '0'(In Progress) in DDP_JOB_REF_HOLDER 
			 * table, which will cause DdpCategorizationTask job to re-process all the SYN_ID less than DDP_JOB_REF_HOLDER record.
			 * This re-process may insert the duplication record into DDP_DMS_DOCS_TXN table and to avoid this duplication,
			 * below section will check for existing R_OBJECT_ID in DDP_DMS_DOCS_TXN table. However, since this process expected to impact impact 
			 * system performance heavily, it has been commented. 
			 * 
			 * But need to find a better option to avoid duplication in DDP_DMS_DOCS_TXN table. 
			 * 
			 *************************************************************************************************************************/
			/*String strRObjectId = dmsDdpSyn.getSynRObjectId();
			DdpDmsDocsTxn existingDdpDmsDocsTxn = (DdpDmsDocsTxn)DdpDmsDocsTxn.findDdpDmsDocsTxnsByDtxRObjectIdEquals(strRObjectId);
			
			if(existingDdpDmsDocsTxn == null || existingDdpDmsDocsTxn.getDtxId() == null) //insert new record in DDP_DMS_DOCS_TXN
			{
				logger.debug("TaskUtil.updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq) - No record found in DDP_DMS_DOCS_TXN for SYN_ID [{}]. Inserting new record in DDP_DMS_DOCS_TXN",iSynId);
				
				DdpDmsDocsTxn newDdpDmsDocsTxn = new DdpDmsDocsTxn();
				
				newDdpDmsDocsTxn.setDtxRObjectId(dmsDdpSyn.getSynRObjectId());
				newDdpDmsDocsTxn.setDtxIsProcessReq(strIsProcessReq);
				newDdpDmsDocsTxn.setDtxGenSystem(dmsDdpSyn.getSynGenSystem());
				
				String strTransLogs = iSynId+"|"+dmsDdpSyn.getSynDocType()+"|"+dmsDdpSyn.getSynClientId()+"|"+dmsDdpSyn.getSynCompanySource()+"|"+dmsDdpSyn.getSynBranchSource()+"|"+dmsDdpSyn.getSynGenSystem()+"|"+dmsDdpSyn.getSynConsignee()+"|"+dmsDdpSyn.getSynShipper()+"|"+dmsDdpSyn.getSynTboName();
				
				newDdpDmsDocsTxn.setDtxTransLogs(strTransLogs.length() > 250 ?strTransLogs.substring(0, 250):strTransLogs);//limited to 250 chars
				newDdpDmsDocsTxn.setDtxStatus("0");//"0" - Active "1" - Inactive
				//newDdpDmsDocsTxn.setDtxArchive("");//this has to be decided in later stage
				//newDdpDmsDocsTxn.setDtxRetentionId(0);//this has to be decided in later stage
				//newDdpDmsDocsTxn.setDtxRetentionDate(null);//this has to be decided in later stage
				Calendar dtxCreatedDate = Calendar.getInstance();
				dtxCreatedDate.setTime(now);
				newDdpDmsDocsTxn.setDtxCreatedDate(dtxCreatedDate);
				
				
				//ddpDmsDocsTxnService.saveDdpDmsDocsTxn(newDdpDmsDocsTxn);
				ddpDmsDocsTxnRepository.saveAndFlush(newDdpDmsDocsTxn);
				
				iDtxId = newDdpDmsDocsTxn.getDtxId();
			}
			else//update a record in DDP_DMS_DOCS_TXN where R_OBJECT_ID matches
			{
				iDtxId = existingDdpDmsDocsTxn.getDtxId();
				logger.debug("TaskUtil.updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq) - Record exists in DDP_DMS_DOCS_TXN table for SYN_ID [{}]. Updating existing record  [{}] with modified date.",iSynId,iDtxId);
				
				existingDdpDmsDocsTxn.setDtxIsProcessReq(strIsProcessReq);
				
				Calendar dtxModifiedDate = Calendar.getInstance();
				dtxModifiedDate.setTime(now);
				existingDdpDmsDocsTxn.setDtxModifiedDate(dtxModifiedDate);
				
				//ddpDmsDocsTxnService.updateDdpDmsDocsTxn(existingDdpDmsDocsTxn);
				ddpDmsDocsTxnRepository.saveAndFlush(existingDdpDmsDocsTxn);
			}
			*/
			logger.debug("TaskUtil.updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq) - Inserting new record in DDP_DMS_DOCS_TXN for [{}]",iSynId);
			
			DdpDmsDocsTxn newDdpDmsDocsTxn = new DdpDmsDocsTxn();
			
			newDdpDmsDocsTxn.setDtxRObjectId(dmsDdpSyn.getSynRObjectId());
			newDdpDmsDocsTxn.setDtxIsProcessReq(strIsProcessReq);
			newDdpDmsDocsTxn.setDtxGenSystem(dmsDdpSyn.getSynGenSystem());
			
			String strTransLogs = iSynId+"|"+dmsDdpSyn.getSynDocType()+"|"+dmsDdpSyn.getSynClientId()+"|"+dmsDdpSyn.getSynCompanySource()+"|"+dmsDdpSyn.getSynBranchSource()+"|"+dmsDdpSyn.getSynGenSystem()+"|"+dmsDdpSyn.getSynConsignee()+"|"+dmsDdpSyn.getSynShipper()+"|"+dmsDdpSyn.getSynTboName();
			
			newDdpDmsDocsTxn.setDtxTransLogs(strTransLogs.length() > 250 ?strTransLogs.substring(0, 250):strTransLogs);//limited to 250 chars
			newDdpDmsDocsTxn.setDtxStatus("0");//"0" - Active "1" - Inactive
			//newDdpDmsDocsTxn.setDtxArchive("");//this has to be decided in later stage
			//newDdpDmsDocsTxn.setDtxRetentionId(0);//this has to be decided in later stage
			//newDdpDmsDocsTxn.setDtxRetentionDate(null);//this has to be decided in later stage
			Calendar dtxCreatedDate = Calendar.getInstance();
			dtxCreatedDate.setTime(now);
			newDdpDmsDocsTxn.setDtxCreatedDate(dtxCreatedDate);
			newDdpDmsDocsTxn.setDtxCreatedBy(Constant.USERLOGINIDCORE);
			newDdpDmsDocsTxn.setDtxModifiedDate(dtxCreatedDate);
			newDdpDmsDocsTxn.setDtxModifiedBy(Constant.USERLOGINIDCORE);
			newDdpDmsDocsTxn.setDtxSynId(dmsDdpSyn.getSynId());
			
			//ddpDmsDocsTxnService.saveDdpDmsDocsTxn(newDdpDmsDocsTxn);
			ddpDmsDocsTxnRepository.saveAndFlush(newDdpDmsDocsTxn);
			
			iDtxId = newDdpDmsDocsTxn.getDtxId();
		
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq) - Exception while updating/inserting DDP_DMS_DOCS_TXN table.");
			ex.printStackTrace();
		}
		logger.debug("TaskUtil.updateDdpDmsDocsTxn(DmsDdpSyn dmsDdpSyn, String strIsProcessReq) executed successfully.");
		return iDtxId;
	}
	
	public void syncDmsDdpSynAndDdpDmsDocsHolderBatch(final List<Object> ddpDmsDocsHolderList) throws Exception
	{
		logger.debug("DdpCategorizationTask.syncDmsDdpSynAndDdpDmsDocsHolder(final List<DmsDdpSyn> dmsDdpSynList) method invoked.");
		try
		{
			jdbcTemplate.batchUpdate(Constant.DEF_SQL_INSERT_DDP_DMS_DOCS_HOLDER, new BatchPreparedStatementSetter() {
				 
				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					DdpDmsDocsHolder ddpDmsDocsHolder = (DdpDmsDocsHolder)ddpDmsDocsHolderList.get(i);
					
					ps.setInt(1, ddpDmsDocsHolder.getThlSynId());
					ps.setString(2, ddpDmsDocsHolder.getThlRObjectId());
					ps.setString(3, ddpDmsDocsHolder.getThlIsProcessReq());//0 - Yes, 1 - No
					ps.setString(4, ddpDmsDocsHolder.getThlTboName());
				}
			 
				@Override
				public int getBatchSize() {
					return ddpDmsDocsHolderList.size();
				}
			  });
		}
		catch(Exception ex)
		{
			logger.error("DdpCategorizationTask.syncDmsDdpSynAndDdpDmsDocsHolder(final List<DmsDdpSyn> dmsDdpSynList) - Exception while sync between DmsDdpSyn and DdpDmsDocsHolder.",ex);
			ex.printStackTrace();
			throw new Exception(ex);
		}
		logger.debug("DdpCategorizationTask.syncDmsDdpSynAndDdpDmsDocsHolder(final List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
	}
	
	
	public void insertDdpCategorizationHolderBatch(final List<Object> ddpCategorizationHolderList) throws Exception
	{
		logger.debug("DdpCategorizationTask.syncDmsDdpSynAndDdpDmsDocsHolder(final List<DmsDdpSyn> dmsDdpSynList) method invoked.");
		try
		{
			jdbcTemplate.batchUpdate(Constant.DEF_SQL_INSERT_DDP_CATEGORIZATION_HOLDER, new BatchPreparedStatementSetter() {
				 
				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					DdpCategorizationHolder ddpCategorizationHolder = (DdpCategorizationHolder) ddpCategorizationHolderList.get(i);
					
					ps.setInt(1, ddpCategorizationHolder.getChlSynId());
					ps.setInt(2, ddpCategorizationHolder.getChlDtxId());
					ps.setInt(3, ddpCategorizationHolder.getChlRulId());
				}
			 
				@Override
				public int getBatchSize() {
					return ddpCategorizationHolderList.size();
				}
			  });
		}
		catch(Exception ex)
		{
			logger.error("DdpCategorizationTask.syncDmsDdpSynAndDdpDmsDocsHolder(final List<DmsDdpSyn> dmsDdpSynList) - Exception while sync between DmsDdpSyn and DdpDmsDocsHolder.",ex);
			ex.printStackTrace();
			throw new Exception(ex);
		}
		logger.debug("DdpCategorizationTask.syncDmsDdpSynAndDdpDmsDocsHolder(final List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
	}
	
    //Get the documentum Session
	    @Value( "${dms.docbaseName}" )
		String docbaseName  ;
//				String docbaseName = "ZUSLogistics1UAT" ;
				
		@Value( "${dms.userName}" )
	    String userName ;
				
//				String userName="dmadminuat" ;
		@Value( "${dms.password}" )
	    String password ;
//				String password ="agildcmnsy";
		//IDfSession session;
			    
	    
	    @Transactional(readOnly = true)
	public boolean insertDdpCategorization(List<DdpCategorizationHolder> ddpCategorizationHolder) throws Exception{
		logger.debug("Insert into DdpCategorization");
		Date now = new Date();
		IDfSession session = null;
		//IDfSessionManager sMgr =null;
	  try{
			  	//Below entry added for reference purpose - PLEASE DON'T UNCOMMENT THE BELOW LINE
			  	//System.setProperty("dfc.properties.file", "E:/jboss_as_7.1.1.Final/standalone/configuration/dfc.properties");

			    IDfClientX clientx = new DfClientX();
			    IDfClient  client = clientx.getLocalClient();
			    IDfSessionManager sMgr = client.newSessionManager();
			    IDfLoginInfo loginInfoObj = clientx.getLoginInfo();
			    loginInfoObj.setUser(userName);
			    loginInfoObj.setPassword(password);
			    sMgr.setIdentity(docbaseName, loginInfoObj);
			    logger.info("process started for creating session manager.");
		        session = sMgr.getSession(docbaseName);  
		  }catch(Exception e){
			  //TODO: need to send the DFC mail.
			logger.error("TaskUtil.insertDdpcategorization() - Failed to create DFC session");
			sendMailByDevelopers("TaskUtil.insertDdpCategorization() : DFCSession issue", " inside  TaskUtil.insertDdpCategorization(String jobName) - DFCSession issue");
			logger.error(e.toString());
			e.printStackTrace();
			return false;
		  }
	  if (session == null) {
		  logger.info("TaskUtil.insertDdpcategorization() - DFC session is null");
		 return false;
	  }
		
	  		List<Integer> invalidDocsTxnIDs = new ArrayList<Integer>();
			  logger.info("CreateSessionManager: Document source = "+docbaseName+":User Name="+userName+":Password="+password);
			
			  try{
				  	//Get the distinct DTX Id
				  List<DdpDmsDocsTxn> dmsDocsTxns = getDistinctDtxIds(ddpCategorizationHolder);
//				  for(DdpCategorizationHolder categorizationHolder : ddpCategorizationHolder){
			      for(DdpDmsDocsTxn ddpDmsDocsTxn : dmsDocsTxns){
			    	    String r_object_id  =null; 
			    	    Hashtable<String, String> dmsMetadata = null;
			    		try{
//							  r_object_id  = dmsDdpSynService.findDmsDdpSyn(categorizationHolder.getChlSynId()).getSynRObjectId();
			    			  r_object_id  = ddpDmsDocsTxn.getDtxRObjectId(); 
			    			 if (!session.isConnected()) 
			    				 throw new Exception ("DFCSession Connection issue");
			    			 
			    			  try  {
			    				  dmsMetadata = new DdpDFCClient().begin(session,r_object_id);
			    				  if (dmsMetadata.size() == 0) {
			    					  invalidDocsTxnIDs.add(ddpDmsDocsTxn.getDtxId());
				    				  continue;
			    				  }
			    					  
			    			  } catch (Exception ex) {
			    				  invalidDocsTxnIDs.add(ddpDmsDocsTxn.getDtxId());
			    				  continue;
			    			  }
					    	  catCreatedDate.setTime(now);
				              DdpCategorizedDetail categorizedDetail = new DdpCategorizedDetail();
//						      categorizedDetail.setCadDtxId(ddpDmsDocsTxnService.findDdpDmsDocsTxn(categorizationHolder.getChlDtxId()));
				              categorizedDetail.setCadDtxId(ddpDmsDocsTxn);
				              categorizedDetail.setCadRObjectId(r_object_id);
				              categorizedDetail.setCadCreatedBy("admin");
				              categorizedDetail.setCadCreatedDate(catCreatedDate);
				              categorizedDetail.setCadModifiedBy("admin");
				              categorizedDetail.setCadModifiedDate(catCreatedDate);
//						      categorizedDetail.setCadOriginSysId(categorizationHolder.getChlSynId());
				              categorizedDetail.setCadOriginSysId(ddpDmsDocsTxn.getDtxSynId());
				              categorizedDetail.setCadRulesCompletedCount(null);
				              categorizedDetail.setCadRulesFailedCount(null);
				              categorizedDetail.setCadRulesInProgressCount(null);
				              categorizedDetail.setCadServiceType("");
				              categorizedDetail.setCadStatus(0);
//						      categorizedDetail.setCadTotalRulesReqCount(0);
				              		//GET COUNT OF RULES
				              int rulesCount = getRulesCount(ddpDmsDocsTxn);
				              categorizedDetail.setCadTotalRulesReqCount(rulesCount);
//						      categorizedDetail.setCadTransMessage(ddpDmsDocsTxnService.findDdpDmsDocsTxn(categorizationHolder.getChlDtxId()).getDtxTransLogs());
				              categorizedDetail.setCadTransMessage(ddpDmsDocsTxn.getDtxTransLogs());
				              	//Save categorization 
				              ddpCategorizedDetailService.saveDdpCategorizedDetail(categorizedDetail);
			    			}catch(Exception ex){
						    	  logger.error("Error while Insert into DdpCategorizedDetail Table ",ex);
								  ex.printStackTrace();
								  throw new Exception(ex);
								  //return false;
			    			} 

			    	  try{
				    		  //Using the syn id, query the syn table and query the DMS attirbute for the r_object_id
//							  Hashtable<String, String> dmsMetadata = new DdpDFCClient().begin(sMgr,r_object_id,docbaseName);
			    		  	 
					    	  DdpDmsDocsDetail dmsDocsDetail = new DdpDmsDocsDetail();
//							  dmsDocsDetail.setDddDtxId(ddpDmsDocsTxnService.findDdpDmsDocsTxn(categorizationHolder.getChlDtxId()));
					    	  dmsDocsDetail.setDddDtxId(ddpDmsDocsTxn);
					    	  dmsDocsDetail.setDddRObjectId(dmsMetadata.get("r_object_id"));
					    	  String strObjectName = dmsMetadata.get("object_name");
					    	  //Removing extension from Object Name 
					    	  strObjectName = strObjectName.substring(0,strObjectName.lastIndexOf('.'));
					    	  dmsDocsDetail.setDddObjectName(dmsMetadata.get("object_name"));
					    	  if(dmsMetadata.get("agl_control_doc_type").equalsIgnoreCase("BULKINV")||dmsMetadata.get("agl_control_doc_type").equalsIgnoreCase("BULKCRD")){
					    		  dmsDocsDetail.setDddJobNumber(dmsMetadata.get("agl_job_numbers").substring(0, 10));
					    	  }else{
					    		  dmsDocsDetail.setDddJobNumber(dmsMetadata.get("agl_job_numbers"));  
					    	  }
					    	  dmsDocsDetail.setDddControlDocType(dmsMetadata.get("agl_control_doc_type"));
					    	  dmsDocsDetail.setDddConsignmentId(dmsMetadata.get("agl_consignment_id"));
					    	  dmsDocsDetail.setDddDocRef(dmsMetadata.get("agl_doc_ref"));
					    	  dmsDocsDetail.setDddGlobalDocRef(dmsMetadata.get("agl_global_doc_ref"));
					    	  dmsDocsDetail.setDddCarrierRef(dmsMetadata.get("agl_carrier_ref"));
					    	  dmsDocsDetail.setDddCompanySource(dmsMetadata.get("agl_company_source"));
					    	  dmsDocsDetail.setDddCompanyDestination(dmsMetadata.get("agl_company_destination"));
					    	  dmsDocsDetail.setDddBranchSource(dmsMetadata.get("agl_branch_source"));
					    	  dmsDocsDetail.setDddBranchDestination(dmsMetadata.get("agl_branch_destination"));
					    	  dmsDocsDetail.setDddDeptSource(dmsMetadata.get("agl_dept_source"));
					    	  dmsDocsDetail.setDddDeptDestination(dmsMetadata.get("agl_dept_destination"));
					    	  dmsDocsDetail.setDddBillOfLadingNumber(dmsMetadata.get("agl_bill_of_lading_number"));
					    	  dmsDocsDetail.setDddMasterAirwayBillNum(dmsMetadata.get("agl_master_airway_bill_num"));
					    	  dmsDocsDetail.setDddHouseAirwayBillNum(dmsMetadata.get("agl_house_airway_bill_num"));
					    	  dmsDocsDetail.setDddMasterJobNumber(dmsMetadata.get("agl_master_job_number"));
					    	  dmsDocsDetail.setDddClientId(dmsMetadata.get("agl_client_id"));
					    	  dmsDocsDetail.setDddConsignee(dmsMetadata.get("agl_consignee"));
					    	  dmsDocsDetail.setDddShipper(dmsMetadata.get("agl_shipper"));
					    	  dmsDocsDetail.setDddDebitsBack(dmsMetadata.get("agl_debits_back"));
					    	  dmsDocsDetail.setDddDebitsForward(dmsMetadata.get("agl_debits_forward"));
					    	  dmsDocsDetail.setDddNotifyParty(dmsMetadata.get("agl_notify_party"));
					    	  dmsDocsDetail.setDddInitialAgent(dmsMetadata.get("agl_initial_agent"));
					    	  dmsDocsDetail.setDddFinalAgentId(dmsMetadata.get("agl_final_agent_id"));
					    	  dmsDocsDetail.setDddIntermediateAgent(dmsMetadata.get("agl_intermediate_agent"));
					    	  dmsDocsDetail.setDddCustomsEntryNo(dmsMetadata.get("agl_customs_entry_no"));
					    	  dmsDocsDetail.setDddDocVersion(dmsMetadata.get("r_version_label"));
					    	  
					    	  Integer intIsRate =0;
					    	  if(dmsMetadata.get("agl_is_rated").equalsIgnoreCase("F") || dmsMetadata.get("agl_is_rated").equalsIgnoreCase("0")) intIsRate = 0;
					    	  else if(dmsMetadata.get("agl_is_rated").equalsIgnoreCase("T") || dmsMetadata.get("agl_is_rated").equalsIgnoreCase("1")) intIsRate = 1;
					    	  
					    	  dmsDocsDetail.setDddIsRated(intIsRate);
					    	  dmsDocsDetail.setDddBatchName(dmsMetadata.get("agl_global_doc_ref"));
					    	  dmsDocsDetail.setDddGeneratingSystem(dmsMetadata.get("agl_generating_system"));
					    	  dmsDocsDetail.setDddUserId(dmsMetadata.get("agl_user_id"));
					    	  dmsDocsDetail.setDddUserReference(dmsMetadata.get("agl_user_reference"));
					    	  dmsDocsDetail.setDddNote(dmsMetadata.get("agl_note"));
					    	  dmsDocsDetail.setDddFileType(dmsMetadata.get("agl_file_type"));
					    	  dmsDocsDetail.setDddContentType(dmsMetadata.get("a_content_type"));
					    	  dmsDocsDetail.setDddContentSize(dmsMetadata.get("r_content_size"));
					    	  //Save DdpDmsDetailService	    	  
					    	  ddpDmsDocsDetailService.saveDdpDmsDocsDetail(dmsDocsDetail);
			    	  }catch(Exception e){
			    		  logger.error("Insert into DdpDmsDocsDetailService ",e);
			  			  e.printStackTrace();
			  			throw new Exception(e);
			  			//TODO: mail need to send
			  			//return false;
			    	  }
			    }
			      
			      try{
				      for(DdpCategorizationHolder  categorizationHolder : ddpCategorizationHolder){
				    	  
				    	  /****while categorizing printed documents , compared rule details stored in categorizationHolder table
				    	   *   when update metadata job running no guarantee that compared rule details exists
				    	   *   check whether rule details exists or not.                                               *****/
				    	   
					    	  if(ddpRuleDetailService.findDdpRuleDetail(categorizationHolder.getChlRulId()) != null && !invalidDocsTxnIDs.contains(categorizationHolder.getChlDtxId())){
					    		  
						    	  DdpCategorizedDocs ddpCategorizedDocs = new DdpCategorizedDocs();
						    	  ddpCategorizedDocs.setCatDtxId(ddpDmsDocsTxnService.findDdpDmsDocsTxn(categorizationHolder.getChlDtxId()));
						    	  ddpCategorizedDocs.setCatRulId( ddpRuleService.findDdpRule( ddpRuleDetailService.findDdpRuleDetail(categorizationHolder.getChlRulId()).getRdtRuleId().getRulId() ) );
						    	  ddpCategorizedDocs.setCatRdtId(categorizationHolder.getChlRulId());
						    	  ddpCategorizedDocs.setCatStatus(0);
						    	  ddpCategorizedDocs.setCatCreatedBy("admin");
						    	  catCreatedDate.setTime(now);
						    	  ddpCategorizedDocs.setCatCreatedDate(catCreatedDate);
						    	  ddpCategorizedDocs.setCatModifiedDate(catCreatedDate);
						    	  ddpCategorizedDocs.setCatModifiedBy("admin");
						    	  ddpCategorizedDocs.setCatRuleType(ddpRuleDetailService.findDdpRuleDetail(categorizationHolder.getChlRulId()).getRdtRuleType());
						    	  ddpCategorizedDocs.setCatSynId(categorizationHolder.getChlSynId());
						    	  //Save the DDPCategorization
						    	  ddpCategorizedDocsService.saveDdpCategorizedDocs(ddpCategorizedDocs);
					    	  }
					    	  else
					    	  {
					    		  logger.info("TaskUtil.insertDdpCategorization : ddpRuleDetail id:"+categorizationHolder.getChlRulId()+" not available for categorizationHolder"+categorizationHolder.getChlId());
					    	  }
				      	}
					  }catch(Exception ex){
							logger.error("Error while, Insert into DdpCategorizedDocs Table - Exception while Chl inserting into DdpCategorizedDocs ",ex);
							ex.printStackTrace();
							throw new Exception(ex);
							//TODO: send the mail 
							//return false;
					  }
			  }catch(Exception e){
					logger.error("Failed to create session",e);
					logger.error(e.toString());
					e.printStackTrace();
					throw new Exception(e);
					//TODO: mail need to send
			  }finally {
				  if (session != null) 
					  session.getSessionManager().release(session);
				  
		      }
			  logger.info("DdpUpdateMetadataTask.insertDdpCategorization(List<DdpCategorizationHolder> ddpCategorizationHolder) executed successfully.");
			return true;
    }
	/**
	 * 
	 * @param ddpCategorizationHolder
	 * @return
	 */
	public List<DdpDmsDocsTxn> getDistinctDtxIds(List<DdpCategorizationHolder> ddpCategorizationHolder)
	{
		String chlIdsLst = "";
		String col="CHL_ID=";
		for(DdpCategorizationHolder categorizationHolder:ddpCategorizationHolder)
		{
			chlIdsLst +=col+categorizationHolder.getChlId()+" OR ";
		}
		chlIdsLst = chlIdsLst.substring(0, chlIdsLst.lastIndexOf(" OR"));
		List<DdpDmsDocsTxn> ddpDmsDocsTxns = this.jdbcTemplate.query("SELECT DISTINCT CHL_DTX_ID FROM DDP_CATEGORIZATION_HOLDER WHERE "+chlIdsLst, new Object[]{},new RowMapper<DdpDmsDocsTxn>(){
			
			@Override
			public DdpDmsDocsTxn mapRow(ResultSet rs, int rowNum)
					throws SQLException {
				DdpDmsDocsTxn dmsDocsTxn = ddpDmsDocsTxnService.findDdpDmsDocsTxn(Integer.parseInt(rs.getString("CHL_DTX_ID")));
				return dmsDocsTxn;
			}
		});
		return ddpDmsDocsTxns;
	}
	/**
	 * 
	 * @param ddpDmsDocsTxn
	 * @return
	 */
	public Integer getRulesCount(DdpDmsDocsTxn ddpDmsDocsTxn)
	{
		int rulesCount = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_GET_RULE_COUNT_BY_DTX_ID, new Object[]{ddpDmsDocsTxn.getDtxId()},Integer.class);
		return rulesCount;
	}
	public List<DdpDmsDocsDetail> getDdpDmsDocsDetail(final int  doc_detail_dtx_id)
	{
		logger.debug("");
		List<DdpDmsDocsDetail> DdpDmsDocsDetail = null;
		
		try
		{
			DdpDmsDocsDetail = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_DOCS_DETAIL, new Object[] {doc_detail_dtx_id}, new RowMapper<DdpDmsDocsDetail>() {
							
				public DdpDmsDocsDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					  DdpDmsDocsDetail dmsDocsDetail = new DdpDmsDocsDetail();
			    	  dmsDocsDetail.setDddDtxId(ddpDmsDocsTxnService.findDdpDmsDocsTxn(doc_detail_dtx_id));
			    	  dmsDocsDetail.setDddRObjectId(rs.getString("DDD_R_OBJECT_ID"));
			    	  dmsDocsDetail.setDddObjectName(rs.getString("DDD_OBJECT_NAME"));
			    	  dmsDocsDetail.setDddJobNumber(rs.getString("DDD_JOB_NUMBER"));
			    	  dmsDocsDetail.setDddControlDocType(rs.getString("DDD_CONTROL_DOC_TYPE"));
			    	  dmsDocsDetail.setDddConsignmentId(rs.getString("DDD_CONSIGNMENT_ID"));
			    	  dmsDocsDetail.setDddDocRef(rs.getString("DDD_DOC_REF"));
			    	  dmsDocsDetail.setDddGlobalDocRef(rs.getString("DDD_GLOBAL_DOC_REF"));
			    	  dmsDocsDetail.setDddCarrierRef(rs.getString("DDD_CARRIER_REF"));
			    	  dmsDocsDetail.setDddCompanySource(rs.getString("DDD_COMPANY_SOURCE"));
			    	  dmsDocsDetail.setDddCompanyDestination(rs.getString("DDD_COMPANY_DESTINATION"));
			    	  dmsDocsDetail.setDddBranchSource(rs.getString("DDD_BRANCH_SOURCE"));
			    	  dmsDocsDetail.setDddBranchDestination(rs.getString("DDD_BRANCH_DESTINATION"));
			    	  dmsDocsDetail.setDddDeptSource(rs.getString("DDD_DEPT_SOURCE"));
			    	  dmsDocsDetail.setDddDeptDestination(rs.getString("DDD_DEPT_DESTINATION"));
			    	  dmsDocsDetail.setDddBillOfLadingNumber(rs.getString("DDD_BILL_OF_LADING_NUMBER"));
			    	  dmsDocsDetail.setDddMasterAirwayBillNum(rs.getString("DDD_MASTER_AIRWAY_BILL_NUM"));
			    	  dmsDocsDetail.setDddHouseAirwayBillNum(rs.getString("DDD_HOUSE_AIRWAY_BILL_NUM"));
			    	  dmsDocsDetail.setDddMasterJobNumber(rs.getString("DDD_MASTER_JOB_NUMBER"));
			    	  dmsDocsDetail.setDddClientId(rs.getString("DDD_CLIENT_ID"));
			    	  dmsDocsDetail.setDddConsignee(rs.getString("DDD_CONSIGNEE"));
			    	  dmsDocsDetail.setDddShipper(rs.getString("DDD_SHIPPER"));
			    	  dmsDocsDetail.setDddDebitsBack(rs.getString("DDD_DEBITS_BACK"));
			    	  dmsDocsDetail.setDddDebitsForward(rs.getString("DDD_DEBITS_FORWARD"));
			    	  dmsDocsDetail.setDddNotifyParty(rs.getString("DDD_NOTIFY_PARTY"));
			    	  dmsDocsDetail.setDddInitialAgent(rs.getString("DDD_INITIAL_AGENT"));
			    	  dmsDocsDetail.setDddFinalAgentId(rs.getString("DDD_FINAL_AGENT_ID"));
			    	  dmsDocsDetail.setDddIntermediateAgent(rs.getString("DDD_INTERMEDIATE_AGENT"));
			    	  dmsDocsDetail.setDddCustomsEntryNo(rs.getString("DDD_CUSTOMS_ENTRY_NO"));
			    	  
			    	  dmsDocsDetail.setDddIsRated(rs.getInt("DDD_IS_RATED"));
			    	  
			    	  dmsDocsDetail.setDddBatchName(rs.getString("DDD_BATCH_NAME"));
			    	  dmsDocsDetail.setDddGeneratingSystem(rs.getString("DDD_GENERATING_SYSTEM"));
			    	  dmsDocsDetail.setDddUserId(rs.getString("DDD_USER_ID"));
			    	  dmsDocsDetail.setDddUserReference(rs.getString("DDD_USER_REFERENCE"));
			    	  dmsDocsDetail.setDddNote(rs.getString("DDD_NOTE"));
			    	  dmsDocsDetail.setDddFileType(rs.getString("DDD_FILE_TYPE"));
			    	  dmsDocsDetail.setDddContentType(rs.getString("DDD_CONTENT_TYPE"));
			    	  dmsDocsDetail.setDddContentSize(rs.getString("DDD_CONTENT_SIZE"));
					
					//dmsDdpSyn.setSynTboName(rs.getString("SYN_TBO_NAME"));
					
					return dmsDocsDetail;
		           }
			});		
			
		}
		catch(Exception ex)
		{
			logger.error("");
			ex.printStackTrace();
		}
		
		logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) executed successfully.");
		return DdpDmsDocsDetail;
	}
	
	 //Get the property file data
    @Value( "${mail.toAddress}" )
	String resToAddress  ;
	
    @Value( "${mail.fromAddress}" )
  	String resFromAddress  ;
    
    @Value( "${mail.subject}" )
  	String resSubject  ;
    
    @Value( "${mail.body}" )
  	String resBody  ;
    
    @Value( "${mail.fileLocation}" )
  	String altUnc  ;
	/**
	 * 
	 * @param ddpCommEmails
	 * @param fileName
	 * @param smtpAddres
	 */
	public int sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres,String mailSubject,String mailBody,String mailFrom,String ruleType,String branchCCMailId,Integer catId,File file, String base64Binary,String contentType) {
		logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres) Method Invoked.");
		int attachFlag =-1;
		try {
				String toAddress ="";
				String fromAddress ="";
				String ccAddress ="";
				//String fileLocation = "";
				
				StringBuffer strSubject = new StringBuffer();
				StringBuffer strbody = new StringBuffer();
				if(null!=ddpCommEmails)
				{
					toAddress = ddpCommEmails.getCemEmailTo();
					fromAddress =  ddpCommEmails.getCemEmailFrom();
					if(null==fromAddress || fromAddress.equals("")){
						//Read from the propery file, need to fine tue.
//						fromAddress = "svc-dmsadmin@agility.com";
						fromAddress = mailFrom;
					}
					logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres) : FromAddress :"+fromAddress);
					ccAddress = ddpCommEmails.getCemEmailCc();
					if(null==ddpCommEmails.getCemEmailSubject()){
						strSubject.append(mailSubject);
					}else{
						strSubject.append(ddpCommEmails.getCemEmailSubject());	
					}
					if(null==ddpCommEmails.getCemEmailBody()){
						strbody.append(mailBody);
					}else{
						strbody.append(ddpCommEmails.getCemEmailBody());	
					}
					
				}else{
					//This email address should be get from the property files
					toAddress =resToAddress;
				}
				Properties props = System.getProperties();
//				props.put("mail.smtp.auth", true);
//				props.put("mail.smtp.starttls.enable", true);
				props.put("mail.smtp.host",smtpAddres);
				// -- Attaching to default Session, or we could start a new one --
//				props.put("mail.smtp.host", "");
				Session session = Session.getDefaultInstance(props, null);
				
				
				// -- Create a new message --
				Message msg = new MimeMessage(session);
				// -- Set the FROM and TO fields --
				msg.setFrom(new InternetAddress(fromAddress));
				//--- To Address
				toAddress = toAddress.replaceAll(";", ",");
				if (toAddress != null && toAddress.length() > 0)
					toAddress = getValidEmailAddress(toAddress);

				logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : ToAddress: "+toAddress);
//				StringTokenizer toAddressTokens = new StringTokenizer(toAddress, ",");
//				InternetAddress[] addressTo = new InternetAddress[toAddressTokens
//						.countTokens()];
//				int j = 0;
//				while (toAddressTokens.hasMoreTokens()) {
//					addressTo[j] = new InternetAddress(toAddressTokens.nextToken());
//					j++;
//				}
				msg.setRecipients(Message.RecipientType.TO,InternetAddress.parse(toAddress, false));
				
				//CC address
				
				if(ccAddress != null) 
				{
					if(branchCCMailId !=null && ! branchCCMailId.equalsIgnoreCase(""))
					{
						if(ccAddress.equals(""))
						{
							ccAddress=branchCCMailId;
						}
						else
							ccAddress=ccAddress.concat(","+branchCCMailId);
					}
					ccAddress = ccAddress.replaceAll(";",",");
					if (ccAddress.length() > 0)
						ccAddress = getValidEmailAddress(ccAddress);

					logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : CCAddress: "+ccAddress);
//					StringTokenizer ccAddressTokens = new StringTokenizer(ccAddress, ",");
//					InternetAddress[] addressCc = new InternetAddress[ccAddressTokens
//							.countTokens()];
//					int k = 0;
//					while (ccAddressTokens.hasMoreTokens()) {
//						addressCc[k] = new InternetAddress(ccAddressTokens.nextToken());
//						k++;
//					}
					
//					msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(addressCc, false));
					msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(ccAddress, false));
				}
				else
				{
					if(branchCCMailId !=null && ! branchCCMailId.equalsIgnoreCase(""))
					{
						ccAddress=branchCCMailId;
						logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : CCAddress: "+ccAddress);
						InternetAddress[] addressCC=new InternetAddress[1];
						addressCC[0]=new InternetAddress(ccAddress);
						msg.setRecipients( javax.mail.Message.RecipientType.CC,addressCC);
					}
					
				}
				
				MimeBodyPart messageBodyPart = new MimeBodyPart();
				messageBodyPart.setContent(strbody.toString(),"text/html");
//				messageBodyPart.setContent(content,"text/plain; charset=\"utf-8\"");
				Multipart multipart = new MimeMultipart();
				multipart.addBodyPart(messageBodyPart);
				
				DataSource source = null;
				ByteArrayDataSource bds = null;
				DataHandler dataHandler = null;
				
//				// Part two is attachment
				messageBodyPart = new MimeBodyPart();
				if (file != null || base64Binary != null) {
					
					if(base64Binary != null){
						bds = new ByteArrayDataSource(Base64.decodeBase64(base64Binary),contentType);
						dataHandler = new DataHandler(bds);
						messageBodyPart.setFileName(fileName);
					}
					else{
						source = new FileDataSource(file);
						dataHandler = new DataHandler(source);
						messageBodyPart.setFileName(file.getName());
					}
					messageBodyPart.setDataHandler(dataHandler);
					multipart.addBodyPart(messageBodyPart);
					attachFlag = 1;
				}

				msg.setContent(multipart);
				
			/*	// create the message part
				MimeBodyPart messageBodyPart = new MimeBodyPart();
				messageBodyPart.setContent(strbody.toString(),"text/html");

				Multipart multipart = new MimeMultipart();
				

				//Filter only PDf files
			
//		        		FileDataSource fileDataSource =new FileDataSource(files[i]);
//			        	messageBodyPart.setDataHandler(new DataHandler(fileDataSource));
//			        	messageBodyPart.setFileName(files[i].getName());
//			        	messageBodyPart.setDataHandler(new DataHandler(new FileDataSource(files[i])));
//			        	messageBodyPart.setFileName(files[i].getName());
				if (file != null && file.exists()) {
					
					//multipart.addBodyPart(messageBodyPart);
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(file);
					attachFlag = 1;
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(file.getName());
				    multipart.addBodyPart(messageBodyPart);
			        
					
				}
			
				msg.setContent(multipart);	*/
				
				if(env.getProperty("mail.evn").equalsIgnoreCase("UAT"))
					strSubject = strSubject.append("- UAT");
				msg.setSubject(strSubject.toString());
				// msg.setText(body);
				// -- Set some other header information --

				msg.setSentDate(new Date());
				// -- Send the message --
				if(! (attachFlag==-1 && ruleType.equalsIgnoreCase("AED_RULE") )  )
				{
					DdpCategorizedDocs categorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(catId);
					if(categorizedDocs.getCatStatus() != 1)
					{
						Transport.send(msg);
						logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : mail sent with attachment for catId::"+catId);
					}
				}
		        if(attachFlag == -1)
		        {
		        	attachFlag = 2;
		        	logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : "+fileName+" not found in location");
		        }
		        return attachFlag;
	
			} catch (Exception ex) {
				logger.error("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  :"+ex.getMessage());
				logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  - Changing the status to -1 due to exception : "+ fileName);
			ex.printStackTrace();
			return -1;
		}
	} 
	
	
	/**
	 * 
	 * if no job Holder in table send a mail.
	 * * */
	 
	 public void sendMail(String EmailAddress) {
			try {
				// 10.20.1.27

				System.out.println("sending from "+EmailAddress);
				String sendTo="";
				String MailTo=Constant.DMSTEAMEmailId;
				if(EmailAddress!=null && !EmailAddress.trim().equals("")){
					sendTo=EmailAddress;
				}else{
					sendTo=MailTo;
				}
				String subject="Test mail from initiate job";
				System.out.println("MailTo "+sendTo);
				Properties props = System.getProperties();
				// -- Attaching to default Session, or we could start a new one --
				props.put("mail.smtp.host", "10.20.1.233");
				Session session = Session.getDefaultInstance(props, null);
				// -- Create a new message --
				Message msg = new MimeMessage(session);
				// -- Set the FROM and TO fields --
				msg.setFrom(new InternetAddress("EmailConfig@agility.com"));
				msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(
						sendTo, false));
				// msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(MailTo, false));
				// fill message
				//messageBodyPart.setText(Constants.BODYCONTENT);

				msg.setText("Hi There is no intiate job Ref. this is test message");

				msg.setSubject(subject);

				msg.setSentDate(new Date());
				// -- Send the message --
				Transport.send(msg);
				System.out.println("Message sent OK.");
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	  	 
	
    /**
    *
    * @param userLogin_Id
    * @return
    */
    public  List<DdpCategorizationHolder> getCatagorizationHolder() {
	   
	   List<DdpCategorizationHolder> lstDdpCategorizationHolder = this.jdbcTemplate.query(Constant.DEF_AQL_SELECT_DTX_ID , new RowMapper<DdpCategorizationHolder>() {

           @Override
           public DdpCategorizationHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
        	   DdpCategorizationHolder ddpCatHolder = new DdpCategorizationHolder();
             
               ddpCatHolder.setChlId(rs.getInt("CHL_ID"));
               
               ddpCatHolder.setChlDtxId(rs.getInt("CHL_DTX_ID"));
               
               ddpCatHolder.setChlSynId(rs.getInt("CHL_SYN_ID"));
               
               ddpCatHolder.setChlRulId(rs.getInt("CHL_RUL_ID"));
               
               return ddpCatHolder;
           }
       });
       return lstDdpCategorizationHolder;
    }
	
	public DdpJobRefHolder getDdpJobRefHolderById(String jobId, String jobName) throws Exception
	{
		logger.debug("TaskUtil.getDdpJobRefHolderById(String jobId, String jobName) method invoked.");
		DdpJobRefHolder jobHolder = null;
		
		try
		{
			
			List<DdpJobRefHolder> jobHolderlist  = this.jdbcTemplate.query(Constant.SELECT_JOB_REF_HOLDER_BY_ID, new Object[] { jobId, jobName}, new RowMapper<DdpJobRefHolder>() {
		
				public DdpJobRefHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					String strJrfId = rs.getString("JRF_ID");
					String strJrfJobId = rs.getString("JRF_JOB_ID");
					Integer intJrfStatus = rs.getInt("JRF_STATUS");
					Calendar calJrfCreateDate = Calendar.getInstance();
					calJrfCreateDate.setTime(rs.getTimestamp("JRF_CREATED_DATE"));
					Calendar calJrfModifiedDate = Calendar.getInstance();
					calJrfModifiedDate.setTime(rs.getTimestamp("JRF_MODIFIED_DATE"));
					
					DdpJobRefHolderPK ddpJobRefHolderKey = new DdpJobRefHolderPK(strJrfId, strJrfJobId, intJrfStatus, calJrfCreateDate, calJrfModifiedDate);
					DdpJobRefHolder ddpJobRefHolder = new DdpJobRefHolder();
					ddpJobRefHolder.setId(ddpJobRefHolderKey);
	
					return ddpJobRefHolder;
		           }
			});
	
	
			if(jobHolderlist.isEmpty())
			{
				logger.info("TaskUtil.getDdpJobRefHolderById(String jobId, String jobName) - No record found for Job [{}].",jobHolderlist.get(0).getId().getJrfJobId());
			}
			else if(jobHolderlist.size() == 1)
			{
				jobHolder = jobHolderlist.get(0);
				logger.debug("TaskUtil.getDdpJobRefHolderById(String jobId, String jobName) - One record found for Job [{}] => [jobHolder.JrfId : {}, jobHolder.JrfJobId : {} ].",jobHolder.getId().getJrfJobId(),jobHolder.getId().getJrfId(),jobHolder.getId().getJrfJobId());
			}
			else
			{
				//TODO : we sort the list & then return the max jrf holder.
				//If more than one record exist, fetch the last record
				jobHolder = jobHolderlist.get(jobHolderlist.size()-1);
				//Notification should be set to remove un-wanted records 
				logger.info("TaskUtil.getDdpJobRefHolderById(String jobId, String jobName) - More than one record found for Job [{}].",jobHolder.getId().getJrfJobId());
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDdpJobRefHolderById(String jobId, String jobName) - Exception while accessing DdpJobRefHolder for Job [{}].",jobHolder.getId().getJrfJobId());
			ex.printStackTrace();
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getDdpJobRefHolderById(String jobId, String jobName) executed successfully.");
		return jobHolder;
	}
	
	
	public DdpCategorizationHolder getDdpCategorizationHoldersLastProcessedChlID(Integer catDtxId, Integer catRdtId) throws Exception
	{
		logger.debug("TaskUtil.getDdpCategorizationHoldersLastProcessedChlID(String strCatDtxId, String strCatRdtId) method invoked.");
		List<DdpCategorizationHolder> ddpCategorizationHolders = null;
		DdpCategorizationHolder ddpCategorizationHolder = null;
		
		try
		{
			ddpCategorizationHolders = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_LAST_PROCESSED_CAT_HOLDER_ID, new Object[] { catDtxId, catRdtId}, new RowMapper<DdpCategorizationHolder>() {
							
				public DdpCategorizationHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					DdpCategorizationHolder categorizationHolder = new DdpCategorizationHolder();
					
					categorizationHolder.setChlDtxId(rs.getInt("CHL_DTX_ID"));
					categorizationHolder.setChlId(rs.getInt("CHL_ID"));
					categorizationHolder.setChlRulId(rs.getInt("CHL_RUL_ID"));
					categorizationHolder.setChlSynId(rs.getInt("CHL_SYN_ID"));
					
					
					return categorizationHolder;
		           }
			});		
		
			
			if(ddpCategorizationHolders.isEmpty())
			{
				logger.info("TaskUtil.getDdpCategorizationHoldersLastProcessedChlID(String strCatDtxId, String strCatRdtId) - No record found for Cat Dtx [{}] and Cat Rtd ID [{}] to retrive LastProcessedChlID.",catDtxId,catRdtId);
			}
			else if(ddpCategorizationHolders.size() == 1)
			{
				ddpCategorizationHolder = ddpCategorizationHolders.get(0);
				logger.info("TaskUtil.getDdpCategorizationHoldersLastProcessedChlID(String strCatDtxId, String strCatRdtId) - One record found for Cat Dtx [{}] and Cat Rtd ID [{}] to retrive LastProcessedChlID.",catDtxId,catRdtId);
			}
			else
			{
				//If more than one record exist, fetch the last record
				ddpCategorizationHolder = ddpCategorizationHolders.get(ddpCategorizationHolders.size()-1);
				//Notification should be set to remove un-wanted records 
				logger.info("TaskUtil.getDdpCategorizationHoldersLastProcessedChlID(String strCatDtxId, String strCatRdtId) - More than one record found for Cat Dtx [{}] and Cat Rtd ID [{}] to retrive LastProcessedChlID.",catDtxId,catRdtId);
			}
			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDdpCategorizationHoldersLastProcessedChlID(String strCatDtxId, String strCatRdtId) - Exception while accessing DdpCategorizationHolder to retrive LastProcessedChlID for Cat Dtx [{}] and Cat Rtd ID [{}].",catDtxId,catRdtId);
			ex.printStackTrace();
			throw new Exception(ex);
		}
		
		logger.debug("TaskUtil.getDdpCategorizationHoldersLastProcessedChlID(String strCatDtxId, String strCatRdtId) executed successfully.");
		return ddpCategorizationHolder;
	}
	
	public DdpCategorizedDocs getDdpCategorizedDocsLastProcessedCatID()
	{
		
		logger.debug("TaskUtil.getDdpCategorizedDocsLastProcessedCatID() method invoked.");
		DdpCategorizedDocs ddpCategorizedDocs = null;
		
		try
		{			
			Integer intCatId  = this.jdbcTemplate.queryForObject(Constant.DEF_SQL_SELECT_LAST_PROCESSED_CAT_DOCS_ID, Integer.class);
			
			ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(intCatId-1);
		    			
		}
		catch(Exception ex)
		{
			logger.error("TaskUtil.getDdpCategorizedDocsLastProcessedCatID() - Exception while accessing Max(Cat_Id) for DdpCategorizedDocs.");
		}
		
		logger.debug("TaskUtil.getDdpCategorizedDocsLastProcessedCatID() executed successfully.");
		return ddpCategorizedDocs;
	}
	
	
	public int sendMail(String smtpAddress,String toAddress,String ccAddress,String fromAddress,String subject,String body,File attchSource) {
		
		logger.info("TaskUtil.sendMail() - Method invoked");
		int mailResponse = -1;
		
		try {
			
						
			Properties props = System.getProperties();
			props.put("mail.smtp.host",smtpAddress);
			// -- Attaching to default Session, or we could start a new one --
			Session session = Session.getDefaultInstance(props, null);
			
			// -- Create a new message --
			Message msg = new MimeMessage(session);
			// -- Set the FROM and TO fields --
			msg.setFrom(new InternetAddress(fromAddress));
			
			if (toAddress != null) {
				
				toAddress = toAddress.replace(";", ",");
//				StringTokenizer toAddressTokens = new StringTokenizer(toAddress, ";");
//				InternetAddress[] addressTo = new InternetAddress[toAddressTokens
//						.countTokens()];
//				int j = 0;
//				while (toAddressTokens.hasMoreTokens()) {
//					addressTo[j] = new InternetAddress(toAddressTokens.nextToken());
//					j++;
//				}
				msg.setRecipients(Message.RecipientType.TO,InternetAddress.parse(toAddress, false));
				
				
			} else {
				return mailResponse;
			}
			
			
			if (ccAddress != null && !ccAddress.isEmpty()) {
				
				ccAddress = ccAddress.replace(";", ",");
				
//				StringTokenizer ccAddressTokens = new StringTokenizer(ccAddress, ";");
//				InternetAddress[] addressCc = new InternetAddress[ccAddressTokens
//						.countTokens()];
//				int k = 0;
//				while (ccAddressTokens.hasMoreTokens()) {
//					addressCc[k] = new InternetAddress(ccAddressTokens.nextToken());
//					k++;
//				}
				
//				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(addressCc, false));
				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(ccAddress, false));
				
			}
				
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setContent(body.toString(),"text/html");
//			messageBodyPart.setContent(content,"text/plain; charset=\"utf-8\"");
			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart);
			
			if (attchSource != null) {
	//			// Part two is attachment
				for (File file : attchSource.listFiles()) {
					
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(file);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(file.getName());
					multipart.addBodyPart(messageBodyPart);
					
				}
			}
			
			mailResponse = 1;
			
			msg.setContent(multipart);
			
			if(env.getProperty("mail.evn").equalsIgnoreCase("UAT"))
				subject = subject+"- UAT";
			msg.setSubject(subject.toString());
			// msg.setText(body);
			// -- Set some other header information --

			msg.setSentDate(new Date());
			// -- Send the message --
			if (mailResponse == 1) {
				Transport.send(msg);
				logger.info("TaskUtils - sendMail is send to "+toAddress);
			}
			
			
		} catch (Exception ex) {
			logger.error("TaskUtil.sendMail() - Unable to send mail due following error message"+ex.getMessage(), ex);
		}
		
		
		return mailResponse;
		
	} 
 
	/**
	 * Method used for sending the email.
	 * 
	 * @param smtpAddress
	 * @param toAddress
	 * @param ccAddress
	 * @param bbcAddress
	 * @param fromAddress
	 * @param replayTo
	 * @param subject
	 * @param body
	 * @param attchSource
	 * @return
	 */
	public int sendMail(String smtpAddress,String toAddress,String ccAddress,String bbcAddress,String fromAddress,String replyToAddress,
						String subject,String body,File attchSource) {
		
		logger.info("TaskUtil.sendMail() - Method invoked");
		int mailResponse = -1;
		
		try {
			
						
			Properties props = System.getProperties();
			props.put("mail.smtp.host",smtpAddress);
			// -- Attaching to default Session, or we could start a new one --
			Session session = Session.getDefaultInstance(props, null);
			
			// -- Create a new message --
			Message msg = new MimeMessage(session);
			// -- Set the FROM and TO fields --
			msg.setFrom(new InternetAddress(fromAddress));
			
			if (toAddress != null) {
				
				toAddress = toAddress.replace(";", ",");
//				StringTokenizer toAddressTokens = new StringTokenizer(toAddress, ";");
//				InternetAddress[] addressTo = new InternetAddress[toAddressTokens
//						.countTokens()];
//				int j = 0;
//				while (toAddressTokens.hasMoreTokens()) {
//					addressTo[j] = new InternetAddress(toAddressTokens.nextToken());
//					j++;
//				}
				msg.setRecipients(Message.RecipientType.TO,InternetAddress.parse(toAddress, false));
				
				
			} else {
				return mailResponse;
			}
			
			
			if (bbcAddress != null && !bbcAddress.isEmpty()) {
				
				bbcAddress = bbcAddress.replace(";", ",");
				
//				StringTokenizer bccAddressTokens = new StringTokenizer(bbcAddress, ";");
//				InternetAddress[] addressBCc = new InternetAddress[bccAddressTokens
//						.countTokens()];
//				int k = 0;
//				while (bccAddressTokens.hasMoreTokens()) {
//					addressBCc[k] = new InternetAddress(bccAddressTokens.nextToken());
//					k++;
//				}
				
//				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(addressCc, false));
				msg.setRecipients( javax.mail.Message.RecipientType.BCC,InternetAddress.parse(bbcAddress, false));
				
			}
				
			if (ccAddress != null && !ccAddress.isEmpty()) {
				
				ccAddress = ccAddress.replace(";", ",");
				
//				StringTokenizer ccAddressTokens = new StringTokenizer(ccAddress, ";");
//				InternetAddress[] addressCc = new InternetAddress[ccAddressTokens
//						.countTokens()];
//				int k = 0;
//				while (ccAddressTokens.hasMoreTokens()) {
//					addressCc[k] = new InternetAddress(ccAddressTokens.nextToken());
//					k++;
//				}
				
//				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(addressCc, false));
				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(ccAddress, false));
				
			}
			
			if (replyToAddress != null && !replyToAddress.isEmpty()) {
				
				StringTokenizer replyToAddressTokens = new StringTokenizer(replyToAddress, ";");
				InternetAddress[] addressReplyTo = new InternetAddress[replyToAddressTokens
						.countTokens()];
				int k = 0;
				while (replyToAddressTokens.hasMoreTokens()) {
					addressReplyTo[k] = new InternetAddress(replyToAddressTokens.nextToken());
					k++;
				}
				msg.setReplyTo(addressReplyTo);
			}
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setContent(body.toString(),"text/html");
//			messageBodyPart.setContent(content,"text/plain; charset=\"utf-8\"");
			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart);
			
			if (attchSource != null) {
	//			// Part two is attachment
				for (File file : attchSource.listFiles()) {
					
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(file);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(file.getName());
					multipart.addBodyPart(messageBodyPart);
					
				}
			}
			
			mailResponse = 1;
			
			msg.setContent(multipart);
			
			if(env.getProperty("mail.evn").equalsIgnoreCase("UAT"))
				subject = subject+"- UAT";
			
			msg.setSubject(subject.toString());
			// msg.setText(body);
			// -- Set some other header information --

			msg.setSentDate(new Date());
			// -- Send the message --
			if (mailResponse == 1) {
				Transport.send(msg);
				logger.info("TaskUtils - sendMail is send to "+toAddress);
			}
			
			
		} catch (Exception ex) {
			logger.error("TaskUtil.sendMail() - Unable to send mail due following error message"+ex.getMessage(), ex);
		}
		
		
		return mailResponse;
		
	}

	
	public int sendMailForMultiAed(DdpCommEmail ddpCommEmails, File tmpFile,String smtpAddres,String mailSubject,String mailBody,String mailFrom,String ruleType,String branchCCMailId) {
		logger.info("TaskUtil.sendMailForMultiAed(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres) Method Invoked.");
		int attachFlag =-1;
		try {
				String toAddress ="";
				String fromAddress ="";
				String ccAddress ="";
				//String fileLocation = "";
				String bccAddress = "";
				String replyToAddress = "";
				
				StringBuffer strSubject = new StringBuffer();
				StringBuffer strbody = new StringBuffer();
				if(null!=ddpCommEmails)
				{
					toAddress = ddpCommEmails.getCemEmailTo();
					fromAddress =  ddpCommEmails.getCemEmailFrom();
					if(null==fromAddress || fromAddress.equals("")){
						//Read from the propery file, need to fine tue.
						fromAddress = mailFrom;
					}
					logger.info("TaskUtil.sendMailForMultiAed(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres) : FromAddress :"+fromAddress);
					
					ccAddress = ddpCommEmails.getCemEmailCc();
					if(null==ddpCommEmails.getCemEmailSubject()){
						strSubject.append(mailSubject);
					}else{
						strSubject.append(ddpCommEmails.getCemEmailSubject());	
					}
					
					if(null==ddpCommEmails.getCemEmailBody()){
						strbody.append(mailBody);
					}else{
						strbody.append(ddpCommEmails.getCemEmailBody());	
					}
					
					if (null != ddpCommEmails.getCemEmailBcc() && !ddpCommEmails.getCemEmailBcc().isEmpty()) 
						bccAddress = ddpCommEmails.getCemEmailBcc().replaceAll(";", ",");
					
					if (null != ddpCommEmails.getCemEmailReplyTo() && !ddpCommEmails.getCemEmailReplyTo().isEmpty())
						replyToAddress = ddpCommEmails.getCemEmailReplyTo().replaceAll(";", ",");
					
					/*//Get the files that need to collected and send as email with attachment
					fileLocation =  ddpCommEmails.getCemFtpLocation();

					if(null != ddpCommEmails.getCemFtpLocation()){
						fileLocation =  ddpCommEmails.getCemFtpLocation();
					}else if(null != ddpCommEmails.getCemUncPath()){
						fileLocation =  ddpCommEmails.getCemUncPath();
					}else{
						fileLocation=uncPath;
					}*/
				}else{
					//This email address should be get from the property files
					toAddress =resToAddress;
				}
				Properties props = System.getProperties();
				props.put("mail.smtp.host",smtpAddres);
				// -- Attaching to default Session, or we could start a new one --
				Session session = Session.getDefaultInstance(props, null);
				
				
				// -- Create a new message --
				Message msg = new MimeMessage(session);
				// -- Set the FROM and TO fields --
				msg.setFrom(new InternetAddress(fromAddress));
				//--- To Address
				toAddress = toAddress.replaceAll(";", ",");
				logger.info("TaskUtil.sendMailForMultiAed(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : ToAddress: "+toAddress);
//				StringTokenizer toAddressTokens = new StringTokenizer(toAddress, ",");
//				InternetAddress[] addressTo = new InternetAddress[toAddressTokens
//						.countTokens()];
//				int j = 0;
//				while (toAddressTokens.hasMoreTokens()) {
//					addressTo[j] = new InternetAddress(toAddressTokens.nextToken());
//					j++;
//				}
				msg.setRecipients(Message.RecipientType.TO,InternetAddress.parse(toAddress, false));
				
				//CC address
				
				if(ccAddress != null) 
				{
					if(branchCCMailId !=null && ! branchCCMailId.equalsIgnoreCase(""))
					{
						if(ccAddress.equals(""))
						{
							ccAddress=branchCCMailId;
						}
						else
							ccAddress=ccAddress.concat(","+branchCCMailId);
					}
					ccAddress = ccAddress.replaceAll(";",",");
					if (ccAddress.length() > 0)
						ccAddress = getValidEmailAddress(ccAddress);

					logger.info("TaskUtil.sendMailForMultiAed(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : CCAddress: "+ccAddress);
					msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(ccAddress, false));
				}
				else
				{
					if(branchCCMailId !=null && ! branchCCMailId.equalsIgnoreCase(""))
					{
						ccAddress=branchCCMailId;
						logger.info("TaskUtil.sendMailForMultiAed(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : CCAddress: "+ccAddress);
						InternetAddress[] addressCC=new InternetAddress[1];
						addressCC[0]=new InternetAddress(ccAddress);
						msg.setRecipients( javax.mail.Message.RecipientType.CC,addressCC);
					}
				}
				
				//BCC address setting
				if (!bccAddress.isEmpty()) {
					
//					StringTokenizer bccAddressTokens = new StringTokenizer(bccAddress, ";");
//					InternetAddress[] addressBcc = new InternetAddress[bccAddressTokens
//							.countTokens()];
//					int k = 0;
//					while (bccAddressTokens.hasMoreTokens()) {
//						addressBcc[k] = new InternetAddress(bccAddressTokens.nextToken());
//						k++;
//					}
					msg.setRecipients( javax.mail.Message.RecipientType.BCC,InternetAddress.parse(bccAddress, false));
				}
				
				//Reply To
				if (!replyToAddress.isEmpty()) {
					
					StringTokenizer replyToAddressTokens = new StringTokenizer(replyToAddress, ",");
					InternetAddress[] addressReplyTo = new InternetAddress[replyToAddressTokens
							.countTokens()];
					int k = 0;
					while (replyToAddressTokens.hasMoreTokens()) {
						addressReplyTo[k] = new InternetAddress(replyToAddressTokens.nextToken());
						k++;
					}
					msg.setReplyTo(addressReplyTo);
				}
				
				MimeBodyPart messageBodyPart = new MimeBodyPart();
				messageBodyPart.setContent(strbody.toString(),"text/html");
//				messageBodyPart.setContent(content,"text/plain; charset=\"utf-8\"");
				Multipart multipart = new MimeMultipart();
				multipart.addBodyPart(messageBodyPart);
//				// Part two is attachment
				for (File file : tmpFile.listFiles()) {
					
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(file);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(file.getName());
					multipart.addBodyPart(messageBodyPart);
					attachFlag = 1;
				}

				msg.setContent(multipart);
				
				if(env.getProperty("mail.evn").equalsIgnoreCase("UAT"))
					strSubject = strSubject.append("- UAT");
				
				msg.setSubject(strSubject.toString());
				// msg.setText(body);
				// -- Set some other header information --

				msg.setSentDate(new Date());
				// -- Send the message --
				if(!(attachFlag==-1 && ruleType.equalsIgnoreCase("MULTI_AED_RULE") )  )
				{
				/*	DdpCategorizedDocs categorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(catId);
					if(categorizedDocs.getCatStatus() != 1)
					{*/
						Transport.send(msg);
						logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : mail sent with attachment given rule ");
					/*}*/
				}
		        if(attachFlag == -1)
		        {
		        	attachFlag = 2;
		        	logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : "+tmpFile.getName()+" not found in location");
		        }
		        return attachFlag;
	
			} catch (Exception ex) {
				logger.error("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  :"+ex.getMessage());
				logger.info("TaskUtil.sendMail(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  - Changing the status to -1 due to exception : "+ tmpFile.getName());
				ex.printStackTrace();
			return -1;
		}

	}
	
	/**
	 * Method used for sending the Consolidated AED Mails based on the Destination code.  
	 * From Branch finding the Company & Region.
	 * 
	 * @param ddpMultiEmails
	 * @param ddpDmsDocsDetail
	 * @param tmpFile
	 * @param smtpAddres
	 * @param mailSubject
	 * @param mailBody
	 * @param mailFrom
	 * @param ruleType
	 * @param branchCCMailId
	 * @return
	 */
	public int sendMailForMultiAedToMultiple(List<DdpMultiEmails> ddpMultiEmails, DdpDmsDocsDetail ddpDmsDocsDetail,File tmpFile,
			String smtpAddres,String mailSubject,String mailBody,String mailFrom,String ruleType,String branchCCMailId) {
		
		logger.info("TaskUtil.sendMailForMultiAedToMultiple(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres) Method Invoked.");
		int attachFlag =-1;
		try {
			
			if (ddpDmsDocsDetail.getDddBranchDestination() != null && !ddpDmsDocsDetail.getDddBranchDestination().isEmpty()) {
				
				DdpBranch destBranch = ddpBranchService.findDdpBranch(ddpDmsDocsDetail.getDddBranchDestination());
					
				 if (destBranch == null) {
					 sendMailByDevelopers("For Consolidated AED rule for Sending destination emails, Destination Branch is :"+ddpDmsDocsDetail.getDddBranchDestination()+" no barnch is found. "
					 		+ "Please add the Branch in DDP_BRANCH TABLE", "TaskUtil.sendMailForMultiAedToMultiple() - Destination Branch :"+ddpDmsDocsDetail.getDddBranchDestination()+" no barnch is found.");
					 return attachFlag;
				 }
				 
				 String destCompanyCode = destBranch.getBrnCompnayCode().trim();
				 
					for(DdpMultiEmails ddpMultiEmail:ddpMultiEmails)
					{
						//Sending the mails based on the destination company.
						if(ddpMultiEmail.getMesDestCompany() != null)
						{
							String destinationCompany = ddpMultiEmail.getMesDestCompany().trim();
							if(destCompanyCode.equalsIgnoreCase(destinationCompany))
							{
								attachFlag = sendMailForMultiAedToDestination(ddpMultiEmail,tmpFile,smtpAddres,mailSubject,mailBody,mailFrom,ruleType,branchCCMailId);
							}
						}
						
						//Sending based on the destination region
						if(ddpMultiEmail.getMesDestRegion() != null)
						{
							DdpCompany company = ddpCompanyService.findDdpCompany(destCompanyCode);
							
							if (company == null)
								sendMailByDevelopers("For Consolidated AED rule for Sending destination emails, Destination Company is :"+destCompanyCode+" no barnch is found. "
										+ "Please add the Branch in DDP_COMPANY TABLE", "TaskUtil.sendMailForMultiAedToMultiple() - Destination Company is "+destCompanyCode+" no company is found.");
							
							if(ddpMultiEmail.getMesDestRegion().equalsIgnoreCase(company.getComRegion()) || ddpMultiEmail.getMesDestRegion().equalsIgnoreCase("All"))
							{
								attachFlag = sendMailForMultiAedToDestination(ddpMultiEmail,tmpFile,smtpAddres,mailSubject,mailBody,mailFrom,ruleType,branchCCMailId);
							}
						}
					}
			} else {
				sendMailByDevelopers("For Consolidated AED rule for Sending destination emails, Destination Branch is empty/null for DMS_DOC_DETAIL - "+ddpDmsDocsDetail.getDddId(), "TaskUtil.sendMailForMultiAedToMultiple() - Destination Branch is empty/null for DMSDOCDETAIL - "+ddpDmsDocsDetail.getDddId());
			}
		}catch(Exception ex)
		{
			logger.error("TaskUtil.sendMailForMultiAedToMultiple : Error Occured while performing Action",ex);
		}
		return attachFlag;
	}
	
	
	/**
	 * Method used for sending the Consolidated AED Mail based on Destination company code.
	 * 
	 * @param ddpMultiEmail
	 * @param tmpFile
	 * @param smtpAddres
	 * @param mailSubject
	 * @param mailBody
	 * @param mailFrom
	 * @param ruleType
	 * @param branchCCMailId
	 * @return
	 */
	public int sendMailForMultiAedToDestination(DdpMultiEmails ddpMultiEmail, File tmpFile,String smtpAddres,String mailSubject,
			String mailBody,String mailFrom,String ruleType,String branchCCMailId) {
		
		logger.info("TaskUtil.sendMailForMultiAedToMultiple(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres) Method Invoked.");
		int attachFlag =-1;
		try {
				String toAddress ="";
				String fromAddress ="";
				String ccAddress ="";
				//String fileLocation = "";
				String bccAddress = "";
				String replyToAddress = "";
				
				String strSubject = "";
				String strBody = "";
				
				if(ddpMultiEmail != null)
				{
					toAddress = ddpMultiEmail.getMesEmailTo();
					fromAddress = ddpMultiEmail.getMesEmailFrom();
					if(fromAddress == null || fromAddress.equals(""))
							fromAddress = mailFrom;
					ccAddress = ddpMultiEmail.getMesEmailCc();
					if(ccAddress != null) 
					{
						if(branchCCMailId !=null && ! branchCCMailId.equalsIgnoreCase(""))
						{
							if(ccAddress.equals(""))
							{
								ccAddress=branchCCMailId;
							}
							else
								ccAddress=ccAddress.concat(","+branchCCMailId);
						}
						ccAddress = ccAddress.replaceAll(";",",");
						if (ccAddress.length() > 0)
							ccAddress = getValidEmailAddress(ccAddress);
					}
					else
					{
						if(branchCCMailId !=null && ! branchCCMailId.equalsIgnoreCase(""))
						{
							ccAddress=branchCCMailId;
							logger.info("TaskUtil.sendMailForMultiAedToMultiple(DdpCommEmail ddpCommEmails, String fileName,String smtpAddres)  : CCAddress: "+ccAddress);
							InternetAddress[] addressCC=new InternetAddress[1];
							addressCC[0]=new InternetAddress(ccAddress);
							ccAddress = addressCC.toString();
						}
					}
					
					if(ddpMultiEmail.getMesEmailSubject() == null)
						strSubject = mailSubject;
					else
						strSubject = ddpMultiEmail.getMesEmailSubject();
					if(ddpMultiEmail.getMesEmailBody() == null)
						strBody = mailBody;
					else
						strBody = ddpMultiEmail.getMesEmailBody();
					if(ddpMultiEmail.getMesEmailBcc() != null && !ddpMultiEmail.getMesEmailBcc().isEmpty())
						bccAddress = ddpMultiEmail.getMesEmailBcc().replaceAll(";",",");
					if(ddpMultiEmail.getMesEmailReply() != null && !ddpMultiEmail.getMesEmailReply().isEmpty())
						replyToAddress = ddpMultiEmail.getMesEmailReply().replaceAll(";",",");
					
					attachFlag = sendMail(smtpAddres, toAddress, ccAddress, bccAddress, fromAddress, replyToAddress, strSubject, strBody, tmpFile);
				}
		}catch(Exception ex)
		{
			logger.error("TaskUtil.sendMailForMultiAedToMultiple : Error Occured while performing Action",ex);
		}
		return attachFlag;
	}
	
	/**
	 * Method used for sending the mail to the customers if any failure in the system.
	 * 
	 * @param stackTrace
	 * @param reason
	 */
	public void sendMailByDevelopers(String stackTrace,String reason ) {
		
		logger.info("TaskUtil.sendMailByDevelopers() - Method invoked");
		//int mailResponse = -1;
		
		try {
			
			String typeOfEnv = env.getProperty("mail.evn");			
			Properties props = System.getProperties();
			props.put("mail.smtp.host",env.getProperty("mail.smtpAddress"));
						
			// -- Attaching to default Session, or we could start a new one --
			Session session = Session.getDefaultInstance(props, null);
			
			// -- Create a new message --
			Message msg = new MimeMessage(session);
			// -- Set the FROM and TO fields --
			msg.setFrom(new InternetAddress(env.getProperty("mail.fromAddress")));
			
			String toAddress = env.getProperty("developer.issue.toaddress");
			String ccAddress = env.getProperty("developer.issue."+typeOfEnv+".ccaddress");
			if (toAddress != null) {
				
				toAddress = toAddress.replace(";", ",");
//				StringTokenizer toAddressTokens = new StringTokenizer(toAddress, ";");
//				InternetAddress[] addressTo = new InternetAddress[toAddressTokens
//						.countTokens()];
//				int j = 0;
//				while (toAddressTokens.hasMoreTokens()) {
//					addressTo[j] = new InternetAddress(toAddressTokens.nextToken());
//					j++;
//				}
				msg.setRecipients(Message.RecipientType.TO,InternetAddress.parse(toAddress, false));
				
				
			}
			
			
			if (ccAddress != null) {
				
				ccAddress = ccAddress.replace(";", ",");
				
//				StringTokenizer ccAddressTokens = new StringTokenizer(ccAddress, ";");
//				InternetAddress[] addressCc = new InternetAddress[ccAddressTokens
//						.countTokens()];
//				int k = 0;
//				while (ccAddressTokens.hasMoreTokens()) {
//					addressCc[k] = new InternetAddress(ccAddressTokens.nextToken());
//					k++;
//				}
				
//				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(addressCc, false));
				msg.setRecipients( javax.mail.Message.RecipientType.CC,InternetAddress.parse(ccAddress, false));
				
			}
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
			
			String body = env.getProperty("developer.issue.mail.body");
			body = body.replace("%%ENV%%",typeOfEnv);
			body = body.replaceAll("%%STACKTRACE%%", stackTrace);
			body = body.replace("%%CURRENTTIME%%", dateFormat.format(new Date()));
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setContent(body,"text/html");
//			messageBodyPart.setContent(content,"text/plain; charset=\"utf-8\"");
			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart);
			
		/*	if (attchSource != null) {
	//			// Part two is attachment
				for (File file : attchSource.listFiles()) {
					
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(file);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(file.getName());
					multipart.addBodyPart(messageBodyPart);
					
				}
			}
		*/	
			//mailResponse = 1;
			
			msg.setContent(multipart);
			String subject = env.getProperty("developer.issue.mail.subject");
			subject = subject.replaceAll("%%ENV%%",typeOfEnv);
			subject = subject.replace("%%REASON%%", reason);
			if(env.getProperty("mail.evn").equalsIgnoreCase("UAT"))
				subject = subject + "- UAT";
			msg.setSubject(subject.toString());
			// msg.setText(body);
			// -- Set some other header information --

			msg.setSentDate(new Date());
			// -- Send the message --
			//if (mailResponse == 1) {
				Transport.send(msg);
				logger.info("TaskUtils - sendMailByDevelopers is send to "+toAddress);
			//}
			
			
		} catch (Exception ex) {
			logger.error("TaskUtil.sendMailByDevelopers() - Unable to send mail due following error message"+ex.getMessage(), ex);
		}
	}
	
	/**
	 * Method used for getting the list of email address.
	 * 
	 * @param emailAddress
	 * @return
	 */
	public  String getValidEmailAddress(String emailAddress) {
		
		if (emailAddress != null && !emailAddress.isEmpty()) {
			String emailString = "";
			String[] emails = emailAddress.split(",");
			for (String email : emails) {
				if (isValidEmailAddress(email.trim())) 
					emailString += email.trim() +",";
			}
			if (emailString.length() > 1)
				emailAddress = emailString.substring(0, emailString.length()-1);
		}
		return emailAddress;
	}
	
	/**
	 * Method is used for checking the valid email address or not.
	 * 
	 * @param email
	 * @return
	 */
	public  boolean isValidEmailAddress(String email) {
	   boolean result = true;
	   try {
	      InternetAddress emailAddr = new InternetAddress(email);
	      emailAddr.validate();
	   } catch (AddressException ex) {
	      result = false;
	   }
	   return result;
	}
	
	public String callSoapWebService(String base64Binary, String filename, String passphrase){
		
		logger.info("callSoapWebService(String base64Binary, String filename, String passphrase) method invoked. passphrase: "+passphrase+" for File: "+filename);
		String soapEndpointUrl = env.getProperty("document.digitalsign.soapendpointurl");
        String soapAction = env.getProperty("document.digitalsign.soapaction");
        String myNamespaceURI = env.getProperty("document.digitalsign.mynamespaceuri");
        
    	String responseBase64Binary = null;
        try {
            // Create SOAP Connection
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            
            SOAPMessage soapMessage = createSOAPRequest(soapAction,myNamespaceURI,base64Binary,filename,passphrase);
            // Send SOAP Message to SOAP Server
            SOAPMessage soapResponse = soapConnection.call(soapMessage, soapEndpointUrl);

            SOAPBody responseSoapBody = soapResponse.getSOAPBody();
            SOAPFault soapFault = responseSoapBody.getFault();
            
            if(soapFault != null)
            {
            	logger.error(soapFault.getTextContent());
            	sendMailByDevelopers(soapFault.getTextContent(), "Unable to Receive response from PDF Digital Sign Web Service");
            }
            else{

            	responseBase64Binary = responseSoapBody.getElementsByTagName("ByteResult").item(0).getTextContent();
            }            
            soapConnection.close();
        } catch (Exception e) {
            System.err.println("\nError occurred while sending SOAP Request to Server!\nMake sure you have the correct endpoint URL and SOAPAction!\n");
            logger.error(e.getMessage());
        }
		return responseBase64Binary;
	}
	 private static SOAPMessage createSOAPRequest(String soapAction,String myNamespaceURI,String base64Binary, String filename, String passphrase) throws Exception {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();

        createSoapEnvelope(soapMessage,myNamespaceURI,base64Binary,filename,passphrase);

        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", soapAction);

        soapMessage.saveChanges();
        logger.info("createSOAPRequest() Excecuted for File Name: "+filename);
        return soapMessage;
    }
	 private static void createSoapEnvelope(SOAPMessage soapMessage,String myNamespaceURI,String base64Binary, String filename, String passphrase) throws SOAPException {
        SOAPPart soapPart = soapMessage.getSOAPPart();

        String myNamespace = "myNamespace";

        // SOAP Envelope
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration(myNamespace, myNamespaceURI);
        // SOAP Body
        SOAPBody soapBody = envelope.getBody();
        
//	        SOAPElement soapBodyElem = soapBody.addChildElement("String", myNamespace);
//	        SOAPElement soapBodyElem1 = soapBodyElem.addChildElement("Passphrase", myNamespace);
//	        soapBodyElem1.addTextNode("portugalphrase");
        
        SOAPElement soapBodyElem = soapBody.addChildElement("Byte", myNamespace);
        SOAPElement soapBodyElem1 = soapBodyElem.addChildElement("PDF", myNamespace);
        soapBodyElem1.addTextNode(base64Binary);
		SOAPElement soapBodyElem2 = soapBodyElem.addChildElement("PdfName", myNamespace);
		soapBodyElem2.addTextNode(filename);
		SOAPElement soapBodyElem3 = soapBodyElem.addChildElement("Passphrase", myNamespace);
		soapBodyElem3.addTextNode(passphrase);
    }
	 public static String convertToBase64(File originalFile){

        String encodedBase64 = null;
        try {
            FileInputStream fileInputStreamReader = new FileInputStream(originalFile);
            byte[] bytes = new byte[(int)originalFile.length()];
            fileInputStreamReader.read(bytes);
            encodedBase64 = new String(Base64.encodeBase64(bytes));
            fileInputStreamReader.close();
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
        	logger.error(e.getMessage());
        }
        logger.info("convertToBase64() Excecuted for File Name: "+originalFile.getName());
        return encodedBase64;
    }
	 public static boolean storeSignedPDF(String path, String content)
    {
    	boolean isFileCopied = false;
    	try {
			FileOutputStream fileOutputStreamWriter = new FileOutputStream(new File(path));
			try {
				fileOutputStreamWriter.write(Base64.decodeBase64(content));
				isFileCopied = true;
				fileOutputStreamWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    	return isFileCopied;
    }

}
