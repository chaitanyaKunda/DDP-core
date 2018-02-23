package com.agility.ddp.core.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import com.agility.ddp.core.rule.Rule;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizationHolder;
import com.agility.ddp.data.domain.DdpCategorizationHolderService;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpDmsDocsTxn;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.agility.ddp.data.domain.DdpJobRefHolderService;
import com.agility.ddp.data.domain.DmsDdpSynService;



//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class DdpUpdateMetadataTask implements Task  
{
////	@Autowired
//	Environment env;
	
	private static final Logger logger = LoggerFactory.getLogger(DdpUpdateMetadataTask.class);

	private final String strJobId = DdpUpdateMetadataTask.class.getCanonicalName();
	
	@Autowired
	private TaskUtil taskUtil;
	
	private PlatformTransactionManager transactionManager;
	
	@Autowired
    DmsDdpSynService dmsDdpSynService;
	
	@Autowired
	DdpCategorizationHolderService ddpCategorizationHolderService;
	
	@Autowired
	DdpJobRefHolderService ddpJobRefHolderService;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Override
	public void execute() {
		execute(strJobId);
	}

	private List<Rule> ruleList = new ArrayList<Rule>();	
	/************************************************
	 * 
	 * To load Rule class instances after bean initiated
	 *   
	 * 
	 * @param 
	 * @return
	 *************************************************/
	
	public void setRuleDataAsList(List<Rule> ruleDataAsList) throws Exception 
	{
		logger.debug("DdpUpdateMetadataTask.setRuleDataAsList(List ruleDataAsList) method invoked.");
		
		ruleList.clear();
		ruleList.addAll(ruleDataAsList);
		
		logger.debug("DdpUpdateMetadataTask.setRuleDataAsList(List ruleDataAsList) executed successfully.");
	}
	
	public void setPlatformTransactionManager (PlatformTransactionManager transactionManager) 
	{
		this.transactionManager = transactionManager;
	}
	
	@Override
	public void execute(JobExecutionContext context) {
		execute(context.getJobDetail().getKey().getGroup()+"."+context.getJobDetail().getKey().getName());
		
	}

	
	@Override
	public void execute(String jobName) 
	{
		logger.info("DdpUpdateMetadataTask.execute() method invoked.");
		try 
		{
			logger.debug("DdpUpdateMetadataTask.execute() Waiting for SQL execution.");
            // sleep for ten minutes for triggers to file....
           // Thread.sleep(180L * 1000L);
			boolean isJobRefHolderUpdated = false;
			//List<DdpCategorizationHolder> ddpCategorizationHolders = new ArrayList<DdpCategorizationHolder>();
			DdpJobRefHolder jobHolder = taskUtil.getDdpJobRefHolder(jobName);
			//String jfrId = "";
			
			List<DdpCategorizationHolder> ddpCategorizationHolderlist = null;
			
			if(null == jobHolder)//No records found in DdpJobRefHolder
			{
				//Send email
				/*DdpCategorizationHolder ddpCategorizationHolder = taskUtil.getMaxChlIdDdpCategarizationHolder();
				if(ddpCategorizationHolder != null)
				{
					ddpCategorizationHolderlist = ddpCategorizationHolderService.findAllDdpCategorizationHolders();
				}*/
				DdpCategorizedDocs ddpCategorizedDoc = taskUtil.getMaxCatIdDdpCategorizedDocs();
				if(ddpCategorizedDoc == null)// no records in DDP_CATEGORIZATION_HOLDER. i.e. running for first time
				{
					logger.info("DdpUpdateMetadataTask.execute(String jobName) : No record found in DDP_CATEGORIZATION_DOCS table to process for job [{}] i.e. running for first time and processing records in DDP_CATEGORIZATION_HOLDER table.",jobName);
					//TODO:need to compare all categarization holder & ddp dms docs detail table. SELECT * FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_DTX_ID NOT IN (SELECT DDD_DTX_ID FROM DDP_DMS_DOCS
					ddpCategorizationHolderlist = fetchCategorizationHolder();
					/*DdpCategorizationHolder ddpCategorizationHolder = taskUtil.getMaxChlIdDdpCategarizationHolder();
					if(ddpCategorizationHolder != null)
					{
						//TODO:need to compare all categarization holder & ddp dms docs detail table. SELECT * FROM DDP_CATEGORIZATION_HOLDER WHERE CHL_DTX_ID NOT IN (SELECT DDD_DTX_ID FROM DDP_DMS_DOCS
						ddpCategorizationHolderlist = ddpCategorizationHolderService.findAllDdpCategorizationHolders();
						logger.info("DdpUpdateMetadataTask.execute(String jobName) : Records found in DDP_CATEGORIZATION_HOLDER table to process for job [{}] and processing them for first time.",jobName);
					}
					else
					{
						logger.info("DdpUpdateMetadataTask.execute(String jobName) : No records found in DDP_CATEGORIZATION_HOLDER and DDP_CATEGORIZATION_DOCS tables to process for job [{}]. System is running for first time and no record has been categorized yet",jobName);
					}*/
				}
				else
				{
					DdpDmsDocsTxn ddpDmsDocsTxn = ddpCategorizedDoc.getCatDtxId();
					Integer catDtxId = ddpDmsDocsTxn.getDtxId();
					Integer catRdtId = ddpCategorizedDoc.getCatRdtId();
				
					DdpCategorizationHolder ddpCategorizationHolder = taskUtil.getDdpCategorizationHoldersLastProcessedChlID(catDtxId, catRdtId);
				
					if(ddpCategorizationHolder!= null)
					{
						ddpCategorizationHolderlist = taskUtil.getDdpCategorizationHoldersGreaterThanMaxCHLID(ddpCategorizationHolder);
						ddpCategorizationHolderlist = taskUtil.sortDdpCategorizationList(ddpCategorizationHolderlist);
						if(null == ddpCategorizationHolderlist || ddpCategorizationHolderlist.isEmpty() ){
						
							logger.info("DdpUpdateMetadataTask.execute(String jobName) : Zero record found in Categorization table for job [{}].",jobName);
							//throw new Exception("DdpUpdateMetadataTask.execute(String jobName) : Zero record found in Categorization table for job [{}].");
						}
					}
					else
					{
						logger.error("DdpUpdateMetadataTask.execute(String jobName) : No record found in DDP_CATEGORIZATION_HOLDER table to process for job [{}].");
					}
				}	
			}
			else//Reference ID found in DdpJobRefHolder to process records
			{
				logger.debug("DdpUpdateMetadataTask.execute(String jobName) - Job Ref ID found for Job [{}] in DdpJobRefHolder. Querying greater than Job Ref ID from  DdpCategorizationHolder",jobName);
				
				ddpCategorizationHolderlist = taskUtil.getDmsDdpCatGreaterThanMaxJobRefID(jobHolder, jobName);
				ddpCategorizationHolderlist = taskUtil.sortDdpCategorizationList(ddpCategorizationHolderlist);
				if(null == ddpCategorizationHolderlist || ddpCategorizationHolderlist.isEmpty() ){
					
					logger.info("DdpUpdateMetadataTask.execute(String jobName) : Zero record found in Categorization table for job [{}].",jobName);
					//throw new Exception("DdpUpdateMetadataTask.execute(String jobName) : Zero record found in Categorization table.");
				}
			}
				
			/*************************************************
			 * SHOULD FIND THE ROLLBACK OPTION IN SPRING
			 ***********************************************/
			//insert/update the record for the 'jobName' (JRF_JOB_ID) with first SYN_ID into JRF_ID column with STATUS as '0' - In Progress
			//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
			if(ddpCategorizationHolderlist != null && !ddpCategorizationHolderlist.isEmpty()){
				isJobRefHolderUpdated = taskUtil.updateJobRefHolder(Integer.toString(ddpCategorizationHolderlist.get(0).getChlId()), jobHolder, jobName, 0);	
				if(isJobRefHolderUpdated == false)
				{
					//TODO : need ot send the mail
					taskUtil.sendMailByDevelopers("DdpUpdateMetadataTask.execute(String jobName) : Unable to insert/update DDP_JOB_REF_HOLDER table to hold the first processing CHL_ID as JRF_ID for Job ["+jobName+"].", " inside  DdpCategorizationTask.execute(String jobName)");
					throw new Exception("DdpUpdateMetadataTask.execute(String jobName) : Unable to insert/update DDP_JOB_REF_HOLDER table to hold the first processing CHL_ID as JRF_ID for Job ["+jobName+"].");
				}
				//Fetch all the records from DDP_CATEGORIZATION_HOLDER table
				boolean isCategorizationDocsInserted = taskUtil.insertDdpCategorization(ddpCategorizationHolderlist);
				
				if(isCategorizationDocsInserted && !ddpCategorizationHolderlist.isEmpty()){
					//update the record for the 'jobName' (JRF_JOB_ID) with last CHL_ID into JRF_ID column with STATUS as '1' - Success
					//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
					jobHolder = taskUtil.getDdpJobRefHolderById(Integer.toString(ddpCategorizationHolderlist.get(0).getChlId()),jobName);
					isJobRefHolderUpdated = taskUtil.updateJobRefHolder(Integer.toString(ddpCategorizationHolderlist.get(ddpCategorizationHolderlist.size()-1).getChlId()), jobHolder, jobName, 1);
				}else if(isCategorizationDocsInserted == false){
					taskUtil.sendMailByDevelopers("DdpUpdateMetadataTask.execute(String jobName) : taskUtil.updateJobRefHolder Unable to insert DDP_Categorization_Docs table", " inside  DdpCategorizationTask.execute(String jobName)");
					throw new Exception("DdpUpdateMetadataTask.execute(String jobName) : taskUtil.updateJobRefHolder Unable to insert DDP_Categorization_Docs table");
				}else if(isJobRefHolderUpdated == false  && !ddpCategorizationHolderlist.isEmpty()){
					taskUtil.sendMailByDevelopers("DdpUpdateMetadataTask.execute(String jobName) : Unable to update DDP_JOB_REF_HOLDER table to hold the last processing SYN_ID as JRF_ID for Job ["+jobName+"].", " inside  DdpCategorizationTask.execute(String jobName)");
					throw new Exception("DdpUpdateMetadataTask.execute(String jobName) : Unable to update DDP_JOB_REF_HOLDER table to hold the last processing SYN_ID as JRF_ID for Job ["+jobName+"].");
				}else{
					logger.info("DdpUpdateMetadataTask.execute(String jobName) : taskUtil.updateJobRefHolder Unable to insert DDP_Categorization_Docs table : Zero record found in Categorization table.");
				}
			}
        } 
		catch (Exception ex) 
		{
			logger.error("DdpUpdateMetadataTask.execute(String jobName) - Exception while executing Job [{}] [Message:"+ex.getMessage()+"].",jobName);
			ex.printStackTrace();
			//TODO: need to send the mail.
			taskUtil.sendMailByDevelopers(ex.getMessage(), " inside  DdpUpdateMetadataTask.execute(String jobName)");
        }
		logger.info("DdpUpdateMetadataTask.execute() executed successfully.");

	}
	
	
	public List<DdpCategorizationHolder> fetchCategorizationHolder()  throws Exception {
		
		List<DdpCategorizationHolder> list = new ArrayList<DdpCategorizationHolder>();
		
		try {
			
			list = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_UPDATE_METADATA_FETCH, new RowMapper<DdpCategorizationHolder>() {

				@Override
				public DdpCategorizationHolder mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					
					DdpCategorizationHolder ddpCategorizationHolder = ddpCategorizationHolderService.findDdpCategorizationHolder(rs.getInt("CHL_ID"));
					return ddpCategorizationHolder;
				
				}
			});

		} catch (Exception ex) {
			logger.error("DdpUpdateMetadatTask.fetchCategorizationHolder() error occurried", ex);
			 throw new Exception(ex);
		}
		return list;
	}
	
	public boolean processChlIDRange(int minChlID, int maxChlID)
	{
		logger.info("DdpUpdateMetadatTask: processChlIDRange("+minChlID+","+maxChlID+") invoked");
		boolean isCategorizationDocsInserted = false;
		List<DdpCategorizationHolder> categorizationHolders = null;
		categorizationHolders = taskUtil.getCategorizationHoldersInRange(minChlID, maxChlID);
		try {
			isCategorizationDocsInserted = taskUtil.insertDdpCategorization(categorizationHolders);
		} catch (Exception e) {
			logger.error("Exception in processChlIDRange",e);
		}
		return isCategorizationDocsInserted;
	}
	
}
