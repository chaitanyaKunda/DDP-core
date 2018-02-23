package com.agility.ddp.core.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import com.agility.ddp.core.rule.Rule;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCommEmailService;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.agility.ddp.data.domain.DmsDdpSynService;

//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class DdpInitiateProcessTask implements Task 
{

	private static final Logger logger = LoggerFactory.getLogger(DdpInitiateProcessTask.class);	
	
	private final String strJobId = DdpInitiateProcessTask.class.getCanonicalName();
	
	@Autowired
	private TaskUtil taskUtil = new TaskUtil();
	
	private PlatformTransactionManager transactionManager;
	
//	@Autowired
//	Environment env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
    DmsDdpSynService dmsDdpSynService;
	
	@Autowired
	DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	DdpCommEmailService ddpCommEmailService;
	
	Scheduler scheduler ;
	
//	@Autowired
//	RuleJob job;
	
	@Autowired
	ApplicationContext applicationContext;
	
	
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
		logger.debug("DdpInitiateProcessTask.setRuleDataAsList(List ruleDataAsList) method invoked.");
		
		ruleList.clear();
		ruleList.addAll(ruleDataAsList);
		
		logger.debug("DdpInitiateProcessTask.setRuleDataAsList(List ruleDataAsList) executed successfully.");
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
	public void execute() 
	{
		execute(strJobId);
	}
	@Override
	public void execute(String jobName) {
		logger.info("DdpInitiateProcessTask.execute(String jobName) method invoked.");
		boolean isJobRefHolderUpdated = false;
		try 
		{
			List<DdpCategorizedDocs> ddpCategorizedDocsLst = null; 
			
			DdpJobRefHolder jobHolder = taskUtil.getDdpJobRefHolder(jobName);
			
			if(jobHolder == null)//No records found in DdpJobRefHolder and querying max(Cat_id) from DdpCategorizedDocs to process
			{
				//Send Email
				/*DdpCategorizedDocs ddpCategorizedDoc = taskUtil.getMaxCatIdDdpCategorizedDocs();
				if(ddpCategorizedDoc != null)
				{
					ddpCategorizedDocsLst = ddpCategorizedDocsService.findAllDdpCategorizedDocses(); 
				}*/
				logger.info("DdpInitiateProcessTask.execute(String jobName) - No records found in DdpJobRefHolder for Job [{}] and querying last processed Cat_id from DdpCategorizedDocs.",jobName);
				//TODO : changing the logic inside this if loap getting the categorized docs of status is 0 or -1 or 100 and created date less than or equal configured days.
				Calendar todaysDate = GregorianCalendar.getInstance();
				
				/*DdpCategorizedDocs ddpCategorizedDocs = taskUtil.getDdpCategorizedDocsLastProcessedCatID();
				if(ddpCategorizedDocs != null)
				{
					logger.info("DdpInitiateProcessTask.execute(String jobName) - Records found for last process id in DDP_CATEGORIZATION_DOCS for Job [{}].",jobName);
					ddpCategorizedDocsLst = taskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(ddpCategorizedDocs);
				}
				else
				{
					DdpCategorizedDocs categorizedDocs = taskUtil.getMaxCatIdDdpCategorizedDocs();
					if(categorizedDocs != null)
					{
						*//************************************************************************************   
						 * This check ADDED TO AVOID REPROCESSING from STARTING. 
						 * If CAT_ID LESS THAN 100, then REPOCESS ALL DOCUMENTS. Assumption is that SYSTEM IS AT INITIAL STAGE.
						 * If CAT_ID GREATED THAN 100, then PROCESS FROM MAX ID. Assumption is that SYSTEM IS RUNNING FOR LONG PERIOD
						 * but JOB_REF_ID in JOB_REF_HOLDER table WAS DELETED for INITIATE JOB  but NOT INSERTED new record. So it will
						 * process from the MAX ID
						*************************************************************************************//*
						if(categorizedDocs.getCatId() < 100)
						{
							ddpCategorizedDocsLst = ddpCategorizedDocsService.findAllDdpCategorizedDocses();
						}
						else
						{
							ddpCategorizedDocsLst = taskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(categorizedDocs);
						}
					}
				}
			}*/
				todaysDate.add(Calendar.DAY_OF_YEAR, -2);
				ddpCategorizedDocsLst = fetchDdpCategorizedDocs(todaysDate);
			}
			else//Reference ID found in DdpCategorizedDocs to process records
			{
				logger.debug("DdpInitiateProcessTask.execute(String jobName) - Job Ref ID found for Job [{}] in DdpCategorizedDocs. Querying greater than Job Ref ID from DdpCategorizedDocs",jobName);
				
				ddpCategorizedDocsLst = taskUtil.getDdpCategorizedDocsGreaterThanMaxCatId(jobHolder ,jobName );

			}
			if(ddpCategorizedDocsLst == null || ddpCategorizedDocsLst.isEmpty() ){
				
				logger.info("DdpInitiateProcessTask.execute(String jobName) : Zero record found in DdpCategorizedDocs table for Job [{}].",jobName);
				//throw new Exception("DdpInitiateProcessTask.execute(String jobName) : Zero record found in DdpCategorizedDocs table.");
				
			}
			else
			{
				
				//sort the dmsDdpSynList into ascending order
				ddpCategorizedDocsLst = taskUtil.sortDdpCategorizedDocsList(ddpCategorizedDocsLst);
			
				/*************************************************
				 * SHOULD FIND THE ROLLBACK OPTION IN SPRING
				 ***********************************************/
				//insert/update the record for the 'jobName' (JRF_JOB_ID) with first SYN_ID into JRF_ID column with STATUS as '0' - In Progress
				//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
				isJobRefHolderUpdated = taskUtil.updateJobRefHolder(Integer.toString(ddpCategorizedDocsLst.get(0).getCatId()), jobHolder, jobName, 0);
				
				if(isJobRefHolderUpdated == false)
				{
					throw new Exception("DdpInitiateProcessTask.execute(String jobName) : Unable to insert/update DDP_JOB_REF_HOLDER table to hold the first processing CAT_ID as JRF_ID for Job ["+jobName+"].");
				}
				
				//Iterate the Rule list ('ruleList') to process Rules
				for (Rule rule : this.ruleList) 
				{
					Map<Integer, ArrayList<Object>> processRuleResult = rule.processRules(ddpCategorizedDocsLst);
					
				}
		
			
			//update the record for the 'jobName' (JRF_JOB_ID) with last SYN_ID into JRF_ID column with STATUS as '1' - Success
			//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
			jobHolder = taskUtil.getDdpJobRefHolderById(Integer.toString(ddpCategorizedDocsLst.get(0).getCatId()),jobName);
			isJobRefHolderUpdated = taskUtil.updateJobRefHolder(Integer.toString(ddpCategorizedDocsLst.get(ddpCategorizedDocsLst.size()-1).getCatId()), jobHolder, jobName, 1);
			if(isJobRefHolderUpdated == false)
			{
				taskUtil.sendMailByDevelopers("DdpInitiateProcessTask.execute(String jobName) : Unable to update DDP_JOB_REF_HOLDER table to hold the last processing Cat_ID as JRF_ID for Job ["+jobName+"].", " inside  DdpInitiateProcessTask.execute(String jobName)");
				throw new Exception("DdpInitiateProcessTask.execute(String jobName) : Unable to update DDP_JOB_REF_HOLDER table to hold the last processing Cat_ID as JRF_ID for Job ["+jobName+"].");
			}
		}
		 
	}
		catch (Exception ex) 
		{
			logger.error("DdpInitiateProcessTask.execute(String jobName) - Exception while executing Job [{}] [Message:"+ex.getMessage()+"].",jobName);
			taskUtil.sendMailByDevelopers("DdpInitiateProcessTask.execute(String jobName) - Exception while executing Job [{}] [Message:"+ex.getMessage()+"].", " inside  DdpInitiateProcessTask.execute(String jobName)");
			ex.printStackTrace();
        }
		logger.info("DdpInitiateProcessTask.execute(String jobName) executed successfully.");
		
	}
	
	public boolean processCatIDRange(int minCatID, int maxCatID)
	{
		logger.info("DdpInitiateProcessTask: processCatIDRange("+minCatID+","+maxCatID+") invoked");
		boolean isCategorizedDocsProcessed = false;
		List<DdpCategorizedDocs> ddpCategorizedDocsLst = null;
		ddpCategorizedDocsLst = taskUtil.getCategorizedDocsInRange(minCatID, maxCatID);
		//Iterate the Rule list ('ruleList') to process Rules
		try{
			for (Rule rule : this.ruleList) 
			{
				Map<Integer, ArrayList<Object>> processRuleResult = rule.processRules(ddpCategorizedDocsLst);
			}
			isCategorizedDocsProcessed = true;
		}catch (Exception e) {
			isCategorizedDocsProcessed = false;
		}
		return isCategorizedDocsProcessed;
	}
	public List<DdpCategorizedDocs> fetchDdpCategorizedDocs(Calendar date) {
		
		List<DdpCategorizedDocs> list = new ArrayList<DdpCategorizedDocs>();
		list = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_INITITATE_PROCESS_FETCH,new Object[]{date}, new RowMapper<DdpCategorizedDocs>() {

			@Override
			public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum)
					throws SQLException {
				
				DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
				return ddpCategorizedDocs;
			
			}
		});
		
		return list;
	}
}
