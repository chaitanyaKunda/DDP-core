package com.agility.ddp.core.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.entity.RuleEntity;
import com.agility.ddp.core.rule.Rule;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpCategorizationHolder;
import com.agility.ddp.data.domain.DdpDmsDocsHolder;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DmsDdpSyn;
import com.agility.ddp.data.domain.DmsDdpSynService;

@Configuration
//@PropertySources({@PropertySource(value ="file:///E:/DDPConfig/ddp.properties"),@PropertySource(value="file:///E:/DDPConfig/mail.properties")})
public class DdpCategorizationTask implements Task
{
	private static final Logger logger = LoggerFactory.getLogger(DdpCategorizationTask.class);
	
	private final String strJobId = DdpCategorizationTask.class.getCanonicalName();
	
	@Autowired
	private TaskUtil taskUtil;
	
	private PlatformTransactionManager transactionManager;
		
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	@Autowired
    DmsDdpSynService dmsDdpSynService;
	
    
	
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
		logger.debug("DdpCategorizationTask.setRuleDataAsList(List ruleDataAsList) method invoked.");
		
		ruleList.clear();
		ruleList.addAll(ruleDataAsList);
		
		logger.debug("DdpCategorizationTask.setRuleDataAsList(List ruleDataAsList) executed successfully.");
	}
	
	public void setTransactionManager (PlatformTransactionManager transactionManager) 
	{
		this.transactionManager = transactionManager;
	}

	
	public void execute()
	{
		execute(strJobId);
	}
	
	public void execute(JobExecutionContext context)
	{
		execute(context.getJobDetail().getKey().getGroup()+"."+context.getJobDetail().getKey().getName());
	}
	
	public void execute(String jobName)
	{
		logger.info("DdpCategorizationTask.execute(String jobName) method invoked.");
		try 
		{
			Map<String, List<Object>> returnedMap = null;
			List<DmsDdpSyn> dmsDdpSynList = null;
			List<Object> ddpDmsDocsHolderList = null;
			List<Object> ddpCategorizationHolder = null;
					
			DdpJobRefHolder jobHolder = taskUtil.getDdpJobRefHolder(jobName);
			
			if(jobHolder == null)//No records found in DdpJobRefHolder and querying max(syn_id) from DdpDmsDocsHolder to process
			{
				logger.debug("DdpCategorizationTask.execute(String jobName) - No record found for Job [{}] in DdpJobRefHolder. Querying Max(SYN_ID) from DdpDmsDocsHolder",jobName);
				
				DdpDmsDocsHolder ddpDmsDocsHolder = taskUtil.getMaxSynIdDdpDmsDocsHolder();
				
				if(ddpDmsDocsHolder == null)//No records found in DdpDmsDocsHolder and processing all DmsDdpSyn records
				{
					logger.debug("DdpCategorizationTask.execute(String jobName) - No Max record found for Job [{}] in DdpDmsDocsHolder. Querying all records from DmsDdpSyn",jobName);
					
					dmsDdpSynList = dmsDdpSynService.findAllDmsDdpSyns();
				}
				else//Records found in DdpDmsDocsHolder and processing from after max(syn_id) record GreaterThanMaxSynID
				{
					logger.debug("DdpCategorizationTask.execute(String jobName) - Max record found for Job [{}] in DdpDmsDocsHolder. Querying greater than Max record from DmsDdpSyn",jobName);
					
					dmsDdpSynList = taskUtil.getDmsDdpSynsGreaterThanMaxSynID(ddpDmsDocsHolder);
				}
			}
			else//Reference ID found in DdpJobRefHolder to process records
			{
				logger.debug("DdpCategorizationTask.execute(String jobName) - Job Ref ID found for Job [{}] in DdpJobRefHolder. Querying greater than Job Ref ID from DmsDdpSyn",jobName);
				
				dmsDdpSynList = taskUtil.getDmsDdpSynsGreaterThanMaxSynID(jobHolder);
			}
			
			if(dmsDdpSynList == null || dmsDdpSynList.isEmpty())
			{
				logger.info("DdpCategorizationTask.execute(String jobName) : Zero record found in DMS_DDP_SYN table OR All data in DMS_DDP_SYN table processed.");
				//throw new Exception("DdpCategorizationTask.execute(String jobName) : Zero record found in DMS_DDP_SYN table.");
				
			}else{
				
				//sort the dmsDdpSynList into ascending order
				dmsDdpSynList = taskUtil.sortDmsDdpSynList(dmsDdpSynList);
				
				/*************************************************
				 * SHOULD FIND THE ROLLBACK OPTION IN SPRING
				 ***********************************************/
				//insert/update the record for the 'jobName' (JRF_JOB_ID) with first SYN_ID into JRF_ID column with STATUS as '0' - In Progress
				//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
				boolean isJobRefHolderUpdated = taskUtil.updateJobRefHolder(Integer.toString(dmsDdpSynList.get(0).getSynId()), jobHolder, jobName, 0);
				if(isJobRefHolderUpdated == false)
				{
					taskUtil.sendMailByDevelopers(" Job ReF Holder returns false  for changing status 0 ", " Job ReF Holder inside DdpCategorizationTask.execute(String jobName)");
					throw new Exception("DdpCategorizationTask.execute(String jobName) : Unable to insert/update DDP_JOB_REF_HOLDER table to hold the first processing SYN_ID as JRF_ID for Job ["+jobName+"].");
				}
				
				//This process need fine-tune to check all the rules in DDP_RULE_DETAIL table
				returnedMap = checkDocsAgainstRules(dmsDdpSynList);
				
				ddpDmsDocsHolderList = returnedMap.get("DMS_DOCS_HOLDER");
				ddpCategorizationHolder = returnedMap.get("CAT_HOLDER");
				
				updateDocsAndCatHolderWithTransaction(ddpDmsDocsHolderList, ddpCategorizationHolder);
				
				//update the record for the 'jobName' (JRF_JOB_ID) with last SYN_ID into JRF_ID column with STATUS as '1' - Success
				//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
				jobHolder = taskUtil.getDdpJobRefHolderById(Integer.toString(dmsDdpSynList.get(0).getSynId()),jobName);
				isJobRefHolderUpdated = taskUtil.updateJobRefHolder(Integer.toString(dmsDdpSynList.get(dmsDdpSynList.size()-1).getSynId()), jobHolder, jobName, 1);
				if(isJobRefHolderUpdated = false)
				{
					//TODO : need to send mail.
					taskUtil.sendMailByDevelopers(" Job ReF Holder returns false  for changing status 1 ", " Job ReF Holder inside DdpCategorizationTask.execute(String jobName)");
					throw new Exception("DdpCategorizationTask.execute(String jobName) : Unable to update DDP_JOB_REF_HOLDER table to hold the last processing SYN_ID as JRF_ID for Job ["+jobName+"].");
				}
			}
        } 
		
		catch (Exception ex)  
		{
			logger.error("DdpCategorizationTask.execute(String jobName) - Exception while executing Job [{}] [Message:"+ex.getMessage()+"]. " + jobName,ex);
			ex.printStackTrace();
			//TODO: need to send the mail.
			taskUtil.sendMailByDevelopers(ex.getMessage(), " inside  DdpCategorizationTask.execute(String jobName)");
        }
		logger.info("DdpCategorizationTask.execute(String jobName) executed successfully.");
	}
	
	public void executeBetweenSynIDs(int minSynID,int maxSynID)
	{
		logger.info("DdpCategorizationTask.executeBetweenSynIDs(String jobName) method invoked.");
		try 
		{
			Map<String, List<Object>> returnedMap = null;
			List<DmsDdpSyn> dmsDdpSynList = null;
			List<Object> ddpDmsDocsHolderList = null;
			List<Object> ddpCategorizationHolder = null;
					
			DdpJobRefHolder jobHolder = null;
			
				
				dmsDdpSynList = taskUtil.getDmsDdpSynBeweenIDs(minSynID,maxSynID);
			
			if(dmsDdpSynList == null || dmsDdpSynList.isEmpty())
			{
				logger.info("DdpCategorizationTask.executeBetweenSynIDs(String jobName) : Zero record found in DMS_DDP_SYN table OR All data in DMS_DDP_SYN table processed.");
				//throw new Exception("DdpCategorizationTask.execute(String jobName) : Zero record found in DMS_DDP_SYN table.");
				
			}else{
				
				//sort the dmsDdpSynList into ascending order
				dmsDdpSynList = taskUtil.sortDmsDdpSynList(dmsDdpSynList);
				
				/*************************************************
				 * SHOULD FIND THE ROLLBACK OPTION IN SPRING
				 ***********************************************/
				//insert/update the record for the 'jobName' (JRF_JOB_ID) with first SYN_ID into JRF_ID column with STATUS as '0' - In Progress
				//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
				
				//This process need fine-tune to check all the rules in DDP_RULE_DETAIL table
				returnedMap = checkDocsAgainstRules(dmsDdpSynList);
				
				ddpDmsDocsHolderList = returnedMap.get("DMS_DOCS_HOLDER");
				ddpCategorizationHolder = returnedMap.get("CAT_HOLDER");
				
				updateDocsAndCatHolderWithTransaction(ddpDmsDocsHolderList, ddpCategorizationHolder);
				
				//update the record for the 'jobName' (JRF_JOB_ID) with last SYN_ID into JRF_ID column with STATUS as '1' - Success
				//Available values for the STATUS field are : '-1' - Failed, '0' - In Progress, '1' - Success)
			}
        } 
		
		catch (Exception ex)  
		{
			ex.printStackTrace();
			//TODO: need to send the mail.
			taskUtil.sendMailByDevelopers(ex.getMessage(), " inside  DdpCategorizationTask.execute(String jobName)");
        }
		logger.info("DdpCategorizationTask.execute(String jobName) executed successfully.");
	}
	/************************************************
	 * 
	 * This method checks DDP_RULE_DETAIL table for the document that newly created/updated in DMS.
	 * 
	 *  <<NEED FINE-TUNE>>
	 * 
	 * 
	 * @param dmsDdpSynList
	 * @return
	 *************************************************/
	private Map<String, List<Object>> checkDocsAgainstRules(List<DmsDdpSyn> dmsDdpSynList)
	{
		logger.debug("DdpCategorizationTask.checkDocsAgainstRules(List<DmsDdpSyn> dmsDdpSynList) method invoked.");
		
		Map<String, List<Object>> holdersList = new HashMap<String, List<Object>>();
		
		Map<Integer, ArrayList<RuleEntity>> ruleMap = new HashMap<Integer, ArrayList<RuleEntity>>();
		
		List<Object> ddpDmsDocsHolderList = new ArrayList<Object>();
		List<Object> dddpCategorizationHolderList = new ArrayList<Object>();
		
		try
		{	
			
			//Iterate the Rule list ('ruleList') to check dmsDdpSynList data against all Rules
			for (Rule rule : this.ruleList) 
			{
				Map<Integer, ArrayList<RuleEntity>> tempRuleMap = rule.chekRules(dmsDdpSynList);
				
				//Iterate each identified SYN_ID and RUL_ID to insert into ruleMap
				Iterator it = tempRuleMap.entrySet().iterator();
			    while (it.hasNext()) 
			    {
			        Map.Entry keyVal = (Map.Entry)it.next();
			        Integer synId = (Integer) keyVal.getKey();
			        ArrayList<RuleEntity> rulIdList = (ArrayList<RuleEntity>) keyVal.getValue();
			        
			        //append 'rulIdList' to the existing ruleList in ruleMap
			        if(ruleMap.containsKey(synId))
			        {
			        	((ArrayList<RuleEntity>)(ruleMap.get(synId))).addAll(rulIdList);
			        
			        }
			        else//create new 'rulIdList' in ruleMap
			        {
			        	ruleMap.put(synId, rulIdList);
			        }
			        
			    }

			}			
			
			
			/*$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
			 * $$$$$$$$$$$$$$$	This section need to be fine-tuned. Currently, it insert one record into DDP_DMS_DOCS_TXN
			 * $$$$$$$$$$$$$$$	each time and fetch DTX_ID to insert into to DDP_CATEGORIZATION_HOLDER table.  	
			 *$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$*/
			
			//Iterate dmsDdpSynList to insert a record into DDP_DMS_DOCS_TXN table and fetch DTX_ID
			//Create 
			for (DmsDdpSyn dmsDdpSyn : dmsDdpSynList) 
			{
				
				Integer orgSynId = dmsDdpSyn.getSynId();
				String strIsProcessReq = ruleMap.containsKey(orgSynId)?"0":"1";//0 - Yes, 1 - No
				
				Integer iDtxId = taskUtil.updateDdpDmsDocsTxn(dmsDdpSyn, strIsProcessReq);
				
				DdpDmsDocsHolder ddpDmsDocsHolder = new DdpDmsDocsHolder();
				
				ddpDmsDocsHolder.setThlSynId(orgSynId);
				ddpDmsDocsHolder.setThlRObjectId(dmsDdpSyn.getSynRObjectId());
				ddpDmsDocsHolder.setThlIsProcessReq(strIsProcessReq);
				ddpDmsDocsHolder.setThlTboName(dmsDdpSyn.getSynTboName());
				
				ddpDmsDocsHolderList.add(ddpDmsDocsHolder);
				
				
				if(ruleMap.containsKey(orgSynId))
				{					
					ArrayList<RuleEntity> ruleMapRulIdList = (ArrayList<RuleEntity>) ruleMap.get(orgSynId);
					
					for( RuleEntity ruleDetail : ruleMapRulIdList)
					{
						DdpCategorizationHolder ddpCategorizationHolder = new DdpCategorizationHolder();
						
						ddpCategorizationHolder.setChlSynId(orgSynId);
						ddpCategorizationHolder.setChlDtxId(iDtxId);
						ddpCategorizationHolder.setChlRulId(ruleDetail.getRdtId());
						
						dddpCategorizationHolderList.add(ddpCategorizationHolder);
					}
						
				}
								
			}
		
			holdersList.put("DMS_DOCS_HOLDER", ddpDmsDocsHolderList);
			holdersList.put("CAT_HOLDER", dddpCategorizationHolderList);
			
		}
		catch(Exception ex)
		{
			logger.error("DdpCategorizationTask.checkDocsAgainstRules(List<DmsDdpSyn> dmsDdpSynList) - Exception while checking the rules.");
			ex.printStackTrace();
		}
		logger.debug("DdpCategorizationTask.checkDocsAgainstRules(List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
		return holdersList;
	}
	
	
	/************************************************
	 * 	&&&&&&&&&&&&&&&&&&&     USING PlatformTransactionManager - MAY REQUIRE FINE-TUNING &&&&&&&&&&&&&&&&&&&&&&&&&
	 *   
	 * 
	 * 
	 * @param dmsDdpSyn, strIsProcessReq
	 * @return Integer
	 *************************************************/
	private void updateDocsAndCatHolderWithTransaction(List<Object> ddpDmsDocsHolderList, List<Object> ddpCategorizationHolderList) throws Exception
	{
		
		DefaultTransactionDefinition def = new DefaultTransactionDefinition();
		def.setName("DMS_DOCS_AND_CAT_UPDATE");
		def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

		TransactionStatus status = transactionManager.getTransaction(def);
		
		try
		{
			taskUtil.syncDmsDdpSynAndDdpDmsDocsHolderBatch(ddpDmsDocsHolderList);
			
			taskUtil.insertDdpCategorizationHolderBatch(ddpCategorizationHolderList);
		}
		catch(Exception ex)
		{
			//ex.printStackTrace();
			transactionManager.rollback(status);
			throw new Exception(ex);
		}
		
		transactionManager.commit(status);
	}
	
}
