/**
 * 
 */
package com.agility.ddp.core.rule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.persistence.TypedQuery;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.SchedulingException;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;
import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.entity.DdpRateSetupEntity;
import com.agility.ddp.core.entity.RuleEntity;
import com.agility.ddp.core.quartz.DdpMultiAedRuleJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpEmailTriggerSetup;
import com.agility.ddp.data.domain.DdpEmailTriggerSetupService;
import com.agility.ddp.data.domain.DdpGenSourceSetup;
import com.agility.ddp.data.domain.DdpParty;
import com.agility.ddp.data.domain.DdpRateSetup;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DmsDdpSyn;

/**
 * @author DGuntha
 *
 */
public class DdpMultiAedRuleChecking implements Rule {

	private static final Logger logger = LoggerFactory.getLogger(DdpMultiAedRuleChecking.class);
	
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    private DdpRuleDetailService ddpRuleDetailService;
    
    
	@Autowired
	private ApplicationContext applicationContext;	
	
	@Autowired
	private DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
    private ApplicationProperties applicationProperties;
	
	@Autowired
	private CommonUtil commonUtil;
	
	/* (non-Javadoc)
	 * @see com.agility.ddp.core.rule.Rule#chekRules(java.util.List)
	 */
	@Override
	public Map<Integer, ArrayList<RuleEntity>> chekRules(List<DmsDdpSyn> dmsDdpSynList) {
	
		 logger.debug("DdpMultiAedRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) method invoked.");
         Map<Integer, ArrayList<RuleEntity>> ruleMap = new ConcurrentHashMap<Integer, ArrayList<RuleEntity>>(); 
         Calendar currentCalendar = Calendar.getInstance();
         
         try {
        	 
        	 List<RuleEntity> ddpRuleDetails = getAllMatchedDdpRuleDetails(currentCalendar);
			 
			 if (ddpRuleDetails == null || ddpRuleDetails.size() == 0) {
 				logger.debug("DdpMultiAedRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) : Is empty list ");
 				return ruleMap;
 			}
			 
        	 for( DmsDdpSyn dmsDdpSyn : dmsDdpSynList) {
        		 
        		 if (dmsDdpSyn.getSynBranchSource() != null && dmsDdpSyn.getSynCompanySource() != null && dmsDdpSyn.getSynDocType() != null && dmsDdpSyn.getSynGenSystem() != null) {
         			
         			ArrayList<RuleEntity> lstruledetails = new ArrayList<RuleEntity>();
         			
         			List<RuleEntity> ddpZRuleDetails = ddpRuleDetails.stream().filter(o -> (((o.getRdtCompany() != null ? o.getRdtCompany() : "").equalsIgnoreCase(dmsDdpSyn.getSynCompanySource()) || (o.getRdtCompany() != null ? o.getRdtCompany() : "").equalsIgnoreCase("GIL")) 
                    		&& (((o.getRdtBranch() != null ? o.getRdtBranch() : "").equalsIgnoreCase(dmsDdpSyn.getSynBranchSource())|| "All".equalsIgnoreCase((o.getRdtBranch() != null ? o.getRdtBranch() : ""))))
                    		&& ((o.getRdtDocType() != null ? o.getRdtDocType() : "").equalsIgnoreCase(dmsDdpSyn.getSynDocType())))).collect(Collectors.toList());
         			
         			for(RuleEntity ddpRuleDetail : ddpZRuleDetails) {
         				       				
	            	
	                    String partyCodeValue= "";
//	                    String strRdtCompany	= (ddpRuleDetail.getRdtCompany() != null ? ddpRuleDetail.getRdtCompany().getComCompanyCode() : "") ;
//			            String strRdtBranch = (ddpRuleDetail.getRdtBranch() != null ? ddpRuleDetail.getRdtBranch().getBrnBranchCode() : "");
//			            String strRdtDocType = (ddpRuleDetail.getRdtDocType() != null ? ddpRuleDetail.getRdtDocType().getDtyDocTypeCode() : "");
//			            
//			            if((strRdtCompany.equalsIgnoreCase(dmsDdpSyn.getSynCompanySource()) ) 
//	                    		&& ((strRdtBranch.equalsIgnoreCase(dmsDdpSyn.getSynBranchSource())|| "All".equalsIgnoreCase(strRdtBranch)))
//	                    		&& (strRdtDocType.equalsIgnoreCase(dmsDdpSyn.getSynDocType()))) {
			            	
	                    String party_code = ddpRuleDetail.getRdtPartyCode();
		                    
	                    if (party_code == null)
	                    	continue;
	                    	
		                    // Department checking 
		                    if (dmsDdpSyn.getSynDeptSource() != null && ddpRuleDetail.getRdtDepartment() != null && ddpRuleDetail.getRdtDepartment().length() > 0 && !Arrays.asList(ddpRuleDetail.getRdtDepartment().split(",")).contains(dmsDdpSyn.getSynDeptSource())) 
		                    	continue;
		                    
		                    
		                    		
			             	if(party_code.equalsIgnoreCase("CLID")){ partyCodeValue = dmsDdpSyn.getSynClientId(); }
	                        if(party_code.equalsIgnoreCase("SHIP")){ partyCodeValue = dmsDdpSyn.getSynShipper(); }
	                        if(party_code.equalsIgnoreCase("CGNE")){ partyCodeValue = dmsDdpSyn.getSynConsignee(); }
	                        if(party_code.equalsIgnoreCase("NTPY")){ partyCodeValue = dmsDdpSyn.getSynNotifyParty(); }
	                        if(party_code.equalsIgnoreCase("DBFD")){ partyCodeValue = dmsDdpSyn.getSynDebitsForward(); }
	                        if(party_code.equalsIgnoreCase("DBBK")){ partyCodeValue = dmsDdpSyn.getSynDebitsBack(); }
	                        if(party_code.equalsIgnoreCase("IMGT")){ partyCodeValue = dmsDdpSyn.getSynIntermediateAgent(); }
	                        if(party_code.equalsIgnoreCase("ITAG")){ partyCodeValue = dmsDdpSyn.getSynInitialAgent(); }
	                        if(party_code.equalsIgnoreCase("FLAG")){ partyCodeValue = dmsDdpSyn.getSynFinalAgentId(); }
	                        if(partyCodeValue==null){ partyCodeValue = ""; }
	                              
	                        if(partyCodeValue.trim().equalsIgnoreCase(ddpRuleDetail.getRdtPartyId().trim())) {
	                        	
	                        	 DdpGenSourceSetup genSourceSetup =  commonUtil.getMatchedGeneratedSourceSetup(ddpRuleDetail.getRdtId());
                           	  	String genSource = (genSourceSetup != null ? genSourceSetup.getGssOption() : "");
                            	                            	
                            	//Checking the Generating source matching 
                            	if (dmsDdpSyn.getSynGenSystem().equalsIgnoreCase(genSource) || genSource.equalsIgnoreCase("any") || (genSource.equalsIgnoreCase("3rd Party") && !dmsDdpSyn.getSynGenSystem().equalsIgnoreCase("Control"))) {
                            	
		                        	boolean isRateSetupAvailable = true;
		    	            		if (dmsDdpSyn.getSynGenSystem() != null && (dmsDdpSyn.getSynGenSystem().equalsIgnoreCase("Control") || genSource.equalsIgnoreCase("Any"))) {
		    	            			
		    	                       	List<DdpRateSetupEntity> ddpRateSetupSet = commonUtil.getMatchedRateSetup(ddpRuleDetail.getRdtId());
		    	                       	
		    	            			if (ddpRateSetupSet != null) {
		    	            				String rateSetupString = (dmsDdpSyn.getSynIsRated() == null  || dmsDdpSyn.getSynIsRated() == 0)? "Non-Rated" : "Rated";
		    	            				for (DdpRateSetupEntity rateSetup : ddpRateSetupSet) {
		    	            					if (rateSetup.getRtsOption().equalsIgnoreCase("N/A")) {
		    	            						continue;
		    	            					} else if (!rateSetupString.equalsIgnoreCase(rateSetup.getRtsOption())) {
		    	            						isRateSetupAvailable = false;
		    	            						break;
		    	            					}                             
		    	            				}
		    	            			}
		    	            		}
		    	            		
		    	            		if (!isRateSetupAvailable)
		    	            			continue;
		    	            	                        	
		                            lstruledetails.add(ddpRuleDetail);
                            	}
	                        }
			            }
	    //              }
         				if(! lstruledetails.isEmpty() ) 
         					ruleMap.put(dmsDdpSyn.getSynId(),lstruledetails); 
   
        		 } else {
        			 logger.debug("DdpMultiAedRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) :No Branch or No Company or No Doc Type for  SYN_ID:"+dmsDdpSyn.getSynId());
        		 }
        	 }
        	 
         } catch (Exception ex) {
        	 logger.error("DdpMultiAedRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) exception is occuirred...'", ex);
         }
	        
		 logger.debug("DdpMultiAedRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) is successfully completed.");
		return ruleMap;
	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.rule.Rule#processRules(java.util.List)
	 */
	@Override
	public Map<Integer, ArrayList<Object>> processRules(
			List<DdpCategorizedDocs> ddpCategorizedDocsLst) {
	
		
		 logger.debug("DdpMultiAedRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) method invoked.");
         Map<Integer, ArrayList<Object>> processResult = new HashMap<Integer, ArrayList<Object>>();
         List<DdpCategorizedDocs> multiAedList = new ArrayList<DdpCategorizedDocs>();
         for (DdpCategorizedDocs ddpCategorizedDocs : ddpCategorizedDocsLst) {
        	 
        	 try 
				{ 
					if(ddpCategorizedDocs.getCatRuleType().equalsIgnoreCase("MULTI_AED_RULE"))
					{
						DdpEmailTriggerSetup ddpEmailTriggerSetup = getDdpEmailTriggerSetup(ddpCategorizedDocs.getCatRulId().getRulId());
						
						if (ddpEmailTriggerSetup == null || ddpEmailTriggerSetup.getEtrTriggerName().equalsIgnoreCase(Constant.TRIGGER_NAME_SPECIFIC_TIME)) 
							continue;
						
						multiAedList.add(ddpCategorizedDocs);
					}
				} catch (Exception ex) {
					//TODO: Send mail to Dev team.
					logger.error("DdpMultiAedRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) while iterating the DdpCategorizedDocs id : "+ddpCategorizedDocs.getCatId(), ex);
				}
        	
         }
         if (multiAedList.size() > 0)
        	 processConsolidatedAEDBatch(multiAedList);
	        
		return processResult;
	}
	
	/**
	 * Method used for processing the multi aed as batch.
	 * 
	 * @param categorizedDocsList
	 * @throws Exception
	 */
	private void processConsolidatedAEDBatch(List<DdpCategorizedDocs> categorizedDocsList) {
		
		try {
			
			/** STEP 1 : INSTANTIATE JOB DETAIL CLASS AND SET ITS PROPERTIES **/
	        MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
	        Scheduler scheduler = applicationProperties.getQuartzScheduler("MULTI_AED_"+categorizedDocsList.get(0).getCatId()+"_"+(new Date()));
	        
	        DdpMultiAedRuleJob job = applicationContext.getBean("multiAedRuleJob", DdpMultiAedRuleJob.class);
	        
	        jdfb.setTargetObject(job);
	        
	        jdfb.setTargetMethod("callWorkFlowWithBulkRecords");
	        
	        jdfb.setArguments(new Object[]{categorizedDocsList,scheduler});
	        jdfb.setName("Multi-AED-"+categorizedDocsList.get(0).getCatId()+"-"+(new Date()));
	        jdfb.setGroup("Multi-AED-"+categorizedDocsList.get(0).getCatId()+"-"+(new Date()));
	        jdfb.afterPropertiesSet();
	        JobDetail jd = (JobDetail)jdfb.getObject();
	
	//        /** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
	//        SimpleTriggerFactoryBean simpleTriggerBean = new SimpleTriggerFactoryBean();
	//        simpleTriggerBean.setJobDetail(jd);
	//        simpleTriggerBean.setRepeatInterval(1000*10);
	//        simpleTriggerBean.setRepeatCount(0);
	//        simpleTriggerBean.setStartDelay(0);
	//        simpleTriggerBean.setStartTime(new Date());
	////        simpleTriggerBean.getObject().setJobName(jd.getName());
	//        simpleTriggerBean.setBeanName("Multi-AED-"+ddpCategorizedDocs.getCatId());
	//        simpleTriggerBean.setName("Multi-AED-"+ddpCategorizedDocs.getCatId());
	//
	//        /** STEP 3 : INSTANTIATE SCHEDULER FACTORY BEAN AND SET ITS PROPERTIES **/
	//        SchedulerFactoryBean sfb = new SchedulerFactoryBean();
	//        sfb.setJobDetails(new JobDetail[]{(JobDetail)jdfb.getObject()});
	//        sfb.setTriggers(new Trigger[]{simpleTriggerBean.getObject()});
	//        sfb.afterPropertiesSet();
	        
	        /** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
	        SimpleTriggerImpl simpleTriggerBean = new SimpleTriggerImpl();
	        //simpleTriggerBean.setJobDetail(jd);
	        simpleTriggerBean.setRepeatInterval(1000*10);
	        simpleTriggerBean.setRepeatCount(0);
	       // simpleTriggerBean.setStartDelay(0);
	        simpleTriggerBean.setStartTime(new Date());
	//        simpleTriggerBean.setJobName(jd.getName());
	//        simpleTriggerBean.getObject().setJobName(jd.getName());
	        simpleTriggerBean.setGroup("Multi-AED-"+categorizedDocsList.get(0).getCatId()+"-"+(new Date()));
	        simpleTriggerBean.setName("Multi-AED-"+categorizedDocsList.get(0).getCatId()+"-"+(new Date()));
	        
	        scheduler.scheduleJob(jd, simpleTriggerBean);
	        try 
	        {
	        	scheduler.start();
	        	logger.info("started Thread with name :Multi-AED-"+categorizedDocsList.get(0).getCatId()+"-"+(new Date())+" and with Scheduler : MULTI_AED_"+categorizedDocsList.get(0).getCatId()+"_"+(new Date()));
	            Thread.sleep(600L);
	        } 
	        catch (SchedulingException e) 
	        {
	        	for (DdpCategorizedDocs ddpCategorizedDocs : categorizedDocsList) {
		        	//Update the status when job failed
		        	ddpCategorizedDocs.setCatStatus(-1);
					ddpCategorizedDocsService.updateDdpCategorizedDocs(ddpCategorizedDocs);
	        	}
	            e.printStackTrace();
	        }
		} catch (Exception ex) {
			logger.error("DdpMultiAedRuleChecking.processConsolidatedAEDBatch(List<DdpCategorizedDocs> ddpCategorizedDocsLst) while iterating the DdpCategorizedDocs  "+categorizedDocsList, ex);
		}
	}
	
	/**
     * Method used for getting all Ddp multi aed Rule Details based on the below matched criteria.
     * 
     * @param branch
     * @param company
     * @param docType
     * @param genSource
     * @param syncID
     * @return
     */
    private List<RuleEntity> getAllMatchedDdpRuleDetails(Calendar currentCalendar) {
    	
    	List<RuleEntity> ruleDetails = null;
    	
    	try {
    		ruleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_MULTI_RULE_DETAIL_ID,new Object[] {currentCalendar}, new RuleEntity());                          
		
		}
		catch(Exception ex)
		{
			logger.error("DdpMultiAedRuleChecking.getAllMatchedDdpRuleDetails() - Exception while accessing DdpRuleDetail retrive based [{}] records  ",ex);
			ex.printStackTrace();
		}

    	return ruleDetails;
    }

    
    private DdpEmailTriggerSetup getDdpEmailTriggerSetup(Integer ruleID) {
    	DdpEmailTriggerSetup triggerSetup = null;
    	try {
    	 List<DdpEmailTriggerSetup>	triggerSetups = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_EMAIL_TIGGER_ID, new Object[]{ruleID},  new RowMapper<DdpEmailTriggerSetup>() {

				@Override
				public DdpEmailTriggerSetup mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					
					DdpEmailTriggerSetup ddpEmailTriggerSetup = new DdpEmailTriggerSetup();
					ddpEmailTriggerSetup.setEtrId(rs.getInt("ETR_ID"));
					ddpEmailTriggerSetup.setEtrTriggerName(rs.getString("ETR_TRIGGER_NAME"));
					ddpEmailTriggerSetup.setEtrDocTypes(rs.getString("ETR_DOC_TYPES"));
					ddpEmailTriggerSetup.setEtrDocSelection(rs.getString("ETR_DOC_SELECTION"));
					ddpEmailTriggerSetup.setEtrRetries(rs.getInt("ETR_RETRIES"));
					
					return ddpEmailTriggerSetup;
    			
    		}
    		});
    	 if (triggerSetups != null && triggerSetups.size() > 0)
    		 triggerSetup = triggerSetups.get(0);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	
    	return triggerSetup;
    }
    
}
