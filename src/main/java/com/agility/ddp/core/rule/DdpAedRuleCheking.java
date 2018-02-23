package com.agility.ddp.core.rule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import com.agility.ddp.core.quartz.RuleJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.data.domain.DdpAedRule;
import com.agility.ddp.data.domain.DdpBranchService;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpCompanyService;
import com.agility.ddp.data.domain.DdpDoctypeService;
import com.agility.ddp.data.domain.DdpGenSourceSetup;
import com.agility.ddp.data.domain.DdpParty;
import com.agility.ddp.data.domain.DdpPartyService;
import com.agility.ddp.data.domain.DdpRateSetup;
import com.agility.ddp.data.domain.DdpRule;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DdpRuleService;
import com.agility.ddp.data.domain.DmsDdpSyn;

//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class DdpAedRuleCheking implements Rule
{
    private static final Logger logger = LoggerFactory.getLogger(DdpAedRuleCheking.class);
    
   // @Autowired
   // Environment env;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    DdpRuleDetailService ddpRuleDetailService;
    
    @Autowired
    DdpRuleService ddpRuleService;

    @Autowired
    DdpCompanyService ddpCompanyService;
    
    @Autowired
    DdpBranchService ddpBranchService;
    
    @Autowired
    DdpDoctypeService ddpDoctypeService;
    
    @Autowired
    DdpPartyService ddpPartyService;
    
    @Autowired
	DdpCategorizedDocsService ddpCategorizedDocsService;
    
    @Autowired
	ApplicationContext applicationContext;
    
    @Autowired
    private ApplicationProperties applicationProperties;
    
    @Autowired
    private CommonUtil commonUtil;
    

    
    public Map<Integer, ArrayList<RuleEntity>> chekRules(List<DmsDdpSyn> dmsDdpSynList)
    {
        logger.debug("DdpAedRuleCheking.chekRules(List<DmsDdpSyn> dmsDdpSynList) method invoked.");
        Map<Integer, ArrayList<RuleEntity>> ruleMap = new ConcurrentHashMap<Integer, ArrayList<RuleEntity>>(); 
        //List<Integer> lstRdtIds = getDmsDdpRuleDetail();
        
       // Calendar today = Calendar.getInstance();
       // today.setTime(new Date());
        Calendar currentCalendar = Calendar.getInstance();
      /*  currentCalendar.set(Calendar.YEAR, today.get(Calendar.YEAR) );
        currentCalendar.set(Calendar.MONTH, today.get(Calendar.MONTH)+1);
        currentCalendar.set(Calendar.DAY_OF_MONTH, today.get(Calendar.DAY_OF_MONTH));*/
      
       // if (dmsDdpSynList.size() > 2500) {
        	processSyncRecordsGreaterThan2000(dmsDdpSynList, currentCalendar, ruleMap);
       // } else {
        //	processSyncRecordsLessThan2000(dmsDdpSynList, currentCalendar, ruleMap);
        //}
       
    logger.debug("DdpAedRuleCheking.chekRules(List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
    
    return ruleMap;
    }
    
    /**
     * Method used for processing the sync records greater than 2000.
     * 
     * @param dmsDdpSynList
     * @param currentCalendar
     * @param ruleMap
     */
    private void processSyncRecordsGreaterThan2000(List<DmsDdpSyn> dmsDdpSynList,Calendar currentCalendar,Map<Integer, ArrayList<RuleEntity>> ruleMap) {
    	
    	logger.info("DdpAedRuleChecking.processSyncRecordsGreaterThan2000() - Invoked Succesfully"); 
    	 try
         {
         	
    		 long startTime = System.currentTimeMillis();
    		 List<RuleEntity> ddpZRuleDetail = getAllMatchedDdpRuleDetails(currentCalendar);
         	//List<DdpRuleDetail> ddpZRuleDetail = ddpRuleDetailService.findAllDdpRuleDetails();
    		 logger.info("DdpAedRuleChecking.processSyncRecordsGreaterThan2000() - ddpRuleDetailService.findAllDdpRuleDetails() Time taken is : -> "+(System.currentTimeMillis() - startTime) );
         	
 			if (ddpZRuleDetail == null || ddpZRuleDetail.size() == 0) {
 				logger.debug("DdpAedRuleCheking.chekRules(List<DmsDdpSyn> dmsDdpSynList) : is empty list");
 				return;
 			}
 			//System.out.println("Size of count : "+ddpZRuleDetail.size());
         	for( DmsDdpSyn dmsDdpSyn : dmsDdpSynList)
 	        	{   
         		 if (dmsDdpSyn.getSynBranchSource() != null && dmsDdpSyn.getSynCompanySource() != null && dmsDdpSyn.getSynDocType() != null && dmsDdpSyn.getSynGenSystem() != null) {
         			
         			ArrayList<RuleEntity> lstruledetails = new ArrayList<RuleEntity>();
 		
         			List<RuleEntity> ddpRuleDetails = ddpZRuleDetail.stream().filter(o -> (((o.getRdtCompany() != null ? o.getRdtCompany() : "").equalsIgnoreCase(dmsDdpSyn.getSynCompanySource()) || (o.getRdtCompany() != null ? o.getRdtCompany() : "").equalsIgnoreCase("GIL")) 
 		                    		&& (((o.getRdtBranch() != null ? o.getRdtBranch() : "").equalsIgnoreCase(dmsDdpSyn.getSynBranchSource())|| "All".equalsIgnoreCase((o.getRdtBranch() != null ? o.getRdtBranch() : ""))))
 		                    		&& ((o.getRdtDocType() != null ? o.getRdtDocType() : "").equalsIgnoreCase(dmsDdpSyn.getSynDocType())))).collect(Collectors.toList());
         			
 		            	for(RuleEntity ddpRuleDetail : ddpRuleDetails)
 		            	{
 		            		
 		                    String partyCodeValue= "";
 		                    	
 		                    //If party id is * then you no need to check the party value.		
 		                    if (!ddpRuleDetail.getRdtPartyId().trim().equalsIgnoreCase("*")) {
 		                    	
 		                    	 String party_code = ddpRuleDetail.getRdtPartyCode();
 		                    	 
 				                if (party_code == null)
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
 		                    }
 	                         
                             if(partyCodeValue.trim().equalsIgnoreCase(ddpRuleDetail.getRdtPartyId().trim())  || ddpRuleDetail.getRdtPartyId().trim().equalsIgnoreCase("*")) { 
                            
                            	  DdpGenSourceSetup genSourceSetup =  commonUtil.getMatchedGeneratedSourceSetup(ddpRuleDetail.getRdtId());
                            	  String genSource = (genSourceSetup != null ? genSourceSetup.getGssOption() : "");
                             	
                             	//Checking the Generating source matching 
                             	if (dmsDdpSyn.getSynGenSystem().equalsIgnoreCase(genSource) || genSource.equalsIgnoreCase("any") || (genSource.equalsIgnoreCase("3rd Party") && !dmsDdpSyn.getSynGenSystem().equalsIgnoreCase("Control"))) {
                             	
 	                                boolean isRateSetupAvailable = true;
 	    		            		if (dmsDdpSyn.getSynGenSystem() != null && (dmsDdpSyn.getSynGenSystem().equalsIgnoreCase("Control") || genSource.equalsIgnoreCase("Any")) ) {
 	    		            			
 	    		                       	List<DdpRateSetupEntity> ddpRateSetupSet =  commonUtil.getMatchedRateSetup(ddpRuleDetail.getRdtId());
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
 		            	//}
 		            
 		    	     if(! lstruledetails.isEmpty() ) 
 		    	    	 ruleMap.put(dmsDdpSyn.getSynId(),lstruledetails); 
 	        } else {
 	    		logger.debug("DdpAedRuleCheking.chekRules(List<DmsDdpSyn> dmsDdpSynList) :No Branch or No Company or No Doc Type for  SYN_ID:"+dmsDdpSyn.getSynId());
 	    	}
         		 
 	    }
     	
      } catch(Exception ex) {
           logger.error("DdpAedRuleCheking.chekRules(List<DmsDdpSyn> dmsDdpSynList) exception while iterating.",ex);
     }
     
    }
    
    /**
     * Method used for processing the syn records less than 2000 records for faster purpose.
     * 
     * @param dmsDdpSynList
     * @param currentCalendar
     * @param ruleMap
     */
    private void processSyncRecordsLessThan2000(List<DmsDdpSyn> dmsDdpSynList,Calendar currentCalendar,Map<Integer, ArrayList<DdpRuleDetail>> ruleMap ) {
    	
    	logger.info("DdpAedRuleChecking.processSynRecordsLessThan2000() is Invoked");
    	try {
    	for( DmsDdpSyn dmsDdpSyn : dmsDdpSynList)
    	{   
			 if (dmsDdpSyn.getSynBranchSource() != null && dmsDdpSyn.getSynCompanySource() != null && dmsDdpSyn.getSynDocType() != null && dmsDdpSyn.getSynGenSystem() != null) {
				
				List<DdpRuleDetail> ddpZRuleDetail = getAllMatchedDdpRuleDetails(dmsDdpSyn.getSynBranchSource(), dmsDdpSyn.getSynCompanySource(), dmsDdpSyn.getSynDocType(), dmsDdpSyn.getSynGenSystem(), dmsDdpSyn.getSynId(),currentCalendar);
				 
				if (ddpZRuleDetail == null || ddpZRuleDetail.size() == 0) {
					logger.debug("DdpAedRuleCheking.chekRules(List<DmsDdpSyn> dmsDdpSynList) : Non control document whose SYN_ID : "+dmsDdpSyn.getSynId());
					continue;
				}
				
				ArrayList<DdpRuleDetail> lstruledetails = new ArrayList<DdpRuleDetail>();
	
	            	for(DdpRuleDetail ddpRuleDetail : ddpZRuleDetail)
	            	{
	            		
	                    String party_code="";
	                    String partyCodeValue= "";
	                   
	                    DdpParty objparty = ddpRuleDetail.getRdtPartyCode();
	                    
	                    if(objparty != null ){ party_code = objparty.getPtyPartyCode() ; }
	                    		
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
	
	                    if(partyCodeValue.trim().equalsIgnoreCase(ddpRuleDetail.getRdtPartyId().trim()) || ddpRuleDetail.getRdtPartyId().trim().equalsIgnoreCase("*")) { 
	                   
	                            boolean isRateSetupAvailable = true;
	                            DdpGenSourceSetup genSourceSetup =  commonUtil.getMatchedGeneratedSourceSetup(ddpRuleDetail.getRdtId());
	                            String gensource = (genSourceSetup != null ? genSourceSetup.getGssOption() : "");
	                            
			            		if (gensource.equalsIgnoreCase("Control") || gensource.equalsIgnoreCase("Any")) {
			            			TypedQuery<DdpRateSetup> query1 =  DdpRateSetup.findDdpRateSetupsByDdpRuleDetail(ddpRuleDetail);
			                       	List<DdpRateSetup> ddpRateSetupSet = query1.getResultList();
			            			if (ddpRateSetupSet != null) {
			            				String rateSetupString = (dmsDdpSyn.getSynIsRated() == null  || dmsDdpSyn.getSynIsRated() == 0)? "Non-Rated" : "Rated";
			            				for (DdpRateSetup rateSetup : ddpRateSetupSet) {
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
	    	     if(! lstruledetails.isEmpty() ) 
	    	    	 ruleMap.put(dmsDdpSyn.getSynId(),lstruledetails); 
			} 
			else
			{
				logger.debug("DdpAedRuleCheking.processSynRecordsLessThan2000(List<DmsDdpSyn> dmsDdpSynList) :No Branch or No Company or No Doc Type for  SYN_ID:"+dmsDdpSyn.getSynId());
			}
		 
      }
    	} catch(Exception ex) {
            logger.error("DdpAedRuleCheking.processSynRecordsLessThan2000(List<DmsDdpSyn> dmsDdpSynList) exception while iterating.",ex);
        }

    }
    
    public Map<Integer, ArrayList<Object>> processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst)
    {
        logger.debug("DdpAedRuleCheking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) method invoked.");
        
        Map<Integer, ArrayList<Object>> processResult = new HashMap<Integer, ArrayList<Object>>();
                
        try
        {
        	for(DdpCategorizedDocs categorizedDocs :  ddpCategorizedDocsLst)
        	{
        		//trigger;
				try 
				{ 
					if(categorizedDocs.getCatRuleType().equalsIgnoreCase("AED_RULE"))
					{
					 	/** STEP 1 : INSTANTIATE JOB DETAIL CLASS AND SET ITS PROPERTIES **/
			            MethodInvokingJobDetailFactoryBean jdfb = new MethodInvokingJobDetailFactoryBean();
			           // Scheduler scheduler = new StdSchedulerFactory().getScheduler();
			            Scheduler scheduler = applicationProperties.getQuartzScheduler("AED_"+categorizedDocs.getCatId()+"_"+(new Date()));
			            //jdfb.setTargetClass(RuleJob.class);
			            //RuleJob job = new RuleJob(ddpCategorizedDocsService,ddpCommEmailService,taskUtil,env);
			            RuleJob job = applicationContext.getBean("ruleJob", RuleJob.class);
			            
			            jdfb.setTargetObject(job);
			            
			            jdfb.setTargetMethod("callWorkFlow");
			            
			            jdfb.setArguments(new Object[]{categorizedDocs,scheduler,1});
			            jdfb.setName("AED:"+categorizedDocs.getCatId()+"-"+(new Date()));
			            jdfb.afterPropertiesSet();
			            JobDetail jd = (JobDetail)jdfb.getObject();

			            /** STEP 2 : INSTANTIATE CRON TRIGGER AND SET ITS PROPERTIES **/
			            SimpleTriggerImpl simpleTriggerBean = new SimpleTriggerImpl();
			            //simpleTriggerBean.setJobDetail(jd);
			            simpleTriggerBean.setRepeatInterval(1000*10);
			            simpleTriggerBean.setRepeatCount(0);
			           // simpleTriggerBean.setStartDelay(0);
			            simpleTriggerBean.setStartTime(new Date());
//			            simpleTriggerBean.setJobName(jd.getName());
//			            simpleTriggerBean.getObject().setJobName(jd.getName());
			            simpleTriggerBean.setGroup("AED:"+categorizedDocs.getCatId()+"-"+(new Date()));
			            simpleTriggerBean.setName("AED:"+categorizedDocs.getCatId()+"-"+(new Date()));
						//CronTriggerBean ctb = new CronTriggerBean();
						//ctb.setJobDetail(jd);
						//ctb.setBeanName("Daily cron");
						//ctb.setJobName(jd.getName());
						//try {
						//    ctb.setCronExpression("* * * * * ? *");
						//} catch (ParseException e) {
						//    e.printStackTrace();
						//}
						//	
						//ctb.afterPropertiesSet();

			            /** STEP 3 : INSTANTIATE SCHEDULER FACTORY BEAN AND SET ITS PROPERTIES **/
//			            SchedulerFactoryBean sfb = new SchedulerFactoryBean();
//			            sfb.setJobDetails(new JobDetail[]{(JobDetail)jdfb.getObject()});
//			            sfb.setTriggers(new Trigger[]{simpleTriggerBean.getObject()});
//			            sfb.afterPropertiesSet();
			           
				         scheduler.scheduleJob(jd, simpleTriggerBean);
				     	
			            try 
			            {
			            	 scheduler.start();
			                Thread.sleep(600L);
			             	//Job is inprogress
			                //categorizedDocs.setCatStatus(100);
			                //ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDocs);
			            } 
			            catch (SchedulingException e) 
			            {
			            	//Update the status when job failed
				        	categorizedDocs.setCatStatus(-1);
							ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDocs);
				            e.printStackTrace();
			            }
			           
					}
				}
				catch (Exception e) 
				{
		        	//Update the status when job failed
		        	categorizedDocs.setCatStatus(-1);
					ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDocs);
		            e.printStackTrace();
		        }
        	}
        }
        catch(Exception ex)
        {
        	logger.error("DdpAedRuleCheking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) exception while iterating.",ex.getMessage());
        }
    
        logger.debug("DdpAedRuleCheking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) executed successfully.");
    
        return processResult;
    }

    
    public List<Integer> getDmsDdpRuleDetail()
    {
                    //logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) method invoked.");
                    
                    final List<DdpRule> ddpRuleList = null;
                    final List<DdpAedRule> ddpAesRuleList = null;
                    List<DdpRuleDetail> ddpRuleDetailList = null;
                    final List<DdpParty> ddpPartyList = null;
                    List<Integer> lsRdtIds = null;
                    
                    try
                    {
                                    /***************************************************************************************************************
                                    * &&&&&&&&&&&&&&&&&&     MAY REQUIRE FINE-TUNING - This queries all  AED_RULE records &&&&&&&&&&&&&&&&&&&&&&&&&
                                    * This has to modified to fetch few records that are relevant to the data in dmsDdpSynList, instead 
                                     * querying all AED_RULE records. Initially, this my be fine, since there will not be much records.
                                    * However, when AED_RULE records grows more that few thousands, it is recommended to fine tune this query
                                    ***************************************************************************************************************/
                                    lsRdtIds = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_AED_RULE, new RowMapper<Integer>() {
                                                                                                    
                                                    public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                                                                    
                                                                    
                                                                    
                                                                    int rdtId = rs.getInt("RDT_ID");
                                                                    
                                                                    /*
                                                                    ddpRuleDetail.setRdtRuleId(ddpRuleService.findDdpRule(rs.getInt("RDT_RULE_ID")));
                                                                    ddpRuleDetail.setRdtCompany(ddpCompanyService.findDdpCompany(rs.getString("RDT_COMPANY")));
                                                                    ddpRuleDetail.setRdtBranch(ddpBranchService.findDdpBranch(rs.getString("RDT_BRANCH")));
                                                                    ddpRuleDetail.setRdtDocType(ddpDoctypeService.findDdpDoctype(rs.getString("RDT_DOC_TYPE")));

                                                                    ddpRuleDetail.setRdtPartyCode(ddpPartyService.findDdpParty(rs.getString("RDT_PARTY_CODE")));
                                                                    ddpRuleDetail.setRdtPartyId(rs.getString("RDT_PARTY_ID"));
                                                                    ddpRuleDetail.setRdtStatus(rs.getInt("RDT_STATUS"));
                                                                    
                                                                    
                                                                                    DdpRule ddpRule = new DdpRule();
                                                                    
                                                                    ddpRule.setRulId(rs.getInt("RUL_ID"));
                                                                                    
                                                                    
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
                                                                    Calendar calDdsCreateDate = Calendar.getInstance();
                                                                    calDdsCreateDate.setTime(rs.getTimestamp("SYN_CREATED_DATE"));
                                                                    dmsDdpSyn.setSynCreatedDate(calDdsCreateDate);
                                                                    
                                                                    //dmsDdpSyn.setSynTboName(rs.getString("SYN_TBO_NAME"));*/
                                                                    
                                                                    return rdtId;
                               }
                                    });                           
                                    
                    }
                    catch(Exception ex)
                    {
                                    //logger.error("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) - Exception while accessing DdpDmsDocsHolder retrive GreaterThanMaxSynID [{}] records.",ddpDmsDocsHolder.getThlSynId());
                                    ex.printStackTrace();
                    }
                    
                    logger.debug("TaskUtil.getDmsDdpSynsGreaterThanMaxSynID(DdpDmsDocsHolder ddpDmsDocsHolder) executed successfully.");
                    return lsRdtIds;
    }
    
    public static void main(String args[]){
    	String temp = "GEODE/MUX";
    	temp = temp .replaceAll("[^\\w\\s\\-_]", "");
    	System.out.println(temp);
    }
    
    
    /**
     * Method used for getting all Ddp Rule Details based on the below matched criteria.
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
    		ruleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_RULE_DETAIL_ID,new Object[] {currentCalendar},new RuleEntity());
    	} catch (Exception ex) {
    		logger.error("DdpAedRuleChecking.getAllMatchedDdpRuleDetails() - Exception is occured while fetching using current date ",ex);
    	}
    	
    	return ruleDetails;
    }
    
    /**
     * Method used for getting all Ddp Rule Details based on the below matched criteria.
     * 
     * @param branch
     * @param company
     * @param docType
     * @param genSource
     * @param syncID
     * @return
     */
    private List<DdpRuleDetail> getAllMatchedDdpRuleDetails(String branch,String company,String docType,String genSource,Integer syncID,Calendar currentCalendar) {
    	
    	List<DdpRuleDetail> ruleDetails = null;
    	
    	try {
    		ruleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_RULE_DETAIL_ID_1,new Object[] {genSource,docType,branch,company,currentCalendar}, new RowMapper<DdpRuleDetail>() {
                
                public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
                	DdpRuleDetail ddpRuleDetail = ddpRuleDetailService.findDdpRuleDetail(rs.getInt("RDT_ID"));
                                
                               return ddpRuleDetail;
                }
    		});                           
		
		}
		catch(Exception ex)
		{
			logger.error("DdpAedRuleChecking.getAllMatchedDdpRuleDetails() - Exception while accessing DdpRuleDetail retrive based [{}] records for SYN_ID : "+syncID,ex);
			ex.printStackTrace();
		}

    	return ruleDetails;
    }
}
