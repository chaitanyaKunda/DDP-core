package com.agility.ddp.core.rule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.persistence.TypedQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.agility.ddp.core.entity.DdpRateSetupEntity;
import com.agility.ddp.core.entity.RuleEntity;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpDmsDocsDetailService;
import com.agility.ddp.data.domain.DdpGenSourceSetup;
import com.agility.ddp.data.domain.DdpParty;
import com.agility.ddp.data.domain.DdpRateSetup;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DmsDdpSyn;

//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class DdpExportRuleChecking implements Rule
{
	private static final Logger logger = LoggerFactory.getLogger(DdpExportRuleChecking.class);
	
//	@Autowired
//	private ApplicationProperties env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
		
	@Autowired
	DdpRuleDetailService ddpRuleDetailService;
	
	@Autowired
	DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	DdpDmsDocsDetailService ddpDmsDocsDetailService;
	
	@Autowired
	private CommonUtil commonUtil;
	
	//Get the documentum Session
//    @Value( "${dms.docbaseName}" )
//	String docbaseName  ;
//    //String docbaseName = "ZUSLogistics1UAT" ;
//			
//	@Value( "${dms.userName}" )
//    String userName ;
//	//String userName="dmadminuat" ;
//	
//	@Value( "${dms.password}" )
//    String password ;
//	//String password ="agildcmnsy";
//	
//	@Value( "${ddp.export.folder}" )
//    String exportFolder ;
	//String exportFolder ="C:\\data\\downloaded";

	//IDfSession session;
   // IDfSessionManager sMgr =null;
	
	
	public Map<Integer, ArrayList<RuleEntity>> chekRules(List<DmsDdpSyn> dmsDdpSynList)
	{
		logger.info("DdpExportRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) method invoked.");
		
		Map<Integer, ArrayList<RuleEntity>> ruleMap = new ConcurrentHashMap<Integer, ArrayList<RuleEntity>>();
		
	    Calendar currentCalendar = Calendar.getInstance();
    
	    try
		{
	    	
	    	List<RuleEntity> ddpZRuleDetail = getAllMatchedDdpRuleDetails(currentCalendar);
			 
			if (ddpZRuleDetail == null || ddpZRuleDetail.size() == 0) {
				logger.debug("DdpExportRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) : Empty list of DDP_RULE_DETAIL ");
				return ruleMap;
			}
			
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
//	                    String strRdtCompany	= (ddpRuleDetail.getRdtCompany() != null ? ddpRuleDetail.getRdtCompany().getComCompanyCode() : "") ;
//			            String strRdtBranch = (ddpRuleDetail.getRdtBranch() != null ? ddpRuleDetail.getRdtBranch().getBrnBranchCode() : "");
//			            String strRdtDocType = (ddpRuleDetail.getRdtDocType() != null ? ddpRuleDetail.getRdtDocType().getDtyDocTypeCode() : "");
//	                   
//			            if((strRdtCompany.equalsIgnoreCase(dmsDdpSyn.getSynCompanySource()) ) 
//	                    		&& ((strRdtBranch.equalsIgnoreCase(dmsDdpSyn.getSynBranchSource())|| "All".equalsIgnoreCase(strRdtBranch)))
//	                    		&& (strRdtDocType.equalsIgnoreCase(dmsDdpSyn.getSynDocType()))) {
			            
				            	
	                    	String party_code = ddpRuleDetail.getRdtPartyCode();
		                    
		                    if(party_code == null )
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
                              
                                
                                List<String> partyIDs = new ArrayList<String>();
                                String[] partys = ddpRuleDetail.getRdtPartyId().split(",");
                                for (String partyid : partys) {
                                	partyIDs.add(partyid.toLowerCase().trim());
                                }
                                if(partyIDs.contains(partyCodeValue.toLowerCase().trim()))
                                {
                                	 DdpGenSourceSetup genSourceSetup =  commonUtil.getMatchedGeneratedSourceSetup(ddpRuleDetail.getRdtId());
                               	  	 String genSource = (genSourceSetup != null ? genSourceSetup.getGssOption() : "");
                                	
                                	//Checking the Generating source matching 
                                	if (dmsDdpSyn.getSynGenSystem().equalsIgnoreCase(genSource) || genSource.equalsIgnoreCase("any") || (genSource.equalsIgnoreCase("3rd Party") && !dmsDdpSyn.getSynGenSystem().equalsIgnoreCase("Control"))) {
                                	
                                		//Checking the RATED & NON-RATED option.
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
	                   //  }
	            	
	    	     if(! lstruledetails.isEmpty() ) 
	    	    	 ruleMap.put(dmsDdpSyn.getSynId(),lstruledetails); 
		    } 
		    else {
				logger.debug("DdpExportRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) :No Branch or No Company or No Doc Type for  SYN_ID:"+dmsDdpSyn.getSynId());
			}
		   }
		 }
		catch(Exception ex)
		{
			logger.error("DdpExportRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) : Exception while matching Export rule - Error message [{}]",ex.getMessage());
			ex.printStackTrace();
		}
		
		logger.info("DdpExportRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
		return ruleMap;
	}
	
	public Map<Integer, ArrayList<Object>> processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst)
    {
        logger.debug("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) method invoked.");
        
        Map<Integer, ArrayList<Object>> processResult = new HashMap<Integer, ArrayList<Object>>();
                
      /*  try
        {
        	
        	IDfClientX clientx = new DfClientX();
		    IDfClient  client = clientx.getLocalClient();
		    
		    sMgr = client.newSessionManager();
		    
		    IDfLoginInfo loginInfoObj = clientx.getLoginInfo();
		    
		    loginInfoObj.setUser(userName);
		    loginInfoObj.setPassword(password);
		    sMgr.setIdentity(docbaseName, loginInfoObj);
		    
		    logger.info("Export process started for creating session manager.");
		    
		    session = sMgr.getSession(docbaseName);
        	
        	for(DdpCategorizedDocs categorizedDocs :  ddpCategorizedDocsLst)
        	{
        		if(categorizedDocs.getCatRuleType().equalsIgnoreCase("EXPORT_RULE"))
        		{
        			
        			TypedQuery<DdpDmsDocsDetail> finderResult = DdpDmsDocsDetail.findDdpDmsDocsDetailsByDddDtxId(categorizedDocs.getCatDtxId());
        			List<DdpDmsDocsDetail> ddpDmsDocsDetails = finderResult.getResultList();
        			
        			DdpDmsDocsDetail ddpDmsDocsDetail = ddpDmsDocsDetails.get(0); //Assumption is that DDP_DMS_DOCS_DETAIL will have only one value. If it existing more than once, it still pointing to same DTX_ID. 
        					
        			String strFileName = new DdpDFCClient().exportDocumentToLocal(ddpDmsDocsDetail.getDddId(), ddpDmsDocsDetail.getDddRObjectId(), exportFolder, session);
        			
        			//Document pooled for processing - status changed to 100
        			categorizedDocs.setCatStatus(100);
        			ddpCategorizedDocsService.updateDdpCategorizedDocs(categorizedDocs);
        			
        			logger.debug("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) - File [{}] exported successfully.",strFileName);
        		}
        	}
        }
        catch(Exception ex)
        {
        	logger.error("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) exception while iterating.",ex.getMessage());
        }*/
    
        logger.debug("DdpExportRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) executed successfully.");
    
        return processResult;
    }
	
	public List<Integer> getDmsDdpRuleDetailForExport()
    {
		logger.debug("DdpExportRuleChecking.getDmsDdpRuleDetailForExport() method invoked.");
                   
		List<Integer> lsRdtIds = null;
                    
		try
        {
			lsRdtIds = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_EXPORT_RULE, new RowMapper<Integer>() {
            
				public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					int rdtId = rs.getInt("RDT_ID");
					return rdtId;
				}
			});
		}
        catch(Exception ex)
        {
        	logger.error("DdpExportRuleChecking.getDmsDdpRuleDetailForExport() - Exception while retrieving Export rules for checking - Error message [{}].",ex.getMessage());
        	ex.printStackTrace();
        }
        
        logger.info("DdpExportRuleChecking.getDmsDdpRuleDetailForExport() executed successfully.");
        return lsRdtIds;
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
//    private List<DdpRuleDetail> getAllMatchedDdpRuleDetails(String branch,String company,String docType,String genSource,Integer syncID,Calendar currentCalendar) {
//    	
//    	List<DdpRuleDetail> ruleDetails = null;
//    	
//    	try {
//    		ruleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_EXPORT_RULE_ID,new Object[] {genSource,docType,branch,company,currentCalendar}, new RowMapper<DdpRuleDetail>() {
//                
//                public DdpRuleDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
//                	DdpRuleDetail ddpRuleDetail = ddpRuleDetailService.findDdpRuleDetail(rs.getInt("RDT_ID"));
//                                
//                                return ddpRuleDetail;
//                }
//    		});                           
//		
//		}
//		catch(Exception ex)
//		{
//			logger.error("DdpAedRuleChecking.getAllMatchedDdpRuleDetails() - Exception while accessing DdpRuleDetail retrive based [{}] records for SYN_ID : "+syncID,ex);
//			ex.printStackTrace();
//		}
//
//    	return ruleDetails;
//    }
	
	  private List<RuleEntity> getAllMatchedDdpRuleDetails(Calendar currentCalendar) {
	    	
	    	List<RuleEntity> ruleDetails = null;
	    	
	    	try {
	    		ruleDetails = this.jdbcTemplate.query(Constant.DEF_SQL_SELECT_MATCHED_EXPORT_RULE_ID,new Object[] {currentCalendar},new RuleEntity());                          
			
			}
			catch(Exception ex)
			{
				logger.error("DdpExportRuleChecking.getAllMatchedDdpRuleDetails() - Exception while accessing RuleEntity retrive based [{}] records.",ex);
				ex.printStackTrace();
			}

	    	return ruleDetails;
	    }
	
}
