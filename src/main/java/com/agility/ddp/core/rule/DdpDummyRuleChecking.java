package com.agility.ddp.core.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.agility.ddp.core.entity.RuleEntity;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DmsDdpSyn;

//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class DdpDummyRuleChecking implements Rule 
{
	private static final Logger logger = LoggerFactory.getLogger(DdpDummyRuleChecking.class);
	
//	@Autowired
//	Environment env;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	public Map<Integer, ArrayList<RuleEntity>> chekRules(List<DmsDdpSyn> dmsDdpSynList)
	{
		logger.debug("DdpDummyRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) method invoked.");
		
		Map<Integer, ArrayList<RuleEntity>> ruleMap = new HashMap<Integer, ArrayList<RuleEntity>>();
		
		try
		{
			
		}
		catch(Exception ex)
		{
			
		}
		
		logger.debug("DdpDummyRuleChecking.chekRules(List<DmsDdpSyn> dmsDdpSynList) executed successfully.");
		return ruleMap;
	}
	
	public Map<Integer, ArrayList<Object>> processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst)
    {
        logger.debug("DdpDummyRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) method invoked.");
        
        Map<Integer, ArrayList<Object>> processResult = new HashMap<Integer, ArrayList<Object>>();
                
        try
        {
        	
        }
        catch(Exception ex)
        {
        	logger.debug("DdpDummyRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) exception while iterating.",ex.getMessage());
        }
    
        logger.debug("DdpDummyRuleChecking.processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst) executed successfully.");
    
        return processResult;
    }
}
