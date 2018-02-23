package com.agility.ddp.core.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.agility.ddp.core.entity.RuleEntity;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
//import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DmsDdpSyn;

public interface Rule 
{
	//public Map<Integer, ArrayList<DdpRuleDetail>> chekRules(List<DmsDdpSyn> dmsDdpSynList);
	public Map<Integer, ArrayList<RuleEntity>> chekRules(List<DmsDdpSyn> dmsDdpSynList);
	public Map<Integer, ArrayList<Object>> processRules(List<DdpCategorizedDocs> ddpCategorizedDocsLst);
}
