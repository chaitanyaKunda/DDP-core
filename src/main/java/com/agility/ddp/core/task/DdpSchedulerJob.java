package com.agility.ddp.core.task;

import java.io.File;
import java.util.Calendar;

import com.agility.ddp.data.domain.DdpScheduler;

/**
 * @author DGuntha
 *
 */
public interface DdpSchedulerJob {

	/**
	 * Method used to call on the OnDemand.
	 * 
	 * @param ddpScheduler
	 * @return
	 */
	public void runOnDemandRuleJob(DdpScheduler ddpScheduler,Calendar fromDate,Calendar toDate);
	
	/**
	 * Method used for re-process the rule.
	 * 
	 * @param appName
	 * @param currentDate
	 */
	public void reprocesRuleJob(String appName,Calendar currentDate,Calendar startDate);
	
	/**
	 * Method used for fetch documents based on provided Job Number.
	 * 
	 * @param ddpScheduler
	 * @param fromDate
	 * @param toDate
	 * @param jobNumbers
	 */
	public void runOnDemandRuleBasedJobNumber(DdpScheduler ddpScheduler,Calendar fromDate,Calendar toDate,String jobNumbers);
	
	/**
	 * Method used for fetching documents based on provided Consignment IDs.
	 * 
	 * @param ddpScheduler
	 * @param fromDate
	 * @param toDate
	 * @param consignmentIDs
	 */
	public void runOnDemandRuleBasedConsignmentId(DdpScheduler ddpScheduler,Calendar fromDate,Calendar toDate,String consignmentIDs);
	
	/**
	 * Method used for fetching the documents based on Document References.
	 * 
	 * @param ddpScheduler
	 * @param fromDate
	 * @param toDate
	 * @param docRefs
	 */
	public void runOnDemandRuleBasedDocRef(DdpScheduler ddpScheduler,Calendar fromDate,Calendar toDate,String docRefs);
	
	/**
	 * Method used for generating the reports.
	 * 
	 * @param ddpScheduler
	 * @param fromDate
	 * @param toDate
	 */
	public File runOnDemandRulForReports(DdpScheduler ddpScheduler,Calendar fromDate,Calendar toDate,String typeOfStatus);
	
	
	/**
	 * Method used for creating the reports.
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerReport(Object[] ddpSchedulerObj);
	
	/**
	 * 
	 * @param ddpSchedulerObj
	 */
	public void initiateSchedulerJob(Object[] ddpSchedulerObj);
}
