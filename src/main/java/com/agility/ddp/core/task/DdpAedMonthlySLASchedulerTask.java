/**
 * 
 */
package com.agility.ddp.core.task;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.TypedQuery;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.core.util.TaskUtil;
import com.agility.ddp.data.domain.DdpGenSourceSetup;
import com.agility.ddp.data.domain.DdpRuleDetail;

/**
 * @author dguntha
 *
 */
//@PropertySource("file:///E:/DDPConfig/mail.properties")
public class DdpAedMonthlySLASchedulerTask implements Task {
	
	private static final Logger logger = LoggerFactory.getLogger(DdpAedMonthlySLASchedulerTask.class);
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private TaskUtil taskUtil;
	
	@Autowired
	private ApplicationProperties env;
	

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.Task#execute()
	 */
	@Override
	public void execute() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.Task#execute(java.lang.String)
	 */
	@Override
	public void execute(String jobName) {
		
		Calendar startDate = GregorianCalendar.getInstance();
		Calendar endDate = GregorianCalendar.getInstance();
		startDate.add(Calendar.DAY_OF_MONTH, -1);
		
		logger.info("Inside the DdpAedMonthlySLASchedulerTask.execute(String jobName) & Job Name : "+jobName);
		
		// Need to fetch details from the ddp rule details base weekly selection.
		List<DdpRuleDetail> ruleDetails = commonUtil.fetchRuleDetailsBasedSLAFrequency(Constant.DDP_SQL_SELECT_AED_MONTHLY_SCH);
		
		if (ruleDetails != null && ruleDetails.size() > 0) {
			
			Map<String,String> ddpRuleMap = new HashMap<String, String>();
			Map<String,DdpRuleDetail> ddpRuleDetailMap = new HashMap<String, DdpRuleDetail>();
			
			for (DdpRuleDetail ddpRuleDetail : ruleDetails) {
				
				TypedQuery<DdpGenSourceSetup> genSourcelst1 = DdpGenSourceSetup.findDdpGenSourceSetupsByGssRdtId(ddpRuleDetail);
				DdpGenSourceSetup ddpGenSource = genSourcelst1.getResultList().get(0);
				
				String key = ddpRuleDetail.getRdtRuleId().getRulId()+""+ddpRuleDetail.getRdtDocType().getDtyDocTypeName()+""+ddpGenSource.getGssOption();
				if (!ddpRuleMap.containsKey(key)) {
					ddpRuleMap.put(key, ddpRuleDetail.getRdtId()+",");
				} else {
					String value = ddpRuleMap.get(key);
					value = value + ddpRuleDetail.getRdtId() + ",";
					ddpRuleMap.put(key, value);
				}
				ddpRuleDetailMap.put(key, ddpRuleDetail);
			}
			
			//Iterate each record find the result from categorize docs table.
			for (String key :  ddpRuleMap.keySet()) {
				
				DdpRuleDetail ddpRuleDetail = ddpRuleDetailMap.get(key);
				try {
						
						String value = ddpRuleMap.get(key);
						value = value.substring(0, value.length()-1);
						
						boolean isSLAStatified = true;
						List<Integer> availableCatIds = commonUtil.findCategorizedDocForSLAFrequency(value, startDate);
						int slaMin = (ddpRuleDetail.getRdtSlaMin() != null ? Integer.parseInt(ddpRuleDetail.getRdtSlaMin()) : 0);
						int slaMax = (ddpRuleDetail.getRdtSlaMax() != null ? Integer.parseInt(ddpRuleDetail.getRdtSlaMax()) : 0);
						String reson = "";
						if (availableCatIds == null && ( slaMin > 0 || slaMax < 0)) {
							isSLAStatified = false;
							reson = "No documents found";
						} else if (availableCatIds.size() < slaMin) {
							isSLAStatified = false;
							reson = "Documents sent to customer is less than SLA.";
						} else if (availableCatIds.size()  > slaMax) {
							isSLAStatified = false;
							reson = "Documents sent to customer is greater than SLA.";
						}
						
						// Based on the configuration if documents is less than or greater than the configured sla. Need to send mail.
						if (!isSLAStatified) {
							
							String smtpAddress = env.getProperty("mail.smtpAddress");
							String fromAddress = env.getProperty("mail.fromAddress");
							
							String toAddress = ddpRuleDetail.getRdtNotificationId().getNotSuccessEmailAddress();
							String subject = env.getProperty("mail.sla.subject");
							subject = subject.replaceAll("%%CLIENTVALUE%%", ddpRuleDetail.getRdtPartyId());
							subject = subject.replaceAll("%%DOCUMENTTYPE%%", ddpRuleDetail.getRdtDocType().getDtyDocTypeName());
							
							SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
							
							String body = env.getProperty("mail.sla.body");
							body = body.replaceAll("%%REASON%%", reson);
							body = body.replaceAll("%%DOCCOUNT%%", availableCatIds.size()+"");
							body = body.replaceAll("%%SLAMAXVALUE%%", slaMax+"");
							body = body.replaceAll("%%SLAMINVALUE%%", slaMin+"");
							body = body.replaceAll("%%STARTDATE%%", dateFormat.format(startDate.getTime()));
							body = body.replaceAll("%%ENDDATE%%", dateFormat.format(endDate.getTime()));
							body = body.replaceAll("%%CLIENTCODE%%", ddpRuleDetail.getRdtPartyCode().getPtyPartyName());
							body = body.replaceAll("%%CLIENTVALUE%%", ddpRuleDetail.getRdtPartyId());
							body = body.replaceAll("%%DOCUMENTTYPE%%", ddpRuleDetail.getRdtDocType().getDtyDocTypeName());
							body = body.replaceAll("%%RULEDETAILID%%", ddpRuleDetail.getRdtId()+"");
							body = body.replaceAll("%%COMPANYCODE%%", ddpRuleDetail.getRdtCompany().getComCompanyCode());
							body = body.replaceAll("%%RULEID%%", ddpRuleDetail.getRdtRuleId().getRulId()+"");
							
							taskUtil.sendMail(smtpAddress, toAddress, null, fromAddress, subject, body, null);
						}
					} catch (Exception ex) {
						logger.error("DdpAedMonthlySLASchedulerTask.execute(String jobName) - Unable to send details to customer for rule detail id : "+ddpRuleDetail.getRdtId(), ex.getMessage());
						taskUtil.sendMailByDevelopers(ex.getMessage(), " inside  DdpAedMonthlySLASchedulerTask.execute(String jobName) for Rule Detail ID : "+ddpRuleDetail.getRdtId());
					}
				
			}
		}
		
		logger.info("End of DdpAedMonthlySLASchedulerTask.execute(String jobName) & Job Name : "+jobName);
	}

	/* (non-Javadoc)
	 * @see com.agility.ddp.core.task.Task#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context) {
		// TODO Auto-generated method stub

	}

}
