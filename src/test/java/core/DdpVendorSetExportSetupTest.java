/**
 * 
 */
package core;

import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.transaction.Transactional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.quartz.DdpAutoLivRuleSchedulerJob;
import com.agility.ddp.core.quartz.DdpRuleSchedulerJob;
import com.agility.ddp.core.quartz.DdpVendorRuleSchedulerJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;

/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
//@Transactional
public class DdpVendorSetExportSetupTest {
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	@Autowired
	private CommonUtil commonUtil;

	//@Test
	public void testVendorSetRuleByQuery() {
		//13778 - UAT rule id.
		
		Calendar fromDate = GregorianCalendar.getInstance();
		fromDate.add(Calendar.DAY_OF_YEAR, -5);
		Calendar toDate = GregorianCalendar.getInstance();
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		
		//UAT - 13616
		DdpScheduler ddpScheduler = schedulerJob.getSchedulerDetails(13778);
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
		
		String strBeanName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".beanName");
		String strClassName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".className");

		if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
				&& strClassName != null) {
		
			try {
			DdpVendorRuleSchedulerJob job = (DdpVendorRuleSchedulerJob) applicationContext
					.getBean(strBeanName, Class.forName(strClassName));
			job.runOnDemandService(ddpScheduler, ddpExportRule, ddpScheduler.getSchRuleCategory(), 1, fromDate, toDate, toDate, null, null,  null);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
	}
}
