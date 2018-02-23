/**
 * 
 */
package core;

import javax.transaction.Transactional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.quartz.DdpRuleSchedulerJob;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;

/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
//@Transactional
public class DdpExportReportTest {
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	
	//@Test	
	public void testSchedulerReports() {
		
		//PRoduction
		DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(11);
		Object[] objArry = {ddpScheduler};
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		schedulerJob.initiateSchedulerReport(objArry);
		
	}

}
