/**
 * 
 */
package core;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.agility.ddp.core.quartz.DdpRuleSchedulerJob;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;

/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
public class DdpExportRuleClientIDSetupTest {

	   
	@Autowired
	private ApplicationContext applicationContext;	
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
		
//	@Test
	public void testGeneralProcess() {
		
		 DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		
		 DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(48);
		 
		 schedulerJob.executeSchedulerJob(ddpScheduler,1);
		 
		 
	}
	
	//@Test
	public void testOnDemandGeneralProcess() {
	
		Calendar queryStartDate = GregorianCalendar.getInstance();
		queryStartDate.add(Calendar.DAY_OF_YEAR, -2);
		
		Calendar queryEndDate = GregorianCalendar.getInstance();
		//queryStartDate.add(Calendar.DAY_OF_YEAR, -2);
		
		 DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
					"schedulerJob", DdpRuleSchedulerJob.class);
			
			 DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(63);
			 
			 schedulerJob.executeOnDemandScheduler(ddpScheduler, queryStartDate, queryEndDate, queryEndDate, 2, null, null, null);
	}
}
