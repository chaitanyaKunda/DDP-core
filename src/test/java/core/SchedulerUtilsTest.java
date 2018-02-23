/**
 * 
 */
package core;

import java.util.Properties;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"file:///E:/DDPTestConfig/data/applicationContext*.xml" ,"file:///E:/DDPTestConfig/core/applicationContext*.xml"})
public class SchedulerUtilsTest {

	@Resource(name = "quartzProperties")
	private  Properties quartzProperties;
	
	//@Test
	public void testSchedulerUtilsMethod() {
		try {
			
			//System.out.println(" Quartiz propertes : "+quartzProperties.getProperty("org.quartz.scheduler.instanceName"));
		//Scheduler scheduler = SchedulerUtils.getQuartzScheduler("Test1");
		//System.out.println(scheduler.getSchedulerName());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
