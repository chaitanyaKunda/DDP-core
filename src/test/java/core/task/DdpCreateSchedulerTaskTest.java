/**
 * 
 */
package core.task;

import static org.junit.Assert.*;

import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.transaction.Transactional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.agility.ddp.core.task.DdpCategorizationTask;
import com.agility.ddp.core.task.DdpCreateSchedulerTask;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;

/**
 * @author DGuntha
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
@Transactional
@Rollback(true)
public class DdpCreateSchedulerTaskTest {

	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#execute()}.
	 */
	@Test
	public void testExecute() {
		
		DdpCreateSchedulerTask ddpCreateSchedulerTask = applicationContext.getBean("ddpCreateSchedulerTask",DdpCreateSchedulerTask.class);
		ddpCreateSchedulerTask.execute();
		
		
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#execute(org.quartz.JobExecutionContext)}.
	 */
	@Test
	public void testExecuteJobExecutionContext() {
		//fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#execute(java.lang.String)}.
	 */
	@Test
	public void testExecuteString() {
		DdpCreateSchedulerTask ddpCreateSchedulerTask = applicationContext.getBean("ddpCreateSchedulerTask",DdpCreateSchedulerTask.class);
		assertEquals(ddpCreateSchedulerTask.getStrJobId(),DdpCategorizationTask.class.getCanonicalName());
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#createNewScheduler(com.agility.ddp.data.domain.DdpScheduler)}.
	 */
	@Test
	public void testCreateNewScheduler() {
		
		List<DdpScheduler> list =  ddpSchedulerService.findAllDdpSchedulers();
		DdpScheduler ddpScheduler = list.stream().filter(o -> o.getSchStatus() == 0).limit(1).collect(Collectors.toList()).get(0);
		assertEquals(ddpScheduler.getSchStatus().intValue(), 0);
		DdpCreateSchedulerTask ddpCreateSchedulerTask = applicationContext.getBean("ddpCreateSchedulerTask",DdpCreateSchedulerTask.class);
		ddpCreateSchedulerTask.createNewScheduler(ddpScheduler);
		assertEquals(ddpScheduler.getSchIsRunning().intValue(), 1);
		
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#createCronExpressionForSchedular(com.agility.ddp.data.domain.DdpScheduler)}.
	 */
	@Test
	public void testCreateCronExpressionForSchedular() {
		
		List<DdpScheduler> list =  ddpSchedulerService.findAllDdpSchedulers();
		DdpScheduler ddpScheduler = list.stream().filter(o -> o.getSchStatus() == 0).limit(1).collect(Collectors.toList()).get(0);
		assertEquals(ddpScheduler.getSchStatus().intValue(), 0);
		DdpCreateSchedulerTask ddpCreateSchedulerTask = applicationContext.getBean("ddpCreateSchedulerTask",DdpCreateSchedulerTask.class);
		CronTriggerFactoryBean cbt = ddpCreateSchedulerTask.createCronExpressionForSchedular(ddpScheduler);
		assertNotNull(cbt);
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#updateSchedularJob(com.agility.ddp.data.domain.DdpScheduler)}.
	 */
	@Test
	public void testUpdateSchedularJob() {
		
		List<DdpScheduler> list =  ddpSchedulerService.findAllDdpSchedulers();
		DdpScheduler ddpScheduler = list.stream().filter(o -> o.getSchStatus() == 0).limit(1).collect(Collectors.toList()).get(0);
		assertEquals(ddpScheduler.getSchStatus().intValue(), 0);
		DdpCreateSchedulerTask ddpCreateSchedulerTask = applicationContext.getBean("ddpCreateSchedulerTask",DdpCreateSchedulerTask.class);
		ddpCreateSchedulerTask.updateSchedularJob(ddpScheduler);
		
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpCreateSchedulerTask#createCronExpressionForSchedulerReports(com.agility.ddp.data.domain.DdpScheduler)}.
	 */
	@Test
	public void testCreateCronExpressionForSchedulerReports() {
		
		List<DdpScheduler> list =  ddpSchedulerService.findAllDdpSchedulers();
		DdpScheduler ddpScheduler = list.stream().filter(o -> o.getSchStatus() == 0).limit(1).collect(Collectors.toList()).get(0);
		assertEquals(ddpScheduler.getSchStatus().intValue(), 0);
		DdpCreateSchedulerTask ddpCreateSchedulerTask = applicationContext.getBean("ddpCreateSchedulerTask",DdpCreateSchedulerTask.class);
		CronTriggerFactoryBean cbt = ddpCreateSchedulerTask.createCronExpressionForSchedulerReports(ddpScheduler);
		assertNotNull(cbt);
	}

}
