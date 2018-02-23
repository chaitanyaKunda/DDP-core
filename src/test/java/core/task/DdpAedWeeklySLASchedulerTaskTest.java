/**
 * 
 */
package core.task;

import static org.junit.Assert.*;

import java.util.Date;

import javax.transaction.Transactional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.quartz.Calendar;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.agility.ddp.core.task.DdpAedWeeklySLASchedulerTask;

/**
 * @author DGuntha
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
@Transactional
public class DdpAedWeeklySLASchedulerTaskTest {

	@Autowired
	private ApplicationContext applicationContext;
	
	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpAedWeeklySLASchedulerTask#execute()}.
	 */
	@Test
	public void testExecute() {
		
		DdpAedWeeklySLASchedulerTask ddpAedDailySLASchedulerTask = applicationContext.getBean("ddpAedWeeklySLASchedulerTask", DdpAedWeeklySLASchedulerTask.class);
		ddpAedDailySLASchedulerTask.execute();
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpAedWeeklySLASchedulerTask#execute(java.lang.String)}.
	 */
	@Test
	public void testExecuteString() {
		DdpAedWeeklySLASchedulerTask ddpAedDailySLASchedulerTask = applicationContext.getBean("ddpAedWeeklySLASchedulerTask", DdpAedWeeklySLASchedulerTask.class);
		ddpAedDailySLASchedulerTask.execute("TestAedWeeklyTask");
	}

	/**
	 * Test method for {@link com.agility.ddp.core.task.DdpAedWeeklySLASchedulerTask#execute(org.quartz.JobExecutionContext)}.
	 */
	@Test
	public void testExecuteJobExecutionContext() {
		DdpAedWeeklySLASchedulerTask ddpAedDailySLASchedulerTask = applicationContext.getBean("ddpAedWeeklySLASchedulerTask", DdpAedWeeklySLASchedulerTask.class);
		ddpAedDailySLASchedulerTask.execute(new JobExecutionContext() {
			
			@Override
			public void setResult(Object result) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void put(Object key, Object value) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public boolean isRecovering() {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public Trigger getTrigger() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Scheduler getScheduler() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Date getScheduledFireTime() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Object getResult() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public int getRefireCount() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public TriggerKey getRecoveringTriggerKey() throws IllegalStateException {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Date getPreviousFireTime() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Date getNextFireTime() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public JobDataMap getMergedJobDataMap() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public long getJobRunTime() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public Job getJobInstance() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public JobDetail getJobDetail() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Date getFireTime() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getFireInstanceId() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Calendar getCalendar() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Object get(Object key) {
				// TODO Auto-generated method stub
				return null;
			}
		});
	}

}
