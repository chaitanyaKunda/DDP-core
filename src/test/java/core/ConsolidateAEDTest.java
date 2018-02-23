/**
 * 
 */
package core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import com.agility.ddp.core.quartz.DdpMultiAedRuleJob;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;

/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
//@Transactional
public class ConsolidateAEDTest {
	
	@Autowired
	private DdpCategorizedDocsService ddpCategorizedDocsService;
	
    
	@Autowired
	private ApplicationContext applicationContext;	
	
	//@Test
	public void testConsolidatedAED() {
		
		DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(602882);    
				 
		Object[] object = new Object[]{ddpCategorizedDocs};
		DdpMultiAedRuleJob job = applicationContext.getBean("multiAedRuleJob", DdpMultiAedRuleJob.class);
		job.callWorkFlow(object);
	}
	
	//@Test
	public void testMultiJobNumberConsolidatedAED() {
		//13772 rule id , Job number : M3527104I7 - 36796,36877
		DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(36877);    
		 
		Object[] object = new Object[]{ddpCategorizedDocs};
		DdpMultiAedRuleJob job = applicationContext.getBean("multiAedRuleJob", DdpMultiAedRuleJob.class);
		job.callWorkFlow(object);
		//job.executeJob(ddpRuleDetails, ddpEmailTriggerSetup, null, null, 1);
	}

}
