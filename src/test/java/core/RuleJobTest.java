/**
 * 
 */
package core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.agility.ddp.core.quartz.RuleJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpRuleDetail;

/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" }) 
public class RuleJobTest {

	@Autowired
	private RuleJob ruleJob;
	
	@Autowired
	private DdpCategorizedDocsService ddpCategorizedDocsService;
	
	@Autowired
	private CommonUtil commonUtil;
	
//	@Test
	public void testcallWorkFlowMethod() throws Exception, IOException {
		//Properties p = new Properties();
		//p.load(new FileInputStream("E://DDPConfig//quartz.properties")); // path to your properties file
//		System.out.println(System.getProperty("org.quartz.threadPool.threadCount"));
//		System.out.println(System.getProperty("org.quartz.threadPool.threadPriority")); // prints 3
//		List<DdpRuleDetail> ddpRuleDetails = commonUtil.getMatchedSchdIDAndRuleDetailForExport(222);
//		System.out.println("Size of DdpRuleDetails : "+ddpRuleDetails.size());
//		
		DdpCategorizedDocs  categorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(759048);
		System.out.println("Categorized id : "+categorizedDocs.getCatId());
		StdSchedulerFactory stdFact = new StdSchedulerFactory();
		Object[] object = {categorizedDocs,stdFact.getScheduler(),1};
		ruleJob.callWorkFlow(object);
	}
}
