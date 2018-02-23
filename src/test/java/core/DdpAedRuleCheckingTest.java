/**
 * 
 */
package core;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.agility.ddp.core.entity.RuleEntity;
import com.agility.ddp.core.rule.DdpAedRuleCheking;
import com.agility.ddp.data.domain.DdpRuleDetail;
import com.agility.ddp.data.domain.DdpRuleDetailService;
import com.agility.ddp.data.domain.DmsDdpSyn;
import com.agility.ddp.data.domain.DmsDdpSynService;

/**
 * @author DGuntha
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
@Transactional
public class DdpAedRuleCheckingTest {
	
	@Autowired
	private ApplicationContext applicationContext;	
	

	@Autowired
    DmsDdpSynService dmsDdpSynService;
	
	@Autowired
	private DdpAedRuleCheking ddpAedRuleCheking;
	
	@Autowired
	private DdpRuleDetailService ddpRuleDetailService;
	
	//@Test
	public void testAEDRuleDetails() {
		//Calendar currentCalendar = GregorianCalendar.getInstance();
		Calendar today = Calendar.getInstance();
        today.setTime(new Date());
        Calendar currentCalendar = Calendar.getInstance();
        currentCalendar.set(Calendar.YEAR, today.get(Calendar.YEAR) );
        currentCalendar.set(Calendar.MONTH, today.get(Calendar.MONTH)+1);
        currentCalendar.set(Calendar.DAY_OF_MONTH, today.get(Calendar.DAY_OF_MONTH));
        
		List<DdpRuleDetail> ddpZRuleDetail = ddpRuleDetailService.findAllDdpRuleDetails();
		long count = ddpZRuleDetail.stream().filter(o -> (o.getRdtStatus().intValue() == 0 && o.getRdtRuleType().equalsIgnoreCase("AED_RULE")) && (( (currentCalendar.equals(o.getRdtActivationDate()) ) ||   (currentCalendar.after(o.getRdtActivationDate()) )))).count();
		System.out.println("Size of matched documetns ; "+count);
	}
	
	@Test
	public void testAEDchekRules() {
		List<DmsDdpSyn> list = new ArrayList<DmsDdpSyn>();
		DmsDdpSyn syn =	dmsDdpSynService.findDmsDdpSyn(134997);
		list.add(syn);
		Map<Integer, ArrayList<RuleEntity>>  map =	ddpAedRuleCheking.chekRules(list);
		System.out.println("Size of map : "+map.size());
	}

}
