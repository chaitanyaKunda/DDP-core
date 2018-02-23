/**
 * 
 */
package core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;







import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.core.monitor.MonitoringObserver;
import com.agility.ddp.core.quartz.DdpAutoLivRuleSchedulerJob;
import com.agility.ddp.core.quartz.DdpRuleSchedulerJob;
import com.agility.ddp.core.task.DdpSchedulerJob;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.Constant;
import com.agility.ddp.data.domain.DdpCategorizedDocs;
import com.agility.ddp.data.domain.DdpCategorizedDocsService;
import com.agility.ddp.data.domain.DdpExportMissingDocs;
import com.agility.ddp.data.domain.DdpExportMissingDocsService;
import com.agility.ddp.data.domain.DdpExportRule;
import com.agility.ddp.data.domain.DdpExportSuccessReport;
import com.agility.ddp.data.domain.DdpExportSuccessReportService;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.agility.ddp.data.domain.DdpJobRefHolderService;
import com.agility.ddp.data.domain.DdpScheduler;
import com.agility.ddp.data.domain.DdpSchedulerService;

/**
 * @author DGuntha
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = {"classpath*:META-INF/spring/applicationContext*.xml" })
//@Transactional
public class DdpExportSetupTest {

	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private ApplicationProperties env;
	
	@Autowired
	private DdpSchedulerService ddpSchedulerService;
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Autowired
	private DdpExportSuccessReportService ddpExportSuccessReportService;
	
	@Autowired
	private DdpExportMissingDocsService ddpExportMissingDocsService;
	
	@Autowired
	private MonitoringObserver monitoringObserver;
	
	@Autowired
	private DdpJobRefHolderService ddpJobRefHolderService;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private DdpCategorizedDocsService ddpCategorizedDocsService;
	
	
	//@Test
	public void testAutoLivRuleByQuery() {
		
		Calendar fromDate = GregorianCalendar.getInstance();
		fromDate.add(Calendar.MONTH, -2);
		Calendar toDate = GregorianCalendar.getInstance();
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		
		//UAT - 13616
		DdpScheduler ddpScheduler = schedulerJob.getSchedulerDetails(13616);
		DdpExportRule ddpExportRule = commonUtil.getExportRuleBasedOnSchedulerID(ddpScheduler.getSchId(),ddpScheduler.getSchRuleCategory());
		
		String strBeanName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".beanName");
		String strClassName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".className");

		if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
				&& strClassName != null) {
		
			try {
			DdpAutoLivRuleSchedulerJob job = (DdpAutoLivRuleSchedulerJob) applicationContext
					.getBean(strBeanName, Class.forName(strClassName));
			job.runOnDemandService(ddpScheduler, ddpExportRule, ddpScheduler.getSchRuleCategory(), 2, fromDate, toDate, toDate, null, null, null, null);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}
	
	//@Test
	public void testRuleByClientIDSetup() {
		Calendar fromDate = GregorianCalendar.getInstance();
		fromDate.add(Calendar.MONTH, -4);
		Calendar toDate = GregorianCalendar.getInstance();
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		//UAT - 13758(GEONZ/AKL)
		DdpScheduler ddpScheduler =  ddpSchedulerService.findDdpScheduler(52);
		//schedulerJob.executeSchedulerJob(ddpScheduler, 1);
		schedulerJob.executeSchedulerWithDateRange(ddpScheduler, fromDate, toDate, toDate, 1, null, null, null);
	}
	
	//@Test
	public void testRuleByQuerySetups() {
		
		Calendar fromDate = GregorianCalendar.getInstance();
		fromDate.add(Calendar.MONTH, -1);
		Calendar toDate = GregorianCalendar.getInstance();
		String jobNumbers = null;//"LA429389E9,SAN35979E1,SAN35978E3,SAN35982E1,SAN35980E5";
		String consignmentIDs = null;//"BUD0085167,SEL0407293,SEL0407294,SEL0407288,SEL0407292";
		String docRefs = "BCN-0450435/001,00BC499177";
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		//AutoLiv
		//UAT - 13749 - 13758(GEONZ/AKL)
		DdpScheduler ddpScheduler = schedulerJob.getSchedulerDetails(13758);
		String strBeanName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".beanName");
		String strClassName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".className");

		if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
				&& strClassName != null) {
			try {
				DdpSchedulerJob job = (DdpSchedulerJob) applicationContext
						.getBean(strBeanName, Class.forName(strClassName));
				job.runOnDemandRuleBasedJobNumber(ddpScheduler, fromDate, toDate, jobNumbers);
				Thread.sleep(100000);
				job.runOnDemandRuleBasedConsignmentId(ddpScheduler, fromDate, toDate, consignmentIDs);
				Thread.sleep(100000);
				job.runOnDemandRuleBasedDocRef(ddpScheduler, fromDate, toDate, docRefs);
				Thread.sleep(100000);
				// logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",strBeanName,strClassName,"SCHEDULER : "+ddpScheduler.getSchId());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else {
//			schedulerJob.runOnDemandRuleJob(ddpScheduler,
//					fromDate,
//					toDate);
			schedulerJob.executeOnDemandScheduler(ddpScheduler, fromDate, toDate, toDate, 2, jobNumbers, consignmentIDs, docRefs);
		}
		
//		consignmentIDs = "IAH0382587,IAH0382991,IAH0382650";
//		jobNumbers = "16110620F3,16110772F9,16110736F2";
//		docRefs = "0000390340,0000390301,0000390299";
//		//NOV
//				 ddpScheduler = schedulerJob.getSchedulerDetails(1776);
//				 strBeanName = env.getProperty("export.rule."
//						+ ddpScheduler.getSchRuleCategory() + ".beanName");
//				 strClassName = env.getProperty("export.rule."
//						+ ddpScheduler.getSchRuleCategory() + ".className");
//
//				if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
//						&& strClassName != null) {
//					try {
//						DdpSchedulerJob job = (DdpSchedulerJob) applicationContext
//								.getBean(strBeanName, Class.forName(strClassName));
//						job.runOnDemandRuleBasedJobNumber(ddpScheduler, fromDate, toDate, jobNumbers);
//						Thread.sleep(10000);
//						job.runOnDemandRuleBasedConsignmentId(ddpScheduler, fromDate, toDate, consignmentIDs);
//						Thread.sleep(100000);
//						job.runOnDemandRuleBasedDocRef(ddpScheduler, fromDate, toDate, docRefs);
//						Thread.sleep(100000);
//								
//						// logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",strBeanName,strClassName,"SCHEDULER : "+ddpScheduler.getSchId());
//					} catch (Exception ex) {
//						ex.printStackTrace();
//					}
//				} else {
//					schedulerJob.runOnDemandRuleJob(ddpScheduler,
//							fromDate,
//							toDate);
//				}
//				
//				consignmentIDs = "HAN0094645,HAN0093535,HAN0094334";
//				jobNumbers = "AEH52409F8,AEH51821E7,AEH52216E8";
//				docRefs = "06062016100613,06062016090652,06062016080651";
//				ddpScheduler = schedulerJob.getSchedulerDetails(1774);
//				 strBeanName = env.getProperty("export.rule."
//						+ ddpScheduler.getSchRuleCategory() + ".beanName");
//				 strClassName = env.getProperty("export.rule."
//						+ ddpScheduler.getSchRuleCategory() + ".className");
//
//				if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
//						&& strClassName != null) {
//					try {
//						DdpSchedulerJob job = (DdpSchedulerJob) applicationContext
//								.getBean(strBeanName, Class.forName(strClassName));
//						job.runOnDemandRuleBasedJobNumber(ddpScheduler, fromDate, toDate, jobNumbers);
//						Thread.sleep(100000);
//						job.runOnDemandRuleBasedConsignmentId(ddpScheduler, fromDate, toDate, consignmentIDs);
//						Thread.sleep(100000);
//						job.runOnDemandRuleBasedDocRef(ddpScheduler, fromDate, toDate, docRefs);
//						Thread.sleep(100000);
//						Thread.sleep(100000);
//						Thread.sleep(100000);
//						// logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",strBeanName,strClassName,"SCHEDULER : "+ddpScheduler.getSchId());
//					} catch (Exception ex) {
//						ex.printStackTrace();
//					}
//				} else {
//					schedulerJob.runOnDemandRuleJob(ddpScheduler,
//							fromDate,
//							toDate);
//				}
		
	}
	//@Test
	public void testOnDemandReportExport() {
		
		Calendar fromDate = GregorianCalendar.getInstance();
		fromDate.add(Calendar.MONTH, -12);
		Calendar toDate = GregorianCalendar.getInstance();
		
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		
		DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(36);
		schedulerJob.runOnDemandRulForReports(ddpScheduler,	fromDate,toDate,Constant.EXECUTION_STATUS_SUCCESS);
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	//@Test
	
	public void testGeneralReportExport() {
		
		try {
			Calendar calendar = GregorianCalendar.getInstance();
			DdpExportSuccessReport ddpExportSuccessReport = commonUtil.constructExportReportDomainObject(10274, 1777, "ABCDEFGH10", "10ABCDEFGH", "RuleByClientID", "HelloWorld.pdf", 1234, 1, "ABCDEFGH", "ABCDEFGHJOB", calendar);
			ddpExportSuccessReportService.saveDdpExportSuccessReport(ddpExportSuccessReport);
			DdpExportMissingDocs docs = constructExportMissingDocs();
			ddpExportMissingDocsService.saveDdpExportMissingDocs(docs);
			//Thread.sleep(500000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//@Test
	public void testOnDemandService() {
		
		
		Calendar fromDate = GregorianCalendar.getInstance();
		fromDate.add(Calendar.DAY_OF_MONTH, -5);
		Calendar toDate = GregorianCalendar.getInstance();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss") ;
		
    	String strStartDate = dateFormat.format(fromDate.getTime());
    	String strEndDate = dateFormat.format(toDate.getTime());
    	
		DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		
		DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(36);
		
		String strBeanName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".beanName");
		String strClassName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".className");

		if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
				&& strClassName != null) {
			try {
				DdpSchedulerJob job = (DdpSchedulerJob) applicationContext
						.getBean(strBeanName, Class.forName(strClassName));
				job.runOnDemandRuleJob(ddpScheduler, fromDate, toDate);
				Thread.sleep(100000);
			//	job.runOnDemandRuleBasedConsignmentId(ddpScheduler, fromDate, toDate, consignmentIDs);
				Thread.sleep(100000);
				//job.runOnDemandRuleBasedDocRef(ddpScheduler, fromDate, toDate, docRefs);
				Thread.sleep(500000);
				// logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",strBeanName,strClassName,"SCHEDULER : "+ddpScheduler.getSchId());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else {
			schedulerJob.runOnDemandRuleJob(ddpScheduler,
					fromDate,
					toDate);
			try {
			Thread.sleep(200000);
			} catch (Exception ex) {
				
			}
		}
	}
	
	//@Test
	public void testGeneralExportScheduler() {
		DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(37);
		 DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
					"schedulerJob", DdpRuleSchedulerJob.class);
		 schedulerJob.initiateSchedulerJob(new Object[]{ddpScheduler});
		 try {
			 
			 Thread.sleep(100000);
		 } catch (Exception ex) {
			 
		 }
	}
	
	public void testExportOnDemandJobScehduler() {
		
	}
	
//	@Test
//	@Rollback(false)
	public void testGeneralProcess() {
		
		boolean isExecuting = monitoringObserver.getStatus(1);
		System.out.println("IsExecuting .. : "+isExecuting);
		List<DdpJobRefHolder> list = ddpJobRefHolderService.findAllDdpJobRefHolders();
		System.out.println("List : "+list.size());
		final DdpRuleSchedulerJob schedulerJob = applicationContext.getBean(
				"schedulerJob", DdpRuleSchedulerJob.class);
		
		final DdpScheduler ddpScheduler = ddpSchedulerService.findDdpScheduler(37);
		
		String strBeanName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".beanName");
		String strClassName = env.getProperty("export.rule."
				+ ddpScheduler.getSchRuleCategory() + ".className");
		
		if (ddpScheduler.getSchRuleCategory() != null && strBeanName != null
				&& strClassName != null) {
			try {
				DdpSchedulerJob job = (DdpSchedulerJob) applicationContext
						.getBean(strBeanName, Class.forName(strClassName));
				//job.initiateSchedulerJob(new Object[]{ddpScheduler});
				Thread.sleep(1000000);
			//	job.runOnDemandRuleBasedConsignmentId(ddpScheduler, fromDate, toDate, consignmentIDs);
				//Thread.sleep(400000);
				//job.runOnDemandRuleBasedDocRef(ddpScheduler, fromDate, toDate, docRefs);
				//Thread.sleep(900000);
				// logger.info("DdpCreateSchedulerTask.execute(String jobName) - Loaded Scheduler Bean and Class [{}] [{}] for [{}].",strBeanName,strClassName,"SCHEDULER : "+ddpScheduler.getSchId());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else {
			

			
			
		
//			Runnable r = new Runnable() {
//				
//				@Override
//				public void run() {
//					Calendar fromDate = GregorianCalendar.getInstance();
//					fromDate.add(Calendar.DAY_OF_MONTH, -7);
//					Calendar toDate = GregorianCalendar.getInstance();
//					String jobNumber = "'B4100417G9'";
//					try
//					{
//						List<DdpExportSuccessReport> list = jdbcTemplate.query(Constant.DDP_SQL_SELECT_EXPORT_REPORTS_BASED_DATE_RANGE, new Object[] {13648,fromDate,toDate},new RowMapper<DdpExportSuccessReport>(){
//
//							@Override
//							public DdpExportSuccessReport mapRow(ResultSet rs, int rowNum)
//									throws SQLException {
//								
//								DdpExportSuccessReport exportReports = ddpExportSuccessReportService.findDdpExportSuccessReport(rs.getInt("ESR_ID"));
//								return exportReports;
//							}
//							
//						});
//						
//						System.out.println("Size of the list : "+list.size());
//						
////						List<DdpCategorizedDocs> ddpCategorizationdocsList = jdbcTemplate.query(Constant.DEF_SQL_SELECT_CAT_DOCS_EXPORT_BY_JOB_NUMBER.replaceAll("dynamiccondition", jobNumber), new Object[]{24636,fromDate,toDate}, new RowMapper<DdpCategorizedDocs>() {
////									
////							public DdpCategorizedDocs mapRow(ResultSet rs, int rowNum) throws SQLException {
////								
////								DdpCategorizedDocs ddpCategorizedDocs = ddpCategorizedDocsService.findDdpCategorizedDocs(rs.getInt("CAT_ID"));
////								
////								return ddpCategorizedDocs;
////					           }
////						});		
////						System.out.println("Size of the list : "+ddpCategorizationdocsList.size());
////					} catch (Exception ex) {
////						ex.printStackTrace();
////					}
//					
//					//schedulerJob.initiateSchedulerReport(new Object[]{ddpScheduler});
//				//	schedulerJob.runOnDemandRuleBasedJobNumber(ddpScheduler, fromDate, toDate, jobNumber);
//					
////				}
//			};
//			 ExecutorService executor = Executors.newCachedThreadPool();
//		     executor.submit(r);
//		 	try {
//		 		Thread.sleep(1000000);
//		 	} catch (InterruptedException ex) {
//		 		
//		 	}
//			System.out.println(monitoringObserver.getStatus(1)+ " : ======= Status -1 ======= : "+monitoringObserver.getJSonStringObject(1));
//			System.out.println(monitoringObserver.getStatus(3)+ " : ======= Status -2======== : "+monitoringObserver.getJSonStringObject(3));
//			for (int i = 0; i < 10; i++) {
//				try {
//					Thread.sleep(1000*2);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				System.out.println(monitoringObserver.getStatus(1)+ " : Status : "+monitoringObserver.getJSonStringObject(1));
//				System.out.println(monitoringObserver.getStatus(3)+ " : Status : "+monitoringObserver.getJSonStringObject(3));
//			}
		}
	}
	
	
	
	public DdpExportMissingDocs constructExportMissingDocs() {
		
		DdpExportMissingDocs exportDocs = new DdpExportMissingDocs();
		Calendar createdDate = GregorianCalendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		
		exportDocs.setMisRObjectId("1234567890");
		exportDocs.setMisEntryNo("1234567890");
		exportDocs.setMisMasterJob("1234567890");
		exportDocs.setMisJobNumber("1234567890");
		exportDocs.setMisConsignmentId("1234567890");
		//Invoice number for sales document reference
		exportDocs.setMisEntryType("1234567890");
		//Content size
		exportDocs.setMisCheckDigit("1234567890");
		//System.out.println("Inside the construnctMissingDocs : "+exportDocs.getMisEntryType());
		exportDocs.setMisDocType("1234567890");
		exportDocs.setMisBranch("1234567890");
		exportDocs.setMisCompany("1234567890");
		exportDocs.setMisRobjectName("1234567890");
		exportDocs.setMisAppName("Test");
		exportDocs.setMisStatus(0);
		exportDocs.setMisExpRuleId(123);
		exportDocs.setMisCreatedDate(GregorianCalendar.getInstance());
		
		createdDate.setTime(new Date());
		exportDocs.setMisDmsRCreationDate(createdDate);
		return exportDocs;
	}
}
