/**
 * 
 */
package com.agility.ddp.core.monitor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.agility.ddp.core.util.CommonUtil;

/**
 * @author DGuntha
 *
 */
@Component
public class MonitoringObserver implements Observer {

	private Map<Integer, Subject> applicationMap = null;
	
	@Autowired
	private CommonUtil commonUtil;
	
	@PostConstruct
	public void init() {
		
		applicationMap = new HashMap<Integer, Subject>();
		
		AutoEmailingDocsSubject aedSubject = new AutoEmailingDocsSubject();
		aedSubject.registerObserver(this);
		aedSubject.executeThread();
		applicationMap.put(aedSubject.getTypeOfService(), aedSubject);
		
		ConsolidatedAEDSubject conSubject = new ConsolidatedAEDSubject();
		conSubject.registerObserver(this);
		conSubject.executeThread();
		applicationMap.put(conSubject.getTypeOfService(), conSubject);
		
		ExportDocsSetupSubject expSubject = new ExportDocsSetupSubject();
		expSubject.registerObserver(this);
		expSubject.executeThread();
		applicationMap.put(expSubject.getTypeOfService(), expSubject);
		
	}

	@Override
	public void setSubject(Integer typeOfService, Subject subject) {
		applicationMap.put(typeOfService, subject);
	}
	
	/**
	 * Method used for getting the status.
	 * 
	 * @param typeOfServer
	 * @return
	 */
	public boolean getStatus(Integer typeOfService) {
		
		boolean isExecuting = false;
		
		for(Integer type : applicationMap.keySet()) {
			
			if (type.intValue() == typeOfService.intValue()) {
				
				Subject subject = applicationMap.get(type);
				isExecuting = subject.isExecuting();
				break;
				
			}
		}
		
		return isExecuting;
	}
	
	/**
	 * Method used for getting the over all status of threads.
	 * 
	 * @return
	 */
	public boolean getOverAllStatus() {
		
		boolean isExecuting = false;
		
		for(Integer type : applicationMap.keySet()) {
			
			Subject subject = applicationMap.get(type);
			
			if (subject.isExecuting()) {
				isExecuting = subject.isExecuting();
				break;
				
			}
		}
		
		return isExecuting;
	}
	
	
	/**
	 * Method used for getting the Json String Object.
	 * 
	 * @param typeOfService
	 * @return
	 */
	public String getJSonStringObject(Integer typeOfService) {
		
		String jsonString = null;
		
		for(Integer type : applicationMap.keySet()) {
			
			if (type.intValue() == typeOfService.intValue()) {
				
				Subject subject = applicationMap.get(type);
				jsonString = subject.getJSonSubject();
				break;
				
			}
		}
		return jsonString;
	}

	@Override
	public CommonUtil getCommonUtil() {
		return commonUtil;
	}
	
	/**
	 * Method used for do Refreshing based the serivce.
	 * 
	 * @param typeOfService
	 * @return
	 */
	public void doRefreshBasedService(Integer typeOfService) {
		
		
		for(Integer type : applicationMap.keySet()) {
			
			if (type.intValue() == typeOfService.intValue()) {
				
				Subject subject = applicationMap.get(type);
				subject.doRefresh();;
				
			}
		}
	}
	
	/**
	 * Method used for doing global refresh.
	 * 
	 * @param typeOfService
	 * @return
	 */
	public void doGlobalReferesh() {
		
		for(Integer type : applicationMap.keySet()) {
			
			Subject subject = applicationMap.get(type);
			subject.doRefresh();
		}
	}
	
	/**
	 * Method used for getting the executed date by service.
	 * 
	 * @param typeOfService
	 * @return
	 */
	public Date getExecutedDateByService(Integer typeOfService) {
		
		Date executedDate = new Date();

		for(Integer type : applicationMap.keySet()) {
			
			if (type.intValue() == typeOfService.intValue()) {
				
				Subject subject = applicationMap.get(type);
				executedDate = subject.getExecutedDate();
				break;
				
			}
		}
		
		return executedDate;
	}
	
}
