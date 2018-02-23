/**
 * 
 */
package com.agility.ddp.core.monitor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.agility.ddp.data.domain.DdpJobRefHolder;
import com.google.gson.Gson;


/**
 * @author DGuntha
 *
 */

public class AutoEmailingDocsSubject implements Subject {

	private int typeOfService = 1;
	private boolean isExecuting = false;
	private List<Observer> observers = new ArrayList<Observer>();
	private String jSonSubject;
	private Date executedDate = new Date();
		
	@Override
	public void registerObserver(Observer observer) {
		observers.add(observer);
	}
	
	/**
	 * @return the typeOfService
	 */
	public int getTypeOfService() {
		return typeOfService;
	}
	
	/**
	 * @param typeOfService the typeOfService to set
	 */
	public void setTypeOfService(int typeOfService) {
		this.typeOfService = typeOfService;
	}
	
	/**
	 * @return the isExecuting
	 */
	public boolean isExecuting() {
		return isExecuting;
	}
	
	/**
	 * @param isExecuting the isExecuting to set
	 */
	public void setExecuting(boolean isExecuting) {
		this.isExecuting = isExecuting;
	}
	
	
	
	/**
	 * @return the jSonSubject
	 */
	public String getJSonSubject() {
		return jSonSubject;
	}

	/**
	 * @param jSonSubject the jSonSubject to set
	 */
	public void setjSonSubject(String jSonSubject) {
		this.jSonSubject = jSonSubject;
	}

	@Override
	public void notifyObserver() {
			
		for (Observer observer : observers) {
			observer.setSubject(typeOfService, this);
		}
		
	}
	
	@Override
	public void unRegisterObserver(Observer observer) {
		observers.remove(observer);
	}
	
	/**
	 * Method used for fetching results.
	 */
	private void fetchResults() {
		
		if (observers != null && observers.size() > 0) {
			
			List<DdpJobRefHolder> jobRefs  = observers.get(0).getCommonUtil().fetchJobRefHolder();
			boolean isStatus = false;
			executedDate = new Date();
			
			if (jobRefs != null) {
				for (DdpJobRefHolder ddpJobRefHolder : jobRefs) {
					if (ddpJobRefHolder.getId().getJrfStatus() != null && ddpJobRefHolder.getId().getJrfStatus().intValue() == 0) { 
						isStatus = true;
						break;
					}
				}
				Gson gson = new Gson();
				jSonSubject = gson.toJson(jobRefs);
				isExecuting = isStatus;
			} else {
				jSonSubject = "";
				isExecuting = false;
			}
			notifyObserver();
		}
	}

	@Override
	public void executeThread() {
		
		Runnable runnable = new Runnable() {
			
			@Override
			public void run() {
				fetchResults();
				
			}
		};
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
		executorService.scheduleAtFixedRate(runnable, 0, 60, TimeUnit.SECONDS);
	}

	@Override
	public void doRefresh() {
		fetchResults();
	}

	public Date getExecutedDate() {
		return executedDate;
	}

	
}
