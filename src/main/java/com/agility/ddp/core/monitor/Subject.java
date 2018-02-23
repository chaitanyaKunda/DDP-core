/**
 * 
 */
package com.agility.ddp.core.monitor;

import java.util.Date;

/**
 * @author DGuntha
 *
 */
public interface Subject {

	public void registerObserver(Observer observer);
	
	public void notifyObserver();
	
	public void unRegisterObserver(Observer observer);
	
	public boolean isExecuting();
	
	public void executeThread();
	
	public String getJSonSubject();
	
	public void doRefresh();
	
	public Date getExecutedDate();
	
	
}
