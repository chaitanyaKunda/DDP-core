/**
 * 
 */
package com.agility.ddp.core.monitor;

import com.agility.ddp.core.util.CommonUtil;

/**
 * @author DGuntha
 *
 */
public interface Observer {

	public void setSubject(Integer typeOfService,Subject subject);
	
	public CommonUtil getCommonUtil();
}
