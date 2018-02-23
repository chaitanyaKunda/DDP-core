/**
 * 
 */
package com.agility.ddp.core.components;

/**
 * @author DGuntha
 *
 */
public interface DdpTransferClient {

	public abstract boolean testConnection(String username,String password,int port,String destinationLocation);
}
