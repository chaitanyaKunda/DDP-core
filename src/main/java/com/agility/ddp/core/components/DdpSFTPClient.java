/**
 * 
 */
package com.agility.ddp.core.components;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

/**
 * @author DGuntha
 *
 */
public class DdpSFTPClient implements DdpTransferClient {

	private static final Logger logger = LoggerFactory.getLogger(DdpSFTPClient.class);
	
	/**
	 * Method used for getting the session object of JSch for SFTP.
	 *  
	 * @param hostname
	 * @param username
	 * @param password
	 * @param port
	 * @return
	 */
	public Session getJSchSession(String hostname,String username,String password,int port) {
		
		JSch jsch = new JSch();
		Session session = null;
		try {
			
			session = jsch.getSession(username, hostname, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(password);
			session.setServerAliveInterval(100000000);
			session.connect();
			
		} catch (Exception ex) {
			logger.error("DdpSFTPClient.getJSchSession() - Unable create the Session for SFTP connection. ", ex);
			session = null;
		}
		return session;
	}
	
	/**
	 * Method used for connecting the SFTP connection.
	 * 
	 * @param session
	 * @return
	 */
	public ChannelSftp connectToSFTP(Session session) {
		
		ChannelSftp sftpChannel = null;
		try {
			if (session != null) {
				sftpChannel	 = (ChannelSftp)session.openChannel("sftp");
				sftpChannel.connect();
			}
		} catch (Exception ex) {
			sftpChannel = null;
			logger.error("DdpSFTPClient.getJSchSession() - Unable to open a connection for SFTP. ", ex);
		}
		return sftpChannel;
	}
	
	/**
	 * Method used for logout the SFTP location.
	 * 
	 * @param channel
	 */
	public void logoutOfSFTP(ChannelSftp channel) {
		
		try {
			if (channel != null) {
				Session session = channel.getSession();
				channel.exit();
		        session.disconnect();
			}
		} catch(Exception ex) {
			logger.error("DdpSFTPClient.getJSchSession() - Unable logout connection for SFTP. ", ex);
		}
	}
	
	/**
	 * Method used for testing the connection.
	 * 
	 * @param hostname
	 * @param username
	 * @param password
	 * @param port
	 * @return
	 */
	public boolean testSFTPConnection(String hostname,String username,String password,int port) {
		
		boolean isConnected = false;
		
		Session session = getJSchSession(hostname, username, password, port);
		if (session != null) {
			ChannelSftp sftp = connectToSFTP(session);
			if (sftp != null) {
				isConnected = true;
				logoutOfSFTP(sftp);
			}
		}
		
		return isConnected;
	}
	
	public static void main(String[] args) throws Exception {
		DdpSFTPClient sftpClient = new DdpSFTPClient();
		System.out.println("SFTP Client connection : "+sftpClient.testSFTPConnection("staging1.b2bi.agility.com", "AUTO", "@ut0t3$t", 22));
	}

	@Override
	public boolean testConnection(String username, String password, int port,
			String destinationLocation) {
		
		boolean isConnected = testSFTPConnection(destinationLocation, username, password, port);
		
		return isConnected;
	}
	
}
