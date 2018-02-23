/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.IOException;

import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author DGuntha
 *
 */
public class DdpFTPSClient extends DdpFTPClient implements DdpTransferClient {
	
	private static final Logger logger = LoggerFactory.getLogger(DdpFTPSClient.class);
	
	/**
	 * Method used for checking FTP location is able to connect or not.
	 * 
	 * @param ftpLocation is host
	 * @param port
	 * @param userID
	 * @param password
	 * @return
	 */
	public boolean testFTPSConnection(String ftpLocation,int port,String userID,String password) {
		
		//logger.info("DdpFTPSClient.testFTPConnection() - UserName : "+userID+" : Password : "+password+" : FTPS Location : "+ftpLocation);
		
		FTPSClient ftpClient = new FTPSClient("SSL",true);
		boolean isFTPSConnected = false;
		
		try {
			ftpClient.connect(ftpLocation, port);
			int reply = ftpClient.getReplyCode();
			if (FTPReply.isPositiveCompletion(reply)) {
				// Set protection buffer size
				 ftpClient.execPBSZ(0);
				 // Set data channel protection to private
				 ftpClient.execPROT("P");
				 // Enter local passive mode
				 ftpClient.enterLocalPassiveMode();
				 //ftpClient.enterRemotePassiveMode();
				
				 // Login
				 isFTPSConnected = ftpClient.login(userID,password);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			//logger.error("DdpFTPSClient.testFTPConnection() - Unable to connect to FTPS location");
		}
		return isFTPSConnected;
	}

	
	/**
	 * Method to connect FTP
	 * 
	 * @param strFTPLocation
	 * 					String
	 * 
	 * @return FTPClient
	 * @throws Exception
	 */
	public FTPSClient connectFTPS(String strFTPLocation,int port)
			throws Exception {

		FTPSClient ftpClient = new FTPSClient("SSL",true);

		try {
			ftpClient.connect(strFTPLocation,port);
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient = null;
			}

		} catch (IOException e) {
			logger.error("DdpFTPSClient.connectFTP() IOException - while connecting to FTPS location...",e);
			throw new Exception(
					"IOException - while connecting to FTPS location..."
							+ e.getMessage(), e);
		}
		
		return ftpClient;
	}
	
	/**
	 * Method to login to FTP
	 * 
	 * @param client
	 * 				FTPClient
	 * @param strUserId
	 * 				String
	 * @param strPassword
	 * 				String
	 * 
	 * @return boolean
	 * @throws Exception
	 */
	public boolean loginFTPS(FTPSClient client, String strUserId, String strPassword)
			throws Exception {
		
		boolean boolLogin = false;
		try {
			int reply = client.getReplyCode();
			if (FTPReply.isPositiveCompletion(reply)) {
				// Set protection buffer size
				client.execPBSZ(0);
				 // Set data channel protection to private
				client.execPROT("P");
				 // Enter local passive mode
				client.enterLocalPassiveMode();
				client.enterRemotePassiveMode();
				 // Login
				boolLogin = client.login(strUserId, strPassword);
			}
			
		} catch (IOException e) {
			logger.error("DdpFTPSClient.loginFTP(), IOException - while logging in FTPS...",e);
			throw new Exception(
					"IOException - while logging in FTPS..." + e.getMessage(), e);
		}
		
		return boolLogin;
	}
	
	/**
	 * Method to logout FTP
	 * 
	 * @param client
	 * 				FTPClient
	 * 
	 * @return boolean
	 * @throws Exception
	 */
	public boolean logoutFTPS(FTPSClient client)
			throws Exception {

		boolean boolLogout = false;
		
		try {
			boolLogout = client.logout();
		} catch (IOException e) {
			logger.error("DdpFTPClient.logoutFTP(), IOException - while logging out FTP...",e);
			throw new Exception(
					"IOException - while logging out FTP..." + e.getMessage(),
					e);
		}
		
		return boolLogout;
	}
	
	public static void main (String[] args) {
		DdpFTPSClient client = new DdpFTPSClient();
		System.out.println("Connected FTPS : "+client.testFTPSConnection("10.20.1.28", 990, "DDPFTPS", "!DDPagility3"));
	}


	@Override
	public boolean testConnection(String username, String password, int port,
			String destinationLocation) {
		
		boolean isConnected = testFTPSConnection(destinationLocation, port, username, password);
		
		return isConnected;
	}
	
}
