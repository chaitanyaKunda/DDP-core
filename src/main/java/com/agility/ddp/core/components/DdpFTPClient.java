/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPHTTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author DGuntha
 *
 */
public class DdpFTPClient implements DdpTransferClient {

	private static final Logger logger = LoggerFactory.getLogger(DdpFTPClient.class);
	
	/**
	 * Method used for checking FTP location is able to connect or not.
	 * 
	 * @param ftpLocation
	 * @param port
	 * @param userID
	 * @param password
	 * @return
	 */
	public boolean testFTPConnection(String ftpLocation,int port,String userID,String password) {
		
		logger.info("DdpFTPClient.testFTPConnection() - UserName : "+userID+" : Password : "+password+" : FTP Location : "+ftpLocation);
		FTPClient ftpClient = new FTPClient();
		boolean isLoggedIn = false;

		try {
			ftpClient.connect(ftpLocation,port);
			isLoggedIn = ftpClient.login(userID, password);
		} catch (Exception ex) {
			logger.error("DdpFTClient.testFTPConnection() - Unable to connect to FTP location without proxy ",ex);
			try {
				//ftpClient =  new FTPHTTPClient("10.201.240.26", 8080);
				ftpClient =  new FTPHTTPClient("10.201.60.76", 8080);
				ftpClient.connect(ftpLocation,port);
				ftpClient.enterLocalPassiveMode();
				isLoggedIn = ftpClient.login(userID, password);
				logger.info("List of files available : " +ftpClient.listFiles());
			} catch (Exception e) {
				logger.error("DdpFTClient.testFTPConnection() - Unable to connect to FTP location with proxy ",e);
			}
			
		}
		return isLoggedIn;
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
	public FTPClient connectFTP(String strFTPLocation,int port)
			 {

		FTPClient ftpClient = new FTPClient();

		try {
			ftpClient.connect(strFTPLocation,port);

		} catch (IOException e) {
			logger.error("DdpFTPClient.connectFTP() IOException - while connecting to FTP location...",e);
			try {
				//ftpClient =  new FTPHTTPClient("10.201.240.26", 8080);
				ftpClient =  new FTPHTTPClient("10.201.60.76", 8080);
				ftpClient.connect(strFTPLocation,port);
				ftpClient.enterLocalPassiveMode(); //to passive Mode connections
				//isLoggedIn = ftpClient.login(userID, password);
			} catch (Exception ex) {
				logger.error("DdpFTClient.testFTPConnection() - Unable to connect to FTP location with proxy ",ex);
			}
		}
		
		return ftpClient;
	}
	public FTPClient checkFTPConnection(String ftpLocation,int port,String userID,String password) {
		
		logger.info("DdpFTPClient.checkFTPConnection() - UserName : "+userID+" : Password : "+password+" : FTP Location : "+ftpLocation);
		FTPClient ftpClient = new FTPClient();

		try {
			ftpClient.connect(ftpLocation,port);
			ftpClient.login(userID, password);
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		} catch (Exception ex) {
			logger.error("DdpFTClient.checkFTPConnection() - Unable to connect to FTP location without proxy ",ex);
			try {
				// TO DO:
				//ftpClient =  new FTPHTTPClient("10.201.240.26", 8080);
				ftpClient =  new FTPHTTPClient("10.201.60.76", 8080);
				ftpClient.connect(ftpLocation,port);
				ftpClient.enterLocalPassiveMode();
				ftpClient.login(userID, password);
				ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
				logger.info("List of files available : " +ftpClient.listFiles());
			} catch (Exception e) {
				logger.error("DdpFTClient.checkFTPConnection() - Unable to connect to FTP location with proxy ",e);
			}
			
		}
		return ftpClient;
	}
	
	public FTPClient connectFTPToDropFiles(String strFTPLocation,int port,String username,String password)
	 {
		FTPClient ftpClient = null;
		try{
			ftpClient = new FTPClient();
			ftpClient.connect(strFTPLocation, port);
			ftpClient.login(username, password);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		}catch (Exception e) {
			// TODO: handle exception
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
	public boolean loginFTP(FTPClient client, String strUserId, String strPassword)
			throws Exception {
		
		boolean boolLogin = false;
		try {
			boolLogin = client.login(strUserId, strPassword);
		} catch (IOException e) {
			logger.error("DdpFTPClient.loginFTP(), IOException - while logging in FTP...",e);
			throw new Exception(
					"IOException - while logging in FTP..." + e.getMessage(), e);
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
	public boolean logoutFTP(FTPClient client)
			throws Exception {

		boolean boolLogout = false;
		
		try {
			client.logout();
			client.disconnect();
			boolLogout = true;
		} catch (IOException e) {
			logger.error("DdpFTPClient.logoutFTP(), IOException - while logging out FTP...",e);
			throw new Exception(
					"IOException - while logging out FTP..." + e.getMessage(),
					e);
		}
		
		return boolLogout;
	}
	
	/**
	 * Method to disconnect FTP
	 * 
	 * @param client
	 * 				FTPClient
	 * 
	 * @throws Exception
	 */
	public void disconnectFTP(FTPClient client)
			throws Exception {

		try {
			client.disconnect();
		} catch (IOException e) {
			logger.error("DdpFTPClient.disconnectFTP(), IOException - while disconnecting FTP...",e);
			throw new Exception(
					"IOException - while disconnecting FTP..." + e.getMessage(),
					e);
		}
	}
	
	/**
	 * Method to send file to FTP
	 * 
	 * @param client
	 * 				FTPClient
	 * @param strFileName
	 * 				String
	 * 
	 * @throws Exception
	 */
	public boolean dropFileIntoFTP(FTPClient client,String sourceLocation,String destinationLocation)
			throws Exception {
		
		boolean isFileStored = false;
		FileInputStream fileInputStream = null;
		
		try {
			if (client instanceof FTPHTTPClient) {
				logger.info("FTPClient is FTPHTTPClient");
			}
			else
			{
				logger.info("FTPClient is not FTPHTTPClient");
				client.setFileType(FTP.BINARY_FILE_TYPE, FTP.BINARY_FILE_TYPE);
				client.setFileTransferMode(FTP.BINARY_FILE_TYPE);
			}
			fileInputStream = new FileInputStream(sourceLocation);			
			isFileStored = client.storeFile(destinationLocation, fileInputStream);
		} catch (FileNotFoundException e) {
			logger.error("DdpFTPClient.dropFileIntoFTP(), FileNotFoundException - while sending file to FTP location...",e);
			throw new Exception(
					"FileNotFoundException - while sending file to FTP location..."
							+ e.getMessage(), e);
		} catch (IOException e) {
			logger.error("DdpFTPClient.dropFileIntoFTP(), FileNotFoundException - while sending file to FTP location...",e);
			throw new Exception(
					"IOException - while sending file to FTP location..."
							+ e.getMessage(), e);
		} finally {
			try {
				if (null != fileInputStream) {
					fileInputStream.close();
				}
			} catch (IOException e) {
				logger.error("DdpFTPClient.dropFileIntoFTP(), FileNotFoundException -  while closing FileINputStream...",e);
				throw new Exception(
						"IOException - while closing FileINputStream..."
								+ e.getMessage(), e);
			}
		}
		
		return isFileStored;
	}
	public boolean dropFileIntoFTPUsingInputStream(FTPClient client,String sourceLocation,String destinationLocation) throws Exception {
		
		boolean isFileStored = false;
		InputStream inputStream = null;
		File sourceFile = new File(sourceLocation);
		inputStream = new FileInputStream(sourceFile);			
		isFileStored = client.storeFile(destinationLocation, inputStream);
		return isFileStored;
	}
	public boolean dropFileIntoFTPUsingOutputStream(FTPClient client,String sourceLocation,String destinationLocation) throws Exception {
		
		boolean isFileStored = false;
		InputStream inputStream = null;
		File sourceFile = new File(sourceLocation);
		inputStream = new FileInputStream(sourceFile);
		
		 OutputStream outputStream = client.storeFileStream(destinationLocation);
         byte[] bytesIn = new byte[4096];
         int read = 0;

         while ((read = inputStream.read(bytesIn)) != -1) {
             outputStream.write(bytesIn, 0, read);
         }
         inputStream.close();
         outputStream.close();

         isFileStored = client.completePendingCommand();
		return isFileStored;
	}
	
	public static void main(String[] args) {
		DdpFTPClient client = new DdpFTPClient();
//		System.out.println("Status : "+client.testFTPConnection("scandoc.agilitylogistics.com", 21, "svc-dmsddp", "P@ssw0rd123"));
		System.out.println("Status : "+client.testFTPConnection("213.30.51.102", 21, "dms", "Agility+.16"));
		//System.out.println("Status : "+client.testFTPConnection("ftp2.integrationpoint.net", 21, "user100521abkr", "8*uRaKupat"));
		//System.out.println("Status : "+client.testFTPConnection("labftp.integrationpoint.net", 21, "user1005219", "TH@sWe7eyu"));
	}

	@Override
	public boolean testConnection(String username, String password, int port,
			String destinationLocation) {
		
		boolean isConnected = testFTPConnection(destinationLocation, port, username, password);
		return isConnected;
	}
}
