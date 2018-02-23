/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.File;
import java.io.FileInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

/**
 * @author DGuntha
 *
 */
public class DdpUNCClient implements DdpTransferClient {

	private static final Logger logger = LoggerFactory.getLogger(DdpUNCClient.class);
	
	/**
	 * Method used for getting the NtlmPassword Authentication for share folder.
	 * 
	 * @param domain
	 * @param username
	 * @param password
	 * @return
	 */
	public NtlmPasswordAuthentication uncAutentication(String domain,String username,String password) {
		
		NtlmPasswordAuthentication authentication = new NtlmPasswordAuthentication(domain,username,password);
		
		return authentication;
	}
	
	/**
	 * Method used for copying the file using UNC.
	 * 
	 * @param sourcePath
	 * @param destinationPath
	 * @param auth
	 * @return
	 * @throws Exception
	 */
	public boolean copyFileUsingJcifs( String sourcePath,
			    String destinationPath,NtlmPasswordAuthentication auth) throws Exception {

		 boolean isFileCopied = false;
		 SmbFileOutputStream smbFileOutputStream = null;
		 FileInputStream fileInputStream = null;
		 
		   try {
			   SmbFile sFile = new SmbFile(destinationPath, auth);
			   
			   smbFileOutputStream  = new SmbFileOutputStream(sFile);
			   fileInputStream =  new FileInputStream(new File(sourcePath));
	
				  final byte[] buf = new byte[16 * 1024 * 1024];
				  int len;
				  while ((len = fileInputStream.read(buf)) > 0) {
				   smbFileOutputStream.write(buf, 0, len);
				  }
				  isFileCopied = true;
		   } catch (Exception ex) {
			   logger.error("DdpUNCClient.copyFileUsingJcifs() Exception - while copy the file using UNC...",ex);
				throw new Exception(
						"Exception - while copy the file using UNC..."
								+ ex.getMessage(), ex);
		   } finally {
			   if (fileInputStream != null)
				   fileInputStream.close();
			   if (smbFileOutputStream != null)
				   smbFileOutputStream.close();
		   }
		   
		   return isFileCopied;
	 }
	
	/**
	 * Method used for checking the connection is exists or not.
	 * 
	 * @param userName
	 * @param password
	 * @param uncPath
	 * @return
	 */
	public boolean testUNCConnection(String userName, String password,String uncPath) {
		
		logger.info("DdpUNCClient.testUNCConnection() - UserName : "+userName+" : Password : "+password+" : UNCPath : "+uncPath);
	   //"svc-dmsddp" username;
		 //"P@ssw0rd123";
		boolean isAccessable = false;
		 String domain = "";
		 String destPath = "smb:"+uncPath+ "/"; //scandoc.agilitylogistics.com/svc-dmsddp/DDPDATA-UAT/testing";
		 SmbFile sFile = null;
		 try {
	  	    // DdpUNCClient ddpUNCClient = new DdpUNCClient();
	  	     NtlmPasswordAuthentication auth = uncAutentication(domain, userName, password);
	  	      sFile = new SmbFile(destPath, auth);
	  	    isAccessable= sFile.canRead();
	  	    sFile.listFiles();
	  	    isAccessable = sFile.canWrite();
	  	    //isAccessable = sFile.isFile(); 
	  	   // isAccessable = true;
		 } catch(Exception ex) {
			 logger.error("CommonUtils - connectUNCPath() - Unable to connect to UNC path. ", ex);	
			 isAccessable = false;
		 }
		 return isAccessable;
	}

	@Override
	public boolean testConnection(String username, String password, int port,
			String destinationLocation) {
		
		boolean isConnected = testUNCConnection(username, password, destinationLocation);
		
		return isConnected;
	}
}
