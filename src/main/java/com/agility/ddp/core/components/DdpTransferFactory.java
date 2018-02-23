/**
 * 
 */
package com.agility.ddp.core.components;

import java.io.File;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import javax.swing.Box.Filler;

import jcifs.smb.SmbFile;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.agility.ddp.core.util.CommonUtil;
import com.agility.ddp.core.util.FileUtils;
import com.agility.ddp.core.util.SchedulerJobUtil;
import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.agility.ddp.data.domain.DdpCommunicationSetup;
import com.jcraft.jsch.ChannelSftp;

/**
 * @author DGuntha
 *
 */
@Component
public class DdpTransferFactory {
	
	@Autowired
	private CommonUtil commonUtil;

	private static final Logger logger = LoggerFactory.getLogger(DdpTransferFactory.class);
	
	/**
	 * Method used for constructing the transferObject.
	 * 
	 * @param ddpCommunicationSetup
	 * @return
	 */
	public List<DdpTransferObject> constructTransferObject(DdpCommunicationSetup ddpCommunicationSetup) {
		
		List<DdpTransferObject> listOfConnections = new LinkedList<DdpTransferObject>();
		
		boolean isFTPConnectionType = ddpCommunicationSetup.getCmsCommunicationProtocol().equalsIgnoreCase("FTP")? true : false;
		String settingID = ddpCommunicationSetup.getCmsProtocolSettingsId();
		listOfConnections.add(constructDdpTransferObject(isFTPConnectionType, settingID,ddpCommunicationSetup.getCmsCommunicationProtocol()));
		
		if (ddpCommunicationSetup.getCmsCommunicationProtocol2() != null && !ddpCommunicationSetup.getCmsCommunicationProtocol2().isEmpty()) {
			
			isFTPConnectionType = ddpCommunicationSetup.getCmsCommunicationProtocol2().equalsIgnoreCase("FTP")? true : false;
			 settingID = ddpCommunicationSetup.getCmsProtocolSettingsId2();
			listOfConnections.add(constructDdpTransferObject(isFTPConnectionType, settingID,ddpCommunicationSetup.getCmsCommunicationProtocol2()));
		}
		
		if (ddpCommunicationSetup.getCmsCommunicationProtocol3() != null && !ddpCommunicationSetup.getCmsCommunicationProtocol3().isEmpty()) {
			
			isFTPConnectionType = ddpCommunicationSetup.getCmsCommunicationProtocol3().equalsIgnoreCase("FTP")? true : false;
			 settingID = ddpCommunicationSetup.getCmsProtocolSettingsId3();
			listOfConnections.add(constructDdpTransferObject(isFTPConnectionType, settingID,ddpCommunicationSetup.getCmsCommunicationProtocol3()));
		}
		
		return listOfConnections;
	}
	
	/**
	 * Method used for construct DdpTransferObject.
	 * 
	 * @param isFTPConnectionType
	 * @param protocolSettingID
	 * @return
	 */
	private DdpTransferObject constructDdpTransferObject(boolean isFTPConnectionType,String protocolSettingID,String  commProtocol) {

		DdpTransferObject ddpTransferObject = new DdpTransferObject();
						
			if (isFTPConnectionType) {
				
				DdpCommFtp ftpDetails = commonUtil.getFTPDetailsBasedOnProtocolID(protocolSettingID); 
				
				if ((ftpDetails.getCftFtpSecure() == null || ftpDetails.getCftFtpSecure().isEmpty()) || ftpDetails.getCftFtpSecure().equalsIgnoreCase("ftp")) {
					
					DdpFTPClient ddpFTPClient = new DdpFTPClient();
					boolean isLogged = false;
					FTPClient ftpClient = null;
					try {
						ftpClient = commonUtil.connectFTP(ddpFTPClient,ftpDetails);
						isLogged =	commonUtil.LoggedIntoFTP(ddpFTPClient, ftpDetails,ftpClient);
					} catch (Exception ex) {
						
					}
					if (!isLogged) {
						ddpTransferObject.setConnected(false);
					}
					ddpTransferObject.setFTPType(isFTPConnectionType);
					ddpTransferObject.setTypeOfConnection("ftp");
					ddpTransferObject.setDestLocation(commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation()));
					ddpTransferObject.setHostName(commonUtil.getHostName(ftpDetails.getCftFtpLocation()));
					ddpTransferObject.setUserName(ftpDetails.getCftFtpUserName());
					ddpTransferObject.setPassword(ftpDetails.getCftFtpPassword());
					ddpTransferObject.setPort(ftpDetails.getCftFtpPort());
					ddpTransferObject.setDdpTransferClient(ddpFTPClient);
					ddpTransferObject.setFtpDetails(ftpDetails);
					ddpTransferObject.setFtpClient(ftpClient);
					
					
				} else if (ftpDetails.getCftFtpSecure().equalsIgnoreCase("sftp")) {
					
					DdpSFTPClient sftpClient = new DdpSFTPClient();
					ChannelSftp sftpChannel = commonUtil.connectChannelSftp(sftpClient, ftpDetails);
					if (sftpChannel == null) {
						ddpTransferObject.setConnected(false);
					}
					ddpTransferObject.setFTPType(isFTPConnectionType);
					ddpTransferObject.setTypeOfConnection("sftp");
					ddpTransferObject.setDestLocation(commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation()));
					ddpTransferObject.setHostName(commonUtil.getHostName(ftpDetails.getCftFtpLocation()));
					ddpTransferObject.setUserName(ftpDetails.getCftFtpUserName());
					ddpTransferObject.setPassword(ftpDetails.getCftFtpPassword());
					ddpTransferObject.setPort(ftpDetails.getCftFtpPort());
					ddpTransferObject.setDdpTransferClient(sftpClient);
					ddpTransferObject.setFtpDetails(ftpDetails);
					ddpTransferObject.setChannelSftp(sftpChannel);
					
				} else if (ftpDetails.getCftFtpSecure().equalsIgnoreCase("ftps")) {
					
					DdpFTPSClient ddpFTPSClient = new DdpFTPSClient();
					FTPSClient ftpsClient = commonUtil.connectFTPS(ddpFTPSClient,ftpDetails);
					boolean isLogged =	commonUtil.LoggedIntoFTPS(ddpFTPSClient, ftpDetails,ftpsClient);
					if (!isLogged) {
						ddpTransferObject.setConnected(false);
					}
					//Type of protocol is is FTP then size is 6 because it should split as ftp:// so point of split is 6
					ddpTransferObject.setFTPType(isFTPConnectionType);
					ddpTransferObject.setTypeOfConnection("ftps");
					ddpTransferObject.setDestLocation(commonUtil.getDestinationLocation(ftpDetails.getCftFtpLocation()));
					ddpTransferObject.setHostName(commonUtil.getHostName(ftpDetails.getCftFtpLocation()));
					ddpTransferObject.setUserName(ftpDetails.getCftFtpUserName());
					ddpTransferObject.setPassword(ftpDetails.getCftFtpPassword());
					ddpTransferObject.setPort(ftpDetails.getCftFtpPort());
					ddpTransferObject.setDdpTransferClient(ddpFTPSClient);
					ddpTransferObject.setFtpDetails(ftpDetails);
					ddpTransferObject.setFtpClient(ftpsClient);
					
				}
				
			} else {
				
				if (commProtocol != null && commProtocol.equalsIgnoreCase("smtp")) {
					
					DdpCommEmail ddpCommEmail = commonUtil.getEmailDetailsBasedOnProtocolID(protocolSettingID);
					ddpTransferObject.setFTPType(isFTPConnectionType);
					ddpTransferObject.setTypeOfConnection("smtp");
					ddpTransferObject.setConnected(true);
					ddpTransferObject.setHostName(ddpCommEmail.getCemEmailTo());
					ddpTransferObject.setUserName(ddpCommEmail.getCemEmailTo());
					ddpTransferObject.setDestLocation("Files send via Email.");
					ddpTransferObject.setEmailDetails(ddpCommEmail);
					
				} else {
					DdpCommUnc uncDetails = commonUtil.getUNCDetailsBasedOnProtocolID(protocolSettingID);
					
					SmbFile	smbFile = commonUtil.connectUNCPath(uncDetails);
					if (smbFile == null) {
						ddpTransferObject.setConnected(false);
					}
					ddpTransferObject.setFTPType(isFTPConnectionType);
					ddpTransferObject.setTypeOfConnection("ftps");
					ddpTransferObject.setDestLocation(uncDetails.getCunUncPath());
					ddpTransferObject.setHostName(commonUtil.getHostName(uncDetails.getCunUncPath()));
					ddpTransferObject.setUserName(uncDetails.getCunUncUserName());
					ddpTransferObject.setPassword(uncDetails.getCunUncPassword());
					//ddpTransferObject.setPort(uncDetails.get());
					ddpTransferObject.setDdpTransferClient(new DdpUNCClient());
					ddpTransferObject.setUncDetails(uncDetails);
					ddpTransferObject.setSmbFile(smbFile);
				}
			}
			
			return ddpTransferObject;
		}
	
		/**
		 * Method used for transfer files using the configured protocol.
		 * 
		 * @param ddpTransferObject
		 * @param endSourceFolderFile
		 * @return
		 */
		public boolean transferFilesUsingProtocol(DdpTransferObject ddpTransferObject,File endSourceFolderFile,String company,String clientID,Calendar fromDate,Calendar toDate,String typeOfService) {
			
			boolean isProcessed = false;
			
			if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("smtp")) {
				
				File zipFile = FileUtils.copyFilesAndZip(endSourceFolderFile, clientID);
				try {
					if (zipFile == null)					
						isProcessed = commonUtil.sendEmailForExportModule(endSourceFolderFile, ddpTransferObject.getEmailDetails(), company, clientID, fromDate, toDate, typeOfService);
					else 
						isProcessed = commonUtil.sendEmailForExportModule(zipFile, ddpTransferObject.getEmailDetails(), company, clientID, fromDate, toDate, typeOfService);
				} catch (Exception ex) {
					logger.error("DdpTransferFactory.transferFileUsingProtocol() - Unable to send the mail due to ",ex);
				} finally {
					if (zipFile != null)
						SchedulerJobUtil.deleteFolder(zipFile);
				}
				
			} else {
				if (ddpTransferObject.isFTPType()) {
					if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("ftp")) 
						isProcessed = SchedulerJobUtil.performFileTransferUsingFTP(endSourceFolderFile, ddpTransferObject.getFtpDetails());
					else if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("sftp")) {
						//TODO: need to implement this code.
						DdpSFTPClient sftpClient = (DdpSFTPClient)ddpTransferObject.getDdpTransferClient(); 
						
						if (sftpClient != null && ddpTransferObject.getChannelSftp() != null)
							sftpClient.logoutOfSFTP(ddpTransferObject.getChannelSftp());
						isProcessed = SchedulerJobUtil.performTransferUsingSFTP(endSourceFolderFile, ddpTransferObject.getChannelSftp(), ddpTransferObject.getDestLocation(),sftpClient,ddpTransferObject.getFtpDetails(),commonUtil);
					} else if (ddpTransferObject.getTypeOfConnection().equalsIgnoreCase("ftps")) {
						isProcessed = SchedulerJobUtil.performFileTransferUsingFTPS(endSourceFolderFile, ddpTransferObject.getFtpDetails());
					}
					
				} else {
					isProcessed = SchedulerJobUtil.peformFileTransferUsingUNC(endSourceFolderFile, ddpTransferObject.getUncDetails());
				}
			}
			return isProcessed;
		}
		
		
		
}
