/**
 * 
 */
package com.agility.ddp.core.components;

import jcifs.smb.SmbFile;

import org.apache.commons.net.ftp.FTPClient;

import com.agility.ddp.data.domain.DdpCommEmail;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.jcraft.jsch.ChannelSftp;

/**
 * @author DGuntha
 *
 */
public class DdpTransferObject {

	private boolean isFTPType;
	private String typeOfConnection;
	private boolean isConnected = true;
	private String destLocation;
	private String hostName;
	private String userName;
	private String password;
	private int port;
	private DdpTransferClient ddpTransferClient;
	private DdpCommFtp ftpDetails;
	private DdpCommUnc uncDetails;
	private FTPClient ftpClient;
	private ChannelSftp channelSftp;
	private SmbFile	smbFile;
	private String protocolType;
	private DdpCommEmail emailDetails;
	
	/**
	 * Default constructor.
	 */
	public DdpTransferObject() {		
	}
	
	/**
	 * Constructor with parameters.
	 * 
	 * @param isFTPType
	 * @param typeOfConnection
	 * @param isConnected
	 * @param destLocation
	 * @param userName
	 * @param password
	 * @param port
	 */
	public DdpTransferObject(boolean isFTPType, String typeOfConnection,
			boolean isConnected, String destLocation,String hostName, String userName,
			String password, int port,DdpTransferClient ddpTransferClient) {
		
		this.isFTPType = isFTPType;
		this.typeOfConnection = typeOfConnection;
		this.isConnected = isConnected;
		this.destLocation = destLocation;
		this.hostName = hostName;
		this.userName = userName;
		this.password = password;
		this.port = port;
		this.ddpTransferClient = ddpTransferClient;
	}

	/**
	 * @return the isFTPType
	 */
	public boolean isFTPType() {
		return isFTPType;
	}

	/**
	 * @param isFTPType the isFTPType to set
	 */
	public void setFTPType(boolean isFTPType) {
		this.isFTPType = isFTPType;
	}

	/**
	 * @return the typeOfConnection
	 */
	public String getTypeOfConnection() {
		return typeOfConnection;
	}

	/**
	 * @param typeOfConnection the typeOfConnection to set
	 */
	public void setTypeOfConnection(String typeOfConnection) {
		this.typeOfConnection = typeOfConnection;
	}

	/**
	 * @return the isConnected
	 */
	public boolean isConnected() {
		return isConnected;
	}

	/**
	 * @param isConnected the isConnected to set
	 */
	public void setConnected(boolean isConnected) {
		this.isConnected = isConnected;
	}

	/**
	 * @return the destLocation
	 */
	public String getDestLocation() {
		return destLocation;
	}

	/**
	 * @param destLocation the destLocation to set
	 */
	public void setDestLocation(String destLocation) {
		this.destLocation = destLocation;
	}

	
	/**
	 * @return the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @param hostName the hostName to set
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	/**
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the ddpTransferClient
	 */
	public DdpTransferClient getDdpTransferClient() {
		return ddpTransferClient;
	}

	/**
	 * @param ddpTransferClient the ddpTransferClient to set
	 */
	public void setDdpTransferClient(DdpTransferClient ddpTransferClient) {
		this.ddpTransferClient = ddpTransferClient;
	}

	/**
	 * @return the ftpDetails
	 */
	public DdpCommFtp getFtpDetails() {
		return ftpDetails;
	}

	/**
	 * @param ftpDetails the ftpDetails to set
	 */
	public void setFtpDetails(DdpCommFtp ftpDetails) {
		this.ftpDetails = ftpDetails;
	}

	/**
	 * @return the uncDetails
	 */
	public DdpCommUnc getUncDetails() {
		return uncDetails;
	}

	/**
	 * @param uncDetails the uncDetails to set
	 */
	public void setUncDetails(DdpCommUnc uncDetails) {
		this.uncDetails = uncDetails;
	}

	/**
	 * @return the ftpClient
	 */
	public FTPClient getFtpClient() {
		return ftpClient;
	}

	/**
	 * @param ftpClient the ftpClient to set
	 */
	public void setFtpClient(FTPClient ftpClient) {
		this.ftpClient = ftpClient;
	}

	/**
	 * @return the channelSftp
	 */
	public ChannelSftp getChannelSftp() {
		return channelSftp;
	}

	/**
	 * @param channelSftp the channelSftp to set
	 */
	public void setChannelSftp(ChannelSftp channelSftp) {
		this.channelSftp = channelSftp;
	}

	/**
	 * @return the smbFile
	 */
	public SmbFile getSmbFile() {
		return smbFile;
	}

	/**
	 * @param smbFile the smbFile to set
	 */
	public void setSmbFile(SmbFile smbFile) {
		this.smbFile = smbFile;
	}

	/**
	 * @return the protocolType
	 */
	public String getProtocolType() {
		return protocolType;
	} 

	/**
	 * @param protocolType the protocolType to set
	 */
	public void setProtocolType(String protocolType) {
		this.protocolType = protocolType;
	}

	/**
	 * @return the emailDetails
	 */
	public DdpCommEmail getEmailDetails() {
		return emailDetails;
	}

	/**
	 * @param emailDetails the emailDetails to set
	 */
	public void setEmailDetails(DdpCommEmail emailDetails) {
		this.emailDetails = emailDetails;
	}

	
	
}
