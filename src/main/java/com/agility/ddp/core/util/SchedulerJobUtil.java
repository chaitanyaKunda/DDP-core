/**
 * 
 */
package com.agility.ddp.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import jcifs.smb.NtlmPasswordAuthentication;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agility.ddp.core.components.DdpFTPClient;
import com.agility.ddp.core.components.DdpFTPSClient;
import com.agility.ddp.core.components.DdpSFTPClient;
import com.agility.ddp.core.components.DdpUNCClient;
import com.agility.ddp.data.domain.DdpCommFtp;
import com.agility.ddp.data.domain.DdpCommUnc;
import com.jcraft.jsch.ChannelSftp;

/**
 * @author DGuntha
 *
 */
public class SchedulerJobUtil {

	 private static final Logger logger = LoggerFactory.getLogger(SchedulerJobUtil.class);
	 
	/**
	 * Method used for checking the type of execution.
	 * 
	 * @param strCronExpression
	 * @return
	 */
	public static String getSchFreq(String strCronExpression)
    {		
		//TODO Need to implement the yearly also.
		
		String freq = "";
		String[] cronEx = strCronExpression.split(" ");
		boolean isExecuted = false;
		//checking
		if(! (cronEx[3].equals("*") || cronEx[3].equals("?")))
		{
			freq ="monthly";
			isExecuted = true;
		}
		if(!isExecuted && !(cronEx[5].equals("*") || cronEx[5].equals("?")))
		{
			freq = "weekly";
			isExecuted = true;
		}
		
		if(!isExecuted && !(cronEx[2].equals("*") || cronEx[2].equals("?")))
		{
			freq ="daily";
			if (cronEx[2].contains("/"))
				freq = "hourly";
			
			isExecuted = true;
		} 
		if(!isExecuted && !cronEx[1].equals("*"))
		{
			freq+="minutes";
			isExecuted = true;
		}
		
		return freq;
    }
	
	/**
	 * Method used for returning the current start date.
	 * 
	 * @param strFeq
	 * @param currTime
	 * @return
	 */
	public static Calendar getQueryStartDate(String strFeq, Calendar startDate, String cronExpression)
    {
		Calendar currTime = startDate;
		//currTime.add(Calendar.MONTH, -1);

			//if frequency is hourly, return start date and time as 1 hour before exactly from now
		if (strFeq.equals("hourly")) {
			currTime.add(Calendar.HOUR, -1);
		} else {
			String[] cron = cronExpression.split(" ");
			try {
				if (!cron[2].contains("/"))
					currTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(cron[2]));
				if (!cron[1].contains("/"))
					currTime.set(Calendar.MINUTE,Integer.parseInt(cron[1]));
			} catch(Exception ex) {
				ex.printStackTrace();
			}
		}
		
		//if frequency is daily, return start date and time as previous date (consider 24 hours exactly from now)  
		if (strFeq.equals("daily")) {
			currTime.add(Calendar.DAY_OF_YEAR, -1);
			
		}
		
		//if frequency is weekly, return start date as one week previous date (exclude today) and time should be same as 'ruleStartDate' time for this date
		if (strFeq.equals("weekly")) {
			currTime.add(Calendar.WEEK_OF_MONTH, -1);
		}

		//if frequency is monthly, return start date as one month previous date (exclude today) and time should be same as 'ruleStartDate' time for this date
		if (strFeq.equals("monthly")) {
			currTime.add(Calendar.MONTH, -1);
		}
		
		return currTime;
		
    }
	
	/**
	 * Method used for returning the current start date.
	 * 
	 * @param strFeq
	 * @param currTime
	 * @return
	 */
	public static Calendar getQueryStartDate(String strFeq, Calendar startDate)
    {
		Calendar currTime = startDate;
		//currTime.add(Calendar.MONTH, -1);

			//if frequency is hourly, return start date and time as 1 hour before exactly from now
		if (strFeq.equals("hourly")) {
			currTime.add(Calendar.HOUR, -1);
		} 
		
		//if frequency is daily, return start date and time as previous date (consider 24 hours exactly from now)  
		if (strFeq.equals("daily")) {
			currTime.add(Calendar.DAY_OF_YEAR, -1);
			
		}
		
		//if frequency is weekly, return start date as one week previous date (exclude today) and time should be same as 'ruleStartDate' time for this date
		if (strFeq.equals("weekly")) {
			currTime.add(Calendar.WEEK_OF_MONTH, -1);
		}

		//if frequency is monthly, return start date as one month previous date (exclude today) and time should be same as 'ruleStartDate' time for this date
		if (strFeq.equals("monthly")) {
			currTime.add(Calendar.MONTH, -1);
		}
		
		return currTime;
		
    }
	
	
	/**
	 * Method used for returning the end date.
	 * 
	 * @param strFeq
	 * @param ruleEndDate
	 * @return
	 */
	public static Calendar getQueryEndDate(String strFeq, Calendar EndDate, String cronExpression)
    {
		Calendar ruleEndDate = EndDate;
		//ruleEndDate.add(Calendar.MONTH, -1);

		if (!strFeq.equals("hourly")) {
			String[] cron = cronExpression.split(" ");
			try {
				if (!cron[2].contains("/"))
					ruleEndDate.set(Calendar.HOUR_OF_DAY, Integer.parseInt(cron[2]));
				if (!cron[1].contains("/"))
					ruleEndDate.set(Calendar.MINUTE,Integer.parseInt(cron[1]));
				} catch(Exception ex) {
					ex.printStackTrace();
				}
			
		}
	/*	if (strFeq.equals("monthly")) {
			ruleEndDate.add(Calendar.MONTH, -1);
		}*/
				
		return ruleEndDate;
    }
	
	/**
	 * Method used for transferring the files using ftp.
	 * 
	 * @param tempFolder
	 * @param ftpDetails
	 */
	public static boolean performFileTransferUsingFTP(File tempFolder,DdpCommFtp ftpDetails) {
		
		logger.debug("DdpRuleSchedulerJob.performFileTransferUsingFTP() method invoked.");
		boolean isExpection = true;
		String ftpPath = ftpDetails.getCftFtpLocation();
		String fileDestPath = "";
		String strFTPURL = "";
		FTPClient ftpClient = null;
		DdpFTPClient ddpFTPClient = null;
		
		if (ftpPath.contains("/")) {
			
			ftpPath = ftpPath.substring(6, ftpPath.length());
			 String[] strArray = ftpPath.split("/");
			 strFTPURL = ftpPath.substring(0, strArray[0].length());
			 //Avoiding "/" for ftp destination path
			 fileDestPath = ftpPath.substring(strFTPURL.length()+1);
		}
		try {
    		ddpFTPClient = new DdpFTPClient();
    		ftpClient = ddpFTPClient.checkFTPConnection(strFTPURL, ftpDetails.getCftFtpPort().intValue(),ftpDetails.getCftFtpUserName(),ftpDetails.getCftFtpPassword());
    		if (ftpClient != null) {
//    			boolean isLogged = ddpFTPClient.loginFTP(ftpClient, ftpDetails.getCftFtpUserName(), ftpDetails.getCftFtpPassword());
//    			if (isLogged) {
    				File[] listOfFiles = tempFolder.listFiles();
    				for (File file : listOfFiles) {
    					
    					boolean isFileCopied = ddpFTPClient.dropFileIntoFTP(ftpClient, file.getAbsolutePath(), fileDestPath+ "/" + file.getName());
    					logger.debug("File name : "+file.getAbsolutePath()+" is " + ( isFileCopied?"copied":"not copied")+" into ftp location");
    				}
    			}
//    		}
		} catch (Exception ex) {
			isExpection = false;
			ex.printStackTrace();
			logger.error("DdpRuleSchedulerJob.performFileTransferUsingFTP(), Expection is occurried while copying file into ftp location",ex);
		} finally {
			try {
				ddpFTPClient.logoutFTP(ftpClient);
			} catch (Exception ex) {
				
			}
			
		}
		
		logger.info("DdpRuleSchedulerJob.performFileTransferUsingFTP() executed successfully.");
		return isExpection;
	}
	
	/**
	 * Method used for perform the Transferring the files using SFTP.
	 * 
	 * @param tempFolder
	 * @param channelSftp
	 * @param destinationLocation
	 * @return
	 */
	public static boolean performTransferUsingSFTP(File tempFolder,ChannelSftp channelSftp,String destFolder,DdpSFTPClient ddpSFTPClient,DdpCommFtp commFtp,CommonUtil commonUtil) {
		
		logger.info("SchedulerJobUtil.performTransferUsingSFTP() - Invoked successfully");
		boolean isTransfered = true;
		try {
		
			if (destFolder.contains("/")) {
				String[] strArray = destFolder.split("/");
				String strFTPURL = destFolder.substring(0, strArray[0].length());
				 //Avoiding "/" for ftp destination path
				destFolder = destFolder.substring(strFTPURL.length()+1);
			}
			channelSftp = commonUtil.connectChannelSftp(ddpSFTPClient, commFtp);
			
			File[] listOfFiles = tempFolder.listFiles();
			for (File file : listOfFiles) {
				if (!channelSftp.isConnected()) {
					logger.info("SchedulerJobUtil.performTransferUsingSFTP() - trying to reconnecting the sftp location");
					channelSftp = commonUtil.connectChannelSftp(ddpSFTPClient, commFtp);
				}
				channelSftp.put(new FileInputStream(file), destFolder+"/"+file.getName(), ChannelSftp.OVERWRITE);
			}
		} catch(Exception ex) {
			isTransfered = false;
			logger.error("SchedulerJobUtils.performFileTransferUsingSFTP(), Expection is occurried while copying file into sftp location",ex);
		} finally {
			if (ddpSFTPClient != null && channelSftp != null)
				ddpSFTPClient.logoutOfSFTP(channelSftp);
		}
	
		return isTransfered;
		
	}
	
	/**
	 * Method used for transferring the files using unc.
	 * 
	 * @param tempFloder
	 * @param DdpCommUnc
	 */
	public static boolean peformFileTransferUsingUNC(File tempFloder,DdpCommUnc ddpUNC) {
		
		logger.debug("DdpRuleSchedulerJob.peformFileTransferUsingUNC() method invoked.");
		boolean isExpection = true;
		 String userName =  ddpUNC.getCunUncUserName(); //"svc-dmsddp";
		 String password = ddpUNC.getCunUncPassword();//"P@ssw0rd123";
		 String domain = "";
		 String destPath = "smb:"+ddpUNC.getCunUncPath(); //scandoc.agilitylogistics.com/svc-dmsddp/DDPDATA-UAT/testing";
		 File[] listOfFiles = tempFloder.listFiles();
		 try {
	  	     DdpUNCClient ddpUNCClient = new DdpUNCClient();
	  	     NtlmPasswordAuthentication auth = ddpUNCClient.uncAutentication(domain, userName, password);
			 for (File file : listOfFiles) {
				boolean isFileCopied = ddpUNCClient.copyFileUsingJcifs(file.getAbsolutePath(), destPath+"/"+file.getName(), auth);
				logger.debug("File name : "+file.getAbsolutePath()+" is " + ( isFileCopied?"copied":"not copied")+" into UNC");
			}
		  } catch (Exception ex) {
			  isExpection = false;
			  logger.error("DdpRuleSchedulerJob.peformFileTransferUsingUNC(), Expection is occurried while copying file into UNC location..",ex);
		  }
		 logger.info("DdpRuleSchedulerJob.peformFileTransferUsingUNC() executed successfully.");
		return isExpection;
	}

	/**
	 * Method used for the deleting the files.
	 * 
	 * @param element
	 */
	public static void deleteFolder(File element) {
		
	    if (element.isDirectory()) {
	        for(File f: element.listFiles()) {	        	
	            deleteFolder(f);
	        }
	    }
	    element.delete();
	}
	
	/**
	 * Method used for copying the file to destination location.
	 * 
	 * @param sourceFile
	 * @param destFile
	 * @return
	 */
	public static boolean copyFile(File sourceFile, File destFile)  {
			
		boolean isFileCopied = false;
		FileChannel source = null;
		FileChannel destination = null;
		
		try {
			if (!sourceFile.exists()) {
				return false;
			}
			if (!destFile.exists()) {
				destFile.createNewFile();
			}
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			if (destination != null && source != null) {
				destination.transferFrom(source, 0, source.size());
			}
			
			isFileCopied = true;
		} catch(Exception ex) {
			logger.error("SchedulerJobUtil.copyFile() : Source file does not exists / due crash while copying into destiion folder ",ex.getMessage());
			ex.printStackTrace();
		} finally {
			
			if (source != null) {
				try {
					source.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (destination != null) {
				try {
					destination.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		
		}
		return isFileCopied;

	}
	
	/**
	 * Method sued for getting the time difference between two dates.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static String timeDifferenceInDays(Date startDate,Date endDate) {
		
		String diff = "";      
		
		long timeDiff = Math.abs(endDate.getTime() - startDate.getTime());      
		diff = String.format("%d hour(s) %d min(s)", TimeUnit.MILLISECONDS.toHours(timeDiff),  TimeUnit.MILLISECONDS.toMinutes(timeDiff) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeDiff)));
		
		return diff;
	}
	
	/**
	 * Method used for transferring the files using ftp.
	 * 
	 * @param tempFolder
	 * @param ftpDetails
	 */
	public static boolean performFileTransferUsingFTPS(File tempFolder,DdpCommFtp ftpDetails) {
		
		logger.debug("DdpRuleSchedulerJob.performFileTransferUsingFTPS() method invoked.");
		boolean isExpection = true;
		String ftpPath = ftpDetails.getCftFtpLocation();
		String fileDestPath = "";
		String strFTPURL = "";
		FTPSClient ftpClient = null;
		DdpFTPSClient ddpFTPClient  = null;
		
		if (ftpPath.contains("/")) {
			
			ftpPath = ftpPath.substring(7, ftpPath.length());
			 String[] strArray = ftpPath.split("/");
			 strFTPURL = ftpPath.substring(0, strArray[0].length());
			 //Avoiding "/" for ftp destination path
			 fileDestPath = ftpPath.substring(strFTPURL.length()+1);
		}
		
		try {
			ddpFTPClient = new DdpFTPSClient();
    		ftpClient = ddpFTPClient.connectFTPS(strFTPURL, ftpDetails.getCftFtpPort().intValue());
    		if (ftpClient != null) {
    			boolean isLogged = ddpFTPClient.loginFTPS(ftpClient, ftpDetails.getCftFtpUserName(), ftpDetails.getCftFtpPassword());
    			if (isLogged) {
    				File[] listOfFiles = tempFolder.listFiles();
    				for (File file : listOfFiles) {
    					
    					boolean isFileCopied = ddpFTPClient.dropFileIntoFTP(ftpClient, file.getAbsolutePath(), fileDestPath+ "/" + file.getName());
    					logger.debug("File name : "+file.getAbsolutePath()+" is " + ( isFileCopied?"copied":"not copied")+" into ftp location");
    				}
    			}
    		}
		} catch (Exception ex) {
			isExpection = false;
			ex.printStackTrace();
			logger.error("DdpRuleSchedulerJob.performFileTransferUsingFTP(), Expection is occurried while copying file into ftp location",ex);
		} finally {
			try {
				ddpFTPClient.logoutFTPS(ftpClient);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		logger.info("DdpRuleSchedulerJob.performFileTransferUsingFTPS() executed successfully.");
		return isExpection;
	}
	
	
}
