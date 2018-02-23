/**
 * 
 */
package com.agility.ddp.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author DGuntha
 *
 */
public class FileUtils {

	/**
	 * Method used for finding the size of file in MB.
	 * 
	 * @param inputFile
	 * @return
	 */
	public static long findSizeofFileInMB (File inputFile) {
		
		long sizeInBytes = (inputFile.isFile() ? inputFile.length() : getFolderSize(inputFile));
		//transform in MB
		long sizeInMb = sizeInBytes / (1024 * 1024);
		
		return sizeInMb;
	}
	
	/**
	 * Method used for finding the size of the file in the Kilo Bytes.
	 * 
	 * @param inputFilePath
	 * @return
	 */
	public static long findSizeofFileInKB (String inputFilePath) {
		//long sizeInMbKb = 0;
		long sizeInBytes = 0;
		try {
			File file  = new File(inputFilePath);
			if (file.exists()) {
				sizeInBytes = (file.isFile() ? file.length() : getFolderSize(file));
				//sizeInMbKb = sizeInBytes / (1024);
			}
		} catch (Exception ex) {
			
		}
		return sizeInBytes;
	}
	
	/**
	 * Method used for finding the size of the file in the Kilo Bytes.
	 * 
	 * @param inputFilePath
	 * @return
	 */
	public static long findSizeofFileInKB (File inputFilePath) {
		//long sizeInMbKb = 0;
		long sizeInBytes = 0;
		try {
			if (inputFilePath.exists()) {
				sizeInBytes = (inputFilePath.isFile() ? inputFilePath.length() : getFolderSize(inputFilePath));
				//sizeInMbKb = sizeInBytes / (1024);
			}
		} catch (Exception ex) {
			
		}
		return sizeInBytes;
	}
	
	/**
	 * Method used for getting the size of the folder.
	 * 
	 * @param dir
	 * @return
	 */
	public static long getFolderSize(File dir) {
		
	    long size = 0;
	    
	    for (File file : dir.listFiles()) {
	        if (file.isFile()) 
	            size += file.length();
	        else
	            size += getFolderSize(file);
	    }
	    return size;
	}
	
	
	/**
	 * Method used for creating the file with content.
	 * 
	 * @param fileName
	 * @param content
	 */
	public static void createFileWithContent(String fileName,String content) {
		
		try {
			 
			File file = new File(fileName);
 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();
 
 
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Method used for reading the text file.
	 * 
	 * @param fileName
	 * @return
	 */
	public static String readTextFile(String fileName) {
		
		BufferedReader br = null;
		String output = "";
		try {
			String sCurrentLine;
			
 
			br = new BufferedReader(new FileReader(fileName));
 
			while ((sCurrentLine = br.readLine()) != null) {
				output += sCurrentLine;
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return output;
 
	}
	
	/**
	 * Method used for zip the file in the file system
	 * 
	 * @param zipName
	 * @param inputFolder
	 * @return
	 */
	 public static boolean zipFiles(String zipName,String inputFolder)
	 {
		 boolean isZipped = false;
		 
		 try {
	         BufferedInputStream origin = null;
	         FileOutputStream dest = new FileOutputStream(zipName);
	         ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));
	         //out.setMethod(ZipOutputStream.DEFLATED);
	         byte data[] = new byte[2048];
	         // get a list of files from current directory
	         File f = new File(inputFolder);
	         String files[] = f.list();

	         for (int i=0; i<files.length; i++) {
	          
	            File file = new File(inputFolder+"\\"+files[i]);
	            if (!file.isFile() || file.getName().equalsIgnoreCase("mail.txt"))
	            	continue;
	           
	            FileInputStream fi = new 
	              FileInputStream(inputFolder+"\\"+files[i]);
	            origin = new 
	              BufferedInputStream(fi, 2048);
	            ZipEntry entry = new ZipEntry(files[i]);
	            out.putNextEntry(entry);
	            int count;
	            while((count = origin.read(data, 0,2048)) != -1) {
	               out.write(data, 0, count);
	            }
	            origin.close();
	         }
	         out.close();
	         isZipped = true;
	      } catch(Exception e) {
	         e.printStackTrace();
	      }
		 
		 return isZipped;
	 }
	 
	 /**
	  * Method used for creating the folder.
	  * 
	  * @param folderPath
	  * @return
	  */
	 public static File createFolder(String folderPath) {
		 
	    File sourceFile = new File(folderPath);
	    sourceFile.mkdir();
	    	
	    return sourceFile;
	 }
	 
	 /**
	  * Method used for getting file extension.
	  * 
	  * @param fileName
	  * @return
	  */
	 public static String getFileExtension(String fileName) {
		 
		 String extension = null;
		 
		 try {
		        return fileName.substring(fileName.lastIndexOf(".") );
		    } catch (Exception e) {
		       
		    }
		 return extension;
	 }
	 
	 
	 	/**
		 * Method used for checking the file exists in the FTP, UNC or local file system.
		 * 
		 * @param fileNames
		 * @param fileName
		 * @param sequence
		 * @return
		 */
		public static String checkFileExists(List<String> fileNames,String fileName,String sequence) {
				
			String numberfileExists = null;
			
			for (String file : fileNames) {
				if (file.startsWith(fileName)) {
					if (sequence != null) {
						int number = Integer.parseInt(sequence);
						number += 1;
						numberfileExists = number + "";
					} else{
						numberfileExists = "1";
					}
					
					break;
				}
			}
				
			return numberfileExists;
		}
		
		public static String getFileNameWithOutExtension(String fileName) {
		  
		    try {
		        return fileName.substring(0, fileName.lastIndexOf(".") );
		    } catch (Exception e) {
		        return null;
		    }
		}
		
		/**
		 * Method used for counting the characters.
		 * 
		 * @param string
		 * @param c
		 * @return
		 */
		public static int countCharacter(String string, Character c) {
			
			int count = 0;
			if (string != null) {
				for (int i = 0 ; i < string.length(); i++) {
					if (string.charAt(i) == c) 
						++count;
				}
			}
			
			return count;
		}
		
		
		/**
		 * Method used for checking the file exists in the FTP, UNC or local file system.
		 * 
		 * @param fileNames
		 * @param fileName
		 * @param sequence
		 * @return
		 */
		public static int checkFileExists(List<String> fileNames,String fileName) {
				
			String ext ="."; 
			String fileNameStr = "";
		   		if (fileName != null && fileName.contains(".")) {
			   		String [] files = fileName.split("\\.");
					 ext = ext +files[1];
					 fileNameStr = files[0];
		   		} else {
		   			ext = ".pdf";
		   		}
		   		
			int sequence = 0;
			for (String file : fileNames) {
				
				if (file.startsWith(fileNameStr)) {
						sequence += 1;
				}
			}
				
			return sequence;
		}
		
		/**
		 * Method used to string convert to hexadecimal.
		 * 
		 * @param text
		 * @return
		 */
		public static String toHexString(String text) {
			byte[] ba = text.getBytes();
		    StringBuilder str = new StringBuilder();
		    for(int i = 0; i < ba.length; i++)
		        str.append(String.format("%x", ba[i]));
		    return str.toString();
		}

		/**
		 * Method used to convert Hexadecimal to String.
		 * 
		 * @param hex
		 * @return
		 */
		public static String fromHexString(String hex) {
		    StringBuilder str = new StringBuilder();
		    for (int i = 0; i < hex.length(); i+=2) {
		        str.append((char) Integer.parseInt(hex.substring(i, i + 2), 16));
		    }
		    return str.toString();
		}
		
		/**
		 * Method used for copying the files and zip.
		 * 
		 * @param endSourceFolder
		 * @param zipName
		 * @return
		 */
		public static File copyFilesAndZip(File endSourceFolder,String zipName) {
			String destName = "";
			boolean isZiped  = false;
			
			if(endSourceFolder.isDirectory()) 
				destName = endSourceFolder.getAbsolutePath();
			else 
				destName = endSourceFolder.getAbsolutePath();
			
			destName += "_Zip";
			File destFolder = createFolder(destName);
			String endDestName = destName+ "/"+zipName;
			File endDestFolder = createFolder(endDestName);
			try {
				File[] fileLIst = endSourceFolder.listFiles();
				for (File file : fileLIst) {
					boolean isCopied = copyFile(file, new File(endDestName+"/"+file.getName()));
					if (!isCopied)
						break;
				}
				//org.apache.commons.io.FileUtils.copyDirectory(endDestFolder, endDestFolder);
				//Thread.sleep(20000);
				isZiped= zipFiles(destName+"/"+zipName+"_export.zip", endDestFolder.getAbsolutePath());
			} catch (Exception ex) {
				
			} finally {
				if (isZiped) 
					SchedulerJobUtil.deleteFolder(endDestFolder);
				else 
					SchedulerJobUtil.deleteFolder(destFolder);
			}
			if (!isZiped) {
				destFolder = null;
			}
			return destFolder;
		}
		
		
		
		/**
		 * Method used for copying the file to destination location.
		 * 
		 * @param sourceFile
		 * @param destFile
		 * @return
		 */
		private static boolean  copyFile(File sourceFile, File destFile)  {
				
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
				//logger.error("DdpRuleSchedulerJob.copyFile() : Source file does not exists / due crash while copying into destiion folder ",ex.getMessage());
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

		public static void main(String[] args) {
			//System.out.println(getFileExtension("weelcom.txt.wee.java"));
			//System.out.println(getFileNameWithOutExtension("weelcom.txt.wee.java") + " : Without extension ");
			File file =	copyFilesAndZip(new File("C:\\data\\downloaded\\temp-13648-2016-07-12 20-53-44"), "13648");
			System.out.println(file.getAbsolutePath());
		}
}
