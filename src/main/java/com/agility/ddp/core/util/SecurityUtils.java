/**
 * 
 */
package com.agility.ddp.core.util;

import java.math.BigInteger;

import org.jasypt.util.text.BasicTextEncryptor;

/**
 * @author dguntha
 *
 */
public final class SecurityUtils {

	private SecurityUtils() {
	}
	
	/**
	 * Method used for encrypt the plan text.
	 * 
	 * @param planText
	 * @return
	 */
	public static String encryption(String planText) {
		
		String encryptedText = "";
		
		BasicTextEncryptor  encryptor = new BasicTextEncryptor ();
		encryptor.setPassword("agility@123");                     // we HAVE TO set a password
		//encryptor.setAlgorithm("PBEWithMD5AndTripleDES");    // optionally set the algorithm
		
		 encryptedText = encryptor.encrypt(planText);
		 
		 return encryptedText;
		
	}
	
	/**
	 * Method used for decrypt the encrypted text into plan text.
	 * 
	 * @param encryptedText
	 * @return
	 */
	public static String decryption(String encryptedText) {
		
		String planText = "";
		BasicTextEncryptor encryptor = new BasicTextEncryptor();
		encryptor.setPassword("agility@123");                     // we HAVE TO set a password
		//encryptor.setAlgorithm("PBEWithMD5AndTripleDES");    // optionally set the algorithm
		planText = encryptor.decrypt(encryptedText);
		
		return planText;
	}
	
	/**
	 * Method used for converting the number into encrypted format.
	 * 
	 * @param planText
	 * @return
	 */
	public static String agilityEncryptionOnlyNumbers(String planText) {
	
		String passWord = "543210";
		BigInteger encyHex = new BigInteger(FileUtils.toHexString(planText));
		BigInteger passHex = new BigInteger(FileUtils.toHexString(passWord));
		//System.out.println(pass + "Hexdemial Text :: "+hex);
		BigInteger encHex = encyHex.add(passHex);
		//System.out.println("Hex text "+toHex("1234568900887777"));
		String encryptedtext = FileUtils.fromHexString(encHex+"");
		
		return encryptedtext;
	}
	
	/**
	 * Method used for converting encrypted string into orginal string.
	 *  
	 * @param encryptedText
	 * @return
	 */
	public static String agilityDecryptionOnlyNumbers(String encryptedText) {
		
		String passWord = "543210";
		BigInteger decrHex = new BigInteger(FileUtils.toHexString(encryptedText));
		BigInteger passHex = new BigInteger(FileUtils.toHexString(passWord));
		
		BigInteger encHex = decrHex.subtract(passHex);
		
		String decryptedText = FileUtils.fromHexString(encHex+"");
		
		return decryptedText;
		
	}
		

	public static void main(String[] args) {
		//String planText = "QGXij3ingzg05zvTkooQ1A==";
		//String encryptedText = encryption(planText);
		//System.out.println("Encrypted TExt ::: "+encryptedText);
		
	
		String encryptedText = agilityEncryptionOnlyNumbers("1");
		System.out.println(" Encrypted text : "+encryptedText);
		String planText = agilityDecryptionOnlyNumbers(encryptedText);
		System.out.println("Plan text "+planText);
		
	}
}
