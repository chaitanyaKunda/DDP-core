package com.agility.ddp.core.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.agility.ddp.data.domain.DdpDmsDocsDetail;

/**
 * @author DGuntha
 *
 */
public class NamingConventionUtil {

	/**
	 * 
	 */
	public static String getExportNamingConvension(String conv,DdpDmsDocsDetail dmsDocsDetails,String seqNumber,List<String> fileNames,Map<String,String> loadNumebrMap) {
		
			conv = getDocName(conv, dmsDocsDetails, seqNumber,loadNumebrMap);
			 String seq = FileUtils.checkFileExists(fileNames, conv, null);
			if (seq != null) {
				String fileName = conv;
				fileName += "_Copy_"+seq;
				seq = FileUtils.checkFileExists(fileNames, fileName, seq);
				 if (seq != null) {
					conv = getExportNamingConvension(conv, dmsDocsDetails, seq, fileNames,loadNumebrMap);
				 } else {
					 conv = fileName;
				 }
			}
			
		return conv;
	}
	
	/**
	 * Method used for exporting duplicate Naming convesion.
	 * 
	 * @param conv
	 * @param dmsDocsDetails
	 * @param seqNumber
	 * @param fileNames
	 * @return
	 */
	public static String getExportDuplicateNamingConvension(String conv,DdpDmsDocsDetail dmsDocsDetails,String seqNumber,List<String> fileNames,Map<String,String> loadNumebrMap) {
		String fileName = conv;
		boolean isNotContainsSeq = false;
		
		if (!conv.toLowerCase().contains("%%seq%%"))
			isNotContainsSeq = true;
		
		fileName = getDocName(conv, dmsDocsDetails, seqNumber,loadNumebrMap);
		 String seq = FileUtils.checkFileExists(fileNames, fileName, seqNumber);
		 
		 if (seq != null) {	
			 //fileName = getDocName(conv, dmsDocsDetails, seq);
			 if (isNotContainsSeq) {
					fileName += "_Copy_"+seq;
			}
			 seq = FileUtils.checkFileExists(fileNames, fileName, seqNumber);
			 
			 if (seq != null)
				 fileName = getExportDuplicateNamingConvension(conv, dmsDocsDetails, seq, fileNames,loadNumebrMap);
			 
		 }
		 return fileName;
	}
	
	/**
	 * Method used for getting the Naming Convention.
	 *  
	 * @param conv
	 * @param dmsDocsDetails
	 * @return
	 */
	public static String getDocName(String conv,DdpDmsDocsDetail dmsDocsDetails,String seqNumber,Map<String,String> loadNumberMap) {
		
		if (conv.toUpperCase().contains("%%JOB_NUM%%")) {
			if (dmsDocsDetails.getDddJobNumber() != null && !dmsDocsDetails.getDddJobNumber().isEmpty()) {
				String jobNumber = (dmsDocsDetails.getDddJobNumber() != null ? dmsDocsDetails.getDddJobNumber().replaceAll("[^a-zA-Z0-9_.]", ""):"");
				conv = conv.toUpperCase().replaceAll("%%JOB_NUM%%", jobNumber);
			} else { 
				conv = conv.toUpperCase().replaceAll("%%JOB_NUM%%","");
			}
		}
		
		if (conv.toUpperCase().contains("%%DMS_VERSION%%")) {
				String dmsVersion = (dmsDocsDetails.getDddDocVersion() != null && !dmsDocsDetails.getDddDocVersion().isEmpty()  ? dmsDocsDetails.getDddDocVersion().replaceAll("[^a-zA-Z0-9_.]", "").replaceAll("\\.","_") : "");
				conv = conv.toUpperCase().replaceAll("%%DMS_VERSION%%", dmsVersion);
		}
		

		if (conv.toUpperCase().contains("%%CONS_ID%%")) {
			if (dmsDocsDetails.getDddConsignmentId() != null && !dmsDocsDetails.getDddConsignmentId().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%CONS_ID%%", dmsDocsDetails.getDddConsignmentId());
			else 
				conv = conv.toUpperCase().replaceAll("%%CONS_ID%%", "");
			
		}
		
		if (conv.toUpperCase().contains("%%DOC_TYPE%%")) {
			if (dmsDocsDetails.getDddControlDocType() != null && !dmsDocsDetails.getDddControlDocType().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%DOC_TYPE%%", dmsDocsDetails.getDddControlDocType());
			else 
				conv = conv.toUpperCase().replaceAll("%%DOC_TYPE%%", "");
		}
		
		if (conv.toUpperCase().contains("%%COMPANY%%")) {
			if (dmsDocsDetails.getDddCompanySource() != null && !dmsDocsDetails.getDddCompanySource().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%COMPANY%%", dmsDocsDetails.getDddCompanySource());
			else 
				conv = conv.toUpperCase().replaceAll("%%COMPANY%%", "");
		}
		
		if (conv.toUpperCase().contains("%%BRANCH%%")) {
			if (dmsDocsDetails.getDddBranchSource() != null && !dmsDocsDetails.getDddBranchSource().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%BRANCH%%", dmsDocsDetails.getDddBranchSource());
			else 
				conv = conv.toUpperCase().replaceAll("%%BRANCH%%", "");
		}
		
		//TODO: Need to implement getting the details using the client id.
		if (conv.toUpperCase().contains("%%ENTRY_NUM%%")) {
			if(dmsDocsDetails.getDddCustomsEntryNo().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%ENTRY_NUM%%", "ENTRY_NUM");
			else
				conv = conv.toUpperCase().replaceAll("%%ENTRY_NUM%%", dmsDocsDetails.getDddCustomsEntryNo());
		}
		
		if (conv.toUpperCase().contains("%%DELIVERY_DOC_NUM%%")) {
			conv = conv.toUpperCase().replaceAll("%%DELIVERY_DOC_NUM%%", "");
		}
		
		if (conv.toUpperCase().contains("%%CONS_LOAD_NUM%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%CONS_LOAD_NUM%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%CONS_LOAD_NUM%%", loadNumberMap.get("%%CONS_LOAD_NUM%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%CONS_LOAD_NUM%%", "99999999");
			}
			
		}
		if (conv.toUpperCase().contains("%%JOB_LOAD_NUM%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%JOB_LOAD_NUM%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%JOB_LOAD_NUM%%", loadNumberMap.get("%%JOB_LOAD_NUM%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%JOB_LOAD_NUM%%", "99999999");
			}
			
		}
		
		if (conv.toUpperCase().contains("%%SHIP_REF%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%SHIP_REF%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%SHIP_REF%%", loadNumberMap.get("%%SHIP_REF%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%SHIP_REF%%_", "");
			}
			
		}
		
		if (conv.toUpperCase().contains("%%CONSIGNEE_REF%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%CONSIGNEE_REF%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%CONSIGNEE_REF%%", loadNumberMap.get("%%CONSIGNEE_REF%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%CONSIGNEE_REF%%_", "");
			}
			
		}
		
		if (conv.toUpperCase().contains("%%DATE_")){
			String dateFormat = getDateFormat(conv);
			if (dateFormat != null &&  dateFormat.contains("DATE_"))
				conv = conv.toUpperCase().replaceAll("%%"+dateFormat+"%%", convertDate(dateFormat));
		}
		
		if (conv.toUpperCase().contains("%%SEQ%%")) {
			conv = conv.toUpperCase().replaceAll("%%SEQ%%", seqNumber);
		}
		
		if (conv.toUpperCase().contains("%%SUPP_NAME%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%SUPP_NAME%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%SUPP_NAME%%", loadNumberMap.get("%%SUPP_NAME%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%SUPP_NAME%%", "");
			}
		}
		
		if (conv.toUpperCase().contains("%%C2C_NUM%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%C2C_NUM%%") != null) {
				conv = conv.toUpperCase().replace("%%C2C_NUM%%", loadNumberMap.get("%%C2C_NUM%%"));
			} else {
				conv = conv.toUpperCase().replace("%%C2C_NUM%%", "");
			}
		}
		
		if (conv.toUpperCase().contains("%%DIV_REF%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%DIV_REF%%") != null) {
				conv = conv.toUpperCase().replace("%%DIV_REF%%", loadNumberMap.get("%%DIV_REF%%").replaceAll("[^a-zA-Z0-9_.]", ""));
			} else {
				conv = conv.toUpperCase().replace("%%DIV_REF%%", "");
			}
		}
		
		if (conv.toUpperCase().contains("%%INVOICE_NUM%%")) {
			String docRef = (dmsDocsDetails.getDddDocRef() != null ? dmsDocsDetails.getDddDocRef().replaceAll("[^a-zA-Z0-9_.]", ""):"");
			conv = conv.toUpperCase().replaceAll("%%INVOICE_NUM%%", docRef);
		}
		return conv.trim();
	}
	
	
	/**
	 * Method used for getting document naming.
	 * 
	 * @param conv
	 * @param jobNum
	 * @param consID
	 * @param docType
	 * @param company
	 * @param branch
	 * @param seqNumber
	 * @param invoiceNumber
	 * @return
	 */
	public static String getGenericDocumentName(String conv,String jobNum,String consID,String docType,String company,String branch,
			String seqNumber,String invoiceNumber,String entryNumber,Map<String, String> loadNumberMap,String dmsVersion) {
		
		if (conv.toUpperCase().contains("%%ITN_DATE%%")) {
			
			if (loadNumberMap != null && loadNumberMap.get("%%ITN_DATE%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%ITN_DATE%%", loadNumberMap.get("%%ITN_DATE%%"));
			} else {
				conv = conv.toUpperCase().replace("%%ITN_DATE%%", "");
			}
			
		}
		
				
		if (conv.toUpperCase().contains("%%ITN_NUMBER%%")) {
			
			if (loadNumberMap != null && loadNumberMap.get("%%ITN_NUMBER%%") != null) {
				conv = conv.toUpperCase().replace("%%ITN_NUMBER%%", loadNumberMap.get("%%ITN_NUMBER%%"));
			} else {
				conv = conv.toUpperCase().replace("%%ITN_DATE%%", "");
			}
			
		}
		
		if (conv.toUpperCase().contains("%%INVOICE_NUM%%") && invoiceNumber != null)  {
			 invoiceNumber = invoiceNumber.replaceAll("[^a-zA-Z0-9_.]", "");
			conv = conv.toUpperCase().replace("%%INVOICE_NUM%%", invoiceNumber);
		}
		
		if (conv.toUpperCase().contains("%%CONS_LOAD_NUM%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%CONS_LOAD_NUM%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%CONS_LOAD_NUM%%", loadNumberMap.get("%%CONS_LOAD_NUM%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%CONS_LOAD_NUM%%", "99999999");
			}
			
		}
		if (conv.toUpperCase().contains("%%JOB_LOAD_NUM%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%JOB_LOAD_NUM%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%JOB_LOAD_NUM%%", loadNumberMap.get("%%JOB_LOAD_NUM%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%JOB_LOAD_NUM%%", "99999999");
			}
			
		}
		
		if (conv.toUpperCase().contains("%%SUPP_NAME%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%SUPP_NAME%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%SUPP_NAME%%", loadNumberMap.get("%%SUPP_NAME%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%SUPP_NAME%%", "");
			}
		}
		
		if (conv.toUpperCase().contains("%%C2C_NUM%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%C2C_NUM%%") != null) {
				conv = conv.toUpperCase().replace("%%C2C_NUM%%", loadNumberMap.get("%%C2C_NUM%%"));
			} else {
				conv = conv.toUpperCase().replace("%%C2C_NUM%%", "");
			}
		}
		
		if (conv.toUpperCase().contains("%%SHIP_REF%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%SHIP_REF%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%SHIP_REF%%", loadNumberMap.get("%%SHIP_REF%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%SHIP_REF%%_", "");
			}
			
		}
		
		if (conv.toUpperCase().contains("%%CONSIGNEE_REF%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%CONSIGNEE_REF%%") != null) {
				conv = conv.toUpperCase().replaceAll("%%CONSIGNEE_REF%%", loadNumberMap.get("%%CONSIGNEE_REF%%"));
			} else {
				conv = conv.toUpperCase().replaceAll("%%CONSIGNEE_REF%%_", "");
			}
			
		}
		if (conv.toUpperCase().contains("%%DIV_REF%%")) {
			if (loadNumberMap != null && loadNumberMap.get("%%DIV_REF%%") != null) {
				conv = conv.toUpperCase().replace("%%DIV_REF%%", loadNumberMap.get("%%DIV_REF%%").replaceAll("[^a-zA-Z0-9_.]", ""));
			} else {
				conv = conv.toUpperCase().replace("%%DIV_REF%%", "");
			}
		}
		
		conv = getDocName(conv, jobNum, consID, docType, company, branch, seqNumber,entryNumber,dmsVersion);
		
		return conv;
	}
	
	/**
	 * Method used for getting document name based on following details.
	 * 
	 * @param conv
	 * @param jobNum
	 * @param consID
	 * @param docType
	 * @param company
	 * @param branch
	 * @param seqNumber
	 * @return
	 */
	public static String getDocName(String conv,String jobNum,String consID,String docType,String company,String branch,
			String seqNumber,String entryNumber,String dmsVersion) {
		
		if (conv.toUpperCase().contains("%%DMS_VERSION%%")) {
			 dmsVersion = (dmsVersion != null && !dmsVersion.isEmpty()  ? dmsVersion.replaceAll("[^a-zA-Z0-9_.]", "").replaceAll("\\.","_") : "");
			conv = conv.toUpperCase().replaceAll("%%DMS_VERSION%%", dmsVersion);
		}
		
		if (conv.toUpperCase().contains("%%JOB_NUM%%")) {
			//if (dmsDocsDetails.getDddJobNumber() != null && !dmsDocsDetails.getDddJobNumber().isEmpty())
			String jobNumber = (jobNum != null ? jobNum.replaceAll("[^a-zA-Z0-9_.]", ""):"");
			conv = conv.toUpperCase().replaceAll("%%JOB_NUM%%", jobNumber);
			//else 
			//	conv = conv.replace("%%JOB_NUM%%","");
		}
		
		if (conv.toUpperCase().contains("%%CONS_ID%%")) {
		//	if (dmsDocsDetails.getDddConsignmentId() != null && !dmsDocsDetails.getDddConsignmentId().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%CONS_ID%%", consID);
			//else 
			//	conv = conv.replace("%%CONS_ID%%", "");
			
		}
		
		if (conv.toUpperCase().contains("%%DOC_TYPE%%")) {
			//if (dmsDocsDetails.getDddControlDocType() != null && !dmsDocsDetails.getDddControlDocType().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%DOC_TYPE%%", docType);
			//else 
				//conv = conv.replace("%%DOC_TYPE%%", "");
		}
		
		if (conv.toUpperCase().contains("%%COMPANY%%")) {
			//if (dmsDocsDetails.getDddCompanySource() != null && !dmsDocsDetails.getDddCompanySource().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%COMPANY%%", company);
			//else 
			//	conv = conv.replace("%%COMPANY%%", "");
		}
		
		if (conv.toUpperCase().contains("%%BRANCH%%")) {
			//if (dmsDocsDetails.getDddBranchSource() != null && !dmsDocsDetails.getDddBranchSource().isEmpty())
				conv = conv.toUpperCase().replaceAll("%%BRANCH%%", branch);
			//else 
				//conv = conv.replace("%%BRANCH%%", "");
		}
		
		//TODO: Need to implement getting the details using the client id.
		if (conv.toUpperCase().contains("%%ENTRY_NUM%%")) {
			if (entryNumber == null)
				conv = conv.toUpperCase().replaceAll("%%ENTRY_NUM%%", "ENTRY_NUM");
			else
				conv = conv.toUpperCase().replaceAll("%%ENTRY_NUM%%", entryNumber);
		}
		
		if (conv.toUpperCase().contains("%%DELIVERY_DOC_NUM%%")) {
			conv = conv.toUpperCase().replaceAll("%%DELIVERY_DOC_NUM%%", "DELIVERY_DOC_NUM");
		}
			
		if (conv.toUpperCase().contains("%%DATE_")){
			String dateFormat = getDateFormat(conv);
			if (dateFormat != null &&  dateFormat.contains("DATE_"))
				conv = conv.replaceAll("%%"+dateFormat+"%%", convertDate(dateFormat));
		}
		
		if (conv.toUpperCase().contains("%%SEQ%%")) {
			conv = conv.toUpperCase().replace("%%SEQ%%", seqNumber);
		}
		
		return conv.trim();
		
	}
	
	/**
	 * Method used for getting the date format.
	 * 
	 * @param conv
	 * @return
	 */
	public static String getDateFormat(String conv) {
		String[] convs = conv.split("%%");
		String date = null;
		for (String con  : convs) {
			if (con != null && con.toUpperCase().startsWith("DATE_")) {
				date = con;
				break;
			}
		}
		return date;
		
	}
	
	/**
	 * Method used for converting the date format.
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate(String date) {
				
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMMdd");
		String[] dates = date.split("_");
		String pattern = dates[1];
		pattern = pattern.replace("Y", "y");
		pattern = pattern.replace("D", "d");
		dateFormat.applyLocalizedPattern(pattern);
		date = dateFormat.format(new Date());
		
		return date.toUpperCase();
	}
	
}
