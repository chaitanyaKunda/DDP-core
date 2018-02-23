/**
 * 
 */
package com.agility.ddp.core.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

/**
 * @author DGuntha
 *
 */
public class RuleEntity implements RowMapper<RuleEntity> {
	
	private Integer rdtId;
	
	private Integer rdtRuleId;
	
	private String rdtCompany;
	
	private String rdtBranch;
	
	private String rdtPartyCode;
	
	private String rdtPartyId;
	
	private String rdtDocType;
	
	private String rdtDepartment;

	/**
	 * @return the rdtId
	 */
	public Integer getRdtId() {
		return rdtId;
	}

	/**
	 * @param rdtId the rdtId to set
	 */
	public void setRdtId(Integer rdtId) {
		this.rdtId = rdtId;
	}

	/**
	 * @return the rdtRuleId
	 */
	public Integer getRdtRuleId() {
		return rdtRuleId;
	}

	/**
	 * @param rdtRuleId the rdtRuleId to set
	 */
	public void setRdtRuleId(Integer rdtRuleId) {
		this.rdtRuleId = rdtRuleId;
	}

	/**
	 * @return the rdtCompany
	 */
	public String getRdtCompany() {
		return rdtCompany;
	}

	/**
	 * @param rdtCompany the rdtCompany to set
	 */
	public void setRdtCompany(String rdtCompany) {
		this.rdtCompany = rdtCompany;
	}


	/**
	 * @return the rdtBranch
	 */
	public String getRdtBranch() {
		return rdtBranch;
	}


	/**
	 * @param rdtBranch the rdtBranch to set
	 */
	public void setRdtBranch(String rdtBranch) {
		this.rdtBranch = rdtBranch;
	}

	/**
	 * @return the rdtPartyCode
	 */
	public String getRdtPartyCode() {
		return rdtPartyCode;
	}


	/**
	 * @param rdtPartyCode the rdtPartyCode to set
	 */
	public void setRdtPartyCode(String rdtPartyCode) {
		this.rdtPartyCode = rdtPartyCode;
	}


	/**
	 * @return the rdtPartyId
	 */
	public String getRdtPartyId() {
		return rdtPartyId;
	}


	/**
	 * @param rdtPartyId the rdtPartyId to set
	 */
	public void setRdtPartyId(String rdtPartyId) {
		this.rdtPartyId = rdtPartyId;
	}

	/**
	 * @return the rdtDocType
	 */
	public String getRdtDocType() {
		return rdtDocType;
	}

	/**
	 * @param rdtDocType the rdtDocType to set
	 */
	public void setRdtDocType(String rdtDocType) {
		this.rdtDocType = rdtDocType;
	}


	/**
	 * @return the rdtDepartment
	 */
	public String getRdtDepartment() {
		return rdtDepartment;
	}

	/**
	 * @param rdtDepartment the rdtDepartment to set
	 */
	public void setRdtDepartment(String rdtDepartment) {
		this.rdtDepartment = rdtDepartment;
	}

	@Override
	public RuleEntity mapRow(ResultSet rs, int rowNum) throws SQLException {

		RuleEntity entity = new RuleEntity();
		entity.setRdtId(Integer.parseInt(rs.getString("RDT_ID")));
		entity.setRdtRuleId(Integer.parseInt(rs.getString("RDT_RULE_ID")));
		entity.setRdtCompany(rs.getString("RDT_COMPANY"));
		entity.setRdtBranch(rs.getString("RDT_BRANCH"));
		entity.setRdtDocType(rs.getString("RDT_DOC_TYPE"));
		entity.setRdtPartyCode(rs.getString("RDT_PARTY_CODE"));
		entity.setRdtPartyId(rs.getString("RDT_PARTY_ID"));
		entity.setRdtDepartment(rs.getString("RDT_DEPARTMENT"));
		
		return entity;
	}

}
