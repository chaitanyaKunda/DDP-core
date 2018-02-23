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
public class DdpRateSetupEntity implements RowMapper<DdpRateSetupEntity>{

	private Integer rtsId;
	private Integer rtsRdtId;
	private String rtsOption;
	
	/**
	 * @return the rtsId
	 */
	public Integer getRtsId() {
		return rtsId;
	}

	/**
	 * @param rtsId the rtsId to set
	 */
	public void setRtsId(Integer rtsId) {
		this.rtsId = rtsId;
	}

	/**
	 * @return the rtsRdtId
	 */
	public Integer getRtsRdtId() {
		return rtsRdtId;
	}

	/**
	 * @param rtsRdtId the rtsRdtId to set
	 */
	public void setRtsRdtId(Integer rtsRdtId) {
		this.rtsRdtId = rtsRdtId;
	}

	/**
	 * @return the rtsOption
	 */
	public String getRtsOption() {
		return rtsOption;
	}

	/**
	 * @param rtsOption the rtsOption to set
	 */
	public void setRtsOption(String rtsOption) {
		this.rtsOption = rtsOption;
	}


	@Override
	public DdpRateSetupEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
		
		DdpRateSetupEntity entity = new DdpRateSetupEntity();
		entity.setRtsId(Integer.parseInt(rs.getString("RTS_ID")));
		entity.setRtsRdtId(Integer.parseInt(rs.getString("RTS_RDT_ID")));
		entity.setRtsOption(rs.getString("RTS_OPTION"));
		
		return entity;
	}
	
}
