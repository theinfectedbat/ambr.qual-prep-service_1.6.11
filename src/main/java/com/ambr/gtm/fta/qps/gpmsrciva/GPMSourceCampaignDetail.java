package com.ambr.gtm.fta.qps.gpmsrciva;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Date;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceCampaignDetail implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String		campID;
	public String		ftaCode;
	public String		prevYearQualOverride;
	public Date			prevYearQualOverrideDate;
	
	
	public GPMSourceCampaignDetail()
	{

	}
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theResultSet
     *************************************************************************************
     */
	public GPMSourceCampaignDetail(ResultSet theResultSet)
		throws Exception
	{
		this.campID = theResultSet.getString("camp_id");
		this.ftaCode = theResultSet.getString("fta_code");
		this.prevYearQualOverride = theResultSet.getString("prev_year_qual_override");
		this.prevYearQualOverrideDate = theResultSet.getTimestamp("prev_year_qual_override_date");
	}
}
