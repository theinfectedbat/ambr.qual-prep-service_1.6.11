package com.ambr.gtm.fta.qps.gpmclass;

import java.util.Date;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMClassification 
{
	public long		prodKey;
	public long		ctryKey;
	public long		cmplKey;
	public String	ctryCode;
	public Date		effectiveFrom;
	public Date		effectiveTo;
	public String	imHS1;
	public String   isActive;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClassification()
		throws Exception
	{
	}
}
