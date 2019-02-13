package com.ambr.gtm.fta.qts.work;

import com.ambr.gtm.fta.qts.TrackerCodes;

public class HeaderHSPullWorkInfo extends QtxWorkInfo
{
	private Long hsWorkId;
	
	private TrackerCodes.QualtxHSPullStatus headerHSStatus;
	public Long getHsWorkId()
	{
		return hsWorkId;
	}
	public void setHsWorkId(Long hsWorkId)
	{
		this.hsWorkId = hsWorkId;
	}
	public TrackerCodes.QualtxHSPullStatus getHeaderHSStatus()
	{
		return headerHSStatus;
	}
	public void setHeaderHSStatus(TrackerCodes.QualtxHSPullStatus headerHSStatus)
	{
		this.headerHSStatus = headerHSStatus;
	}
}
