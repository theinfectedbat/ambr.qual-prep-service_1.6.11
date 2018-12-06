package com.ambr.gtm.fta.qts.work;

import com.ambr.gtm.fta.qts.TrackerCodes;

public class CompHSPullWorkInfo extends QtxCompWorkInfo
{
	private Long						hsPullWorkId;
	private TrackerCodes.QualtxCompHSPullStatus	hsPullStatus;

	public Long getHsPullWorkId()
	{
		return hsPullWorkId;
	}

	public void setHsPullWorkId(Long hsPullWorkId)
	{
		this.hsPullWorkId = hsPullWorkId;
	}

	public TrackerCodes.QualtxCompHSPullStatus getHsPullStatus()
	{
		return hsPullStatus;
	}

	public void setHsPullStatus(TrackerCodes.QualtxCompHSPullStatus hsPullStatus)
	{
		this.hsPullStatus = hsPullStatus;
	}
}
