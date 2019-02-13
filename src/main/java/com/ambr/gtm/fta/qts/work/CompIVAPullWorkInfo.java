package com.ambr.gtm.fta.qts.work;

import com.ambr.gtm.fta.qts.TrackerCodes;

public class CompIVAPullWorkInfo extends QtxCompWorkInfo
{
	private Long						ivaWorkId;
	private TrackerCodes.QualtxCompIVAPullStatus	ivaStatus;

	public Long getIvaWorkId()
	{
		return ivaWorkId;
	}

	public void setIvaWorkId(Long ivaWorkId)
	{
		this.ivaWorkId = ivaWorkId;
	}

	public TrackerCodes.QualtxCompIVAPullStatus getIvaStatus()
	{
		return ivaStatus;
	}

	public void setIvaStatus(TrackerCodes.QualtxCompIVAPullStatus ivaStatus)
	{
		this.ivaStatus = ivaStatus;
	}


}
