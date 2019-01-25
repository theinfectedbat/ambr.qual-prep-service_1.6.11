package com.ambr.gtm.fta.qts.work;

import com.ambr.gtm.fta.qts.TrackerCodes;

public class QtxCompWorkInfo extends WorkInfo
{
	private Long	qtxCompKey;
	private Long	qtxCompWorkId;
	private TrackerCodes.QualtxCompStatus qtxCompStatus;

	public TrackerCodes.QualtxCompStatus getQtxCompStatus()
	{
		return qtxCompStatus;
	}

	public void setQtxCompStatus(TrackerCodes.QualtxCompStatus qtxComStatus)
	{
		this.qtxCompStatus = qtxComStatus;
	}

	public Long getQtxCompKey()
	{
		return qtxCompKey;
	}

	public void setQtxCompKey(Long qtxCompKey)
	{
		this.qtxCompKey = qtxCompKey;
	}

	public Long getQtxCompWorkId()
	{
		return qtxCompWorkId;
	}

	public void setQtxCompWorkId(Long qtxCompWorkId)
	{
		this.qtxCompWorkId = qtxCompWorkId;
	}

}
