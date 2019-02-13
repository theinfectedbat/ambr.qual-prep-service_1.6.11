package com.ambr.gtm.fta.qts.work;

import com.ambr.gtm.fta.qts.TrackerCodes;

public class QtxWorkInfo extends WorkInfo
{
	
	private TrackerCodes.AnalysisMethod	analysisMethod;
	private Long						totalComponents;
	private TrackerCodes.QualtxStatus	qtxStatus;
	private boolean						waitForNextAnalysisMethodFlg;

	public TrackerCodes.QualtxStatus getQtxStatus()
	{
		return qtxStatus;
	}

	public void setQtxStatus(TrackerCodes.QualtxStatus qtxStatus)
	{
		this.qtxStatus = qtxStatus;
	}

	

	public TrackerCodes.AnalysisMethod getAnalysisMethod()
	{
		return analysisMethod;
	}

	public void setAnalysisMethod(TrackerCodes.AnalysisMethod analysisMethod)
	{
		this.analysisMethod = analysisMethod;
	}

	public Long getTotalComponents()
	{
		return totalComponents;
	}

	public void setTotalComponents(Long totalComponents)
	{
		this.totalComponents = totalComponents;
	}

	public boolean isWaitForNextAnalysisMethodFlg()
	{
		return waitForNextAnalysisMethodFlg;
	}

	public void setWaitForNextAnalysisMethodFlg(boolean waitForNextAnalysisMethodFlg)
	{
		this.waitForNextAnalysisMethodFlg = waitForNextAnalysisMethodFlg;
	}
	
}
