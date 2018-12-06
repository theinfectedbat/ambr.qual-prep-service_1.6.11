package com.ambr.gtm.fta.qts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QtxWorkTracker
{
	private static Logger				logger			= LogManager.getLogger(QtxWorkTracker.class);
	private TrackerCodes.AnalysisMethod	analysisMethod;
	private Long						totalComponents	= 0L;
	private Long						m_QualtxWorkId;
	private Long						m_QualtxKey;
	private Long						m_QtxLastModifiedTime;
	private TrackerCodes.QualtxStatus	qtxStatus;
	public boolean						isEligibleForDelete;
	private boolean						waitForNextAnalysisMethodFlg;


	public void setAnalysisMethod(TrackerCodes.AnalysisMethod analysisMethod)
	{
		this.analysisMethod = analysisMethod;
	}

	public Long getQualtxKey()
	{
		return m_QualtxKey;
	}

	public void setQualtxKey(Long m_QualtxKey)
	{
		this.m_QualtxKey = m_QualtxKey;
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
	
	public QtxWorkTracker(Long theQualtxKey,Long theQtxWorkId )
	{
		this.m_QualtxKey=theQualtxKey;
		this.m_QualtxWorkId = theQtxWorkId;
		this.qtxStatus = TrackerCodes.QualtxStatus.INIT;
		this.m_QtxLastModifiedTime = System.currentTimeMillis();
	}

	public QtxWorkTracker(Long theQualtxKey, TrackerCodes.AnalysisMethod theAnalysisMethod, Long theQualtxWorkId) throws Exception
	{
		if (theQualtxKey != null) this.m_QualtxKey = theQualtxKey;
		this.analysisMethod = theAnalysisMethod;
		this.m_QualtxWorkId = theQualtxWorkId;
		this.qtxStatus = TrackerCodes.QualtxStatus.INIT;
		this.m_QtxLastModifiedTime = System.currentTimeMillis();

	}

	public QtxWorkTracker(Long theQualtxKey, TrackerCodes.AnalysisMethod theAnalysisMethod, Long theQualtxWorkId, Long theTotalComponents) throws Exception
	{
		if (theQualtxKey != null) this.m_QualtxKey = theQualtxKey;
		this.analysisMethod = theAnalysisMethod;
		this.m_QualtxWorkId = theQualtxWorkId;
		this.totalComponents = theTotalComponents;
		this.qtxStatus = TrackerCodes.QualtxStatus.INIT;
		this.m_QtxLastModifiedTime = System.currentTimeMillis();

	}

	public void load(Boolean isForceLoad) throws Exception
	{
		// TODO Auto-generated method stub
	}

	public void removeComponent(Long theQualtxCompId) throws Exception
	{
		// TODO Auto-generated method stub

	}

	public TrackerCodes.AnalysisMethod getAnalysisMethod()
	{
		return analysisMethod;
	}

	public Long getTotalComponents()
	{
		return totalComponents;
	}

	public Long getQualtxWorkId()
	{
		return m_QualtxWorkId;
	}

	public Long geQualtxKey()
	{
		return m_QualtxKey;
	}

	public TrackerCodes.QualtxStatus getQtxStatus()
	{
		return qtxStatus;
	}

	public void setQtxStatus(TrackerCodes.QualtxStatus theQtxStatus)
	{
		this.qtxStatus = theQtxStatus;
	}

	public Long getQtxLastModifiedTime()
	{
		return m_QtxLastModifiedTime;
	}

	public void setQtxLastModifiedTime(Long m_QtxLastModifiedTime)
	{
		this.m_QtxLastModifiedTime = m_QtxLastModifiedTime;
	}

	public boolean isEligibleForReload(long theReloadInterval)
	{
		if (System.currentTimeMillis() - this.m_QtxLastModifiedTime > theReloadInterval * 1000) return true;
		return false;
	}

	@Override
	public boolean equals(Object obj)
	{

		if (this == obj) return true;
		if (obj == null || obj.getClass() != this.getClass()) return false;
		QtxWorkTracker qtxTracker = (QtxWorkTracker) obj;

		// comparing the state of argument with
		// the state of 'this' Object.
		return (qtxTracker.m_QualtxWorkId == this.m_QualtxWorkId);
	}

	@Override
	public int hashCode()
	{
		return this.m_QualtxWorkId.intValue();
	}

}
