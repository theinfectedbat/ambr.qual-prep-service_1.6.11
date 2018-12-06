package com.ambr.gtm.fta.qts;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qts.TrackerCodes.QualTrackerStatus;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.gtm.fta.qts.work.QtxWorkInfo;
import com.ambr.platform.utils.log.MessageFormatter;

public class QtxTracker
{
	static Logger									logger					= LogManager.getLogger(QtxTracker.class);
	private static TrackerCodes.QualTrackerStatus	QUALIFICATION_FAILED	= TrackerCodes.QualTrackerStatus.QUALIFICATION_FAILED;
	private static TrackerCodes.QualTrackerStatus	QUALIFICATION_COMPLETE	= TrackerCodes.QualTrackerStatus.QUALIFICATION_COMPLETE;

	private Long									m_QualtxKey;
	private TrackerCodes.QualTrackerStatus			qtxStatus;
	private Set<QtxWorkTracker>						qtxWorkTrackerSet		= new HashSet<QtxWorkTracker>();
	private Set<QtxWorkTracker>						qtxQualCompletedWorkSet	= new HashSet<QtxWorkTracker>();
	public boolean									isEligibleForDelete;
	private boolean									waitForNextAnalysisMethodFlg;
	private TrackerContainer trackerContainer;
	public Set<QtxWorkTracker> getQtxQualCompletedWorkList()
	{
		return qtxQualCompletedWorkSet;
	}

	public void setQtxQualCompletedWorkList(Set<QtxWorkTracker> qtxQualCompletedWorkList)
	{
		this.qtxQualCompletedWorkSet = qtxQualCompletedWorkList;
	}

	public Set<QtxWorkTracker> getQtxWorkTrackerList()
	{
		return qtxWorkTrackerSet;
	}

	public void setQtxWorkTrackerList(Set<QtxWorkTracker> qtxWorkTrackerList)
	{
		this.qtxWorkTrackerSet = qtxWorkTrackerList;
	}

	public QtxTracker(Long theQualtxKey, TrackerContainer trackerContainer)
	{
		this.m_QualtxKey = theQualtxKey;
		this.trackerContainer = trackerContainer;
		this.qtxStatus = TrackerCodes.QualTrackerStatus.INIT;
		this.waitForNextAnalysisMethodFlg=false;
	}

	public Long getQualtxKey()
	{
		return m_QualtxKey;
	}

	
	public QualTrackerStatus getQtxStatus()
	{
		return qtxStatus;
	}
	
	public void setQtxStatus(QualTrackerStatus theQtxStatus)
	{
		this.qtxStatus = theQtxStatus;
	}
	
	public boolean isWaitForNextAnalysisMethodFlg()
	{
		return waitForNextAnalysisMethodFlg;
	}

	public void setWaitForNextAnalysisMethodFlg(boolean waitForNextAnalysisMethodFlg)
	{
		this.waitForNextAnalysisMethodFlg = waitForNextAnalysisMethodFlg;
	}

	public QtxWorkTracker addQualtxWork(QtxWorkInfo qtxWork) throws Exception
	{
		QtxWorkTracker qtxWorkTracker = null;

		Long aBomKey = qtxWork.getBomKey();
		Long aQtxKey = qtxWork.getQtxKey();
		Long aQtxWorkId = qtxWork.getQtxWorkId();
		TrackerCodes.QualtxStatus aStatus = qtxWork.getQtxStatus();
		if (aBomKey != null && aQtxKey != null && aQtxWorkId != null && aStatus != null)
		{
			try
			{
				qtxWorkTracker = this.trackerContainer.getWorkQtxTracker(aQtxKey,aQtxWorkId);

				if (qtxWorkTracker != null && !this.qtxWorkTrackerSet.contains(qtxWorkTracker))
				{
					this.isEligibleForDelete = false;
					this.qtxStatus = TrackerCodes.QualTrackerStatus.INIT;
				}
				qtxWorkTracker.setQtxStatus(aStatus);
				qtxWorkTracker.setQualtxKey(aQtxKey);
				if (qtxWork.getTotalComponents() != null && qtxWork.getTotalComponents() != 0L)
				{
					qtxWorkTracker.setTotalComponents(qtxWork.getTotalComponents());
				}
				if (qtxWork.getAnalysisMethod() != null) qtxWorkTracker.setAnalysisMethod(qtxWork.getAnalysisMethod());
				if(qtxWork.isWaitForNextAnalysisMethodFlg())  qtxWorkTracker.setWaitForNextAnalysisMethodFlg(true);
				synchronized (this.qtxWorkTrackerSet)
				{
					this.qtxWorkTrackerSet.add(qtxWorkTracker);
				}
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "addQualtxWork",e, "Exception while adding Qualtx work record into QTX Tracker, BOM key [{0}]  , QTX key: [{1}],QTX Work Id: [{2}] ",aBomKey,aQtxKey,aQtxWorkId);
			}
		}
		return qtxWorkTracker;
	}
	
	
	public TrackerCodes.QualTrackerStatus getCalculatedQualtxStatus() throws Exception
	{
		TrackerCodes.QualTrackerStatus aCalculatedStatus = null;
		boolean isQualificationCompleted = false;
		synchronized (this.qtxWorkTrackerSet)
		{
			for (QtxWorkTracker qtxWorkTracker : qtxWorkTrackerSet)
			{
				if(qtxWorkTracker.isEligibleForDelete) continue;
				TrackerCodes.QualtxStatus aQtxWorkStatus = qtxWorkTracker.getQtxStatus();
				if (!TrackerCodes.QualtxStatus.QUALIFICATION_FAILED.equals(aQtxWorkStatus) && !TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.equals(aQtxWorkStatus)  && !TrackerCodes.QualtxStatus.QUALTX_PREP_FAILED.equals(aQtxWorkStatus))
				{
					isQualificationCompleted = false;
					return null;
				}
				else if (TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.equals(aQtxWorkStatus))
				{
					isQualificationCompleted = true;
					synchronized (this.qtxQualCompletedWorkSet)
					{  
						this.qtxQualCompletedWorkSet.add(qtxWorkTracker);
					}
				}else{
					aCalculatedStatus = QUALIFICATION_FAILED;
				}
			}
		}
		if (isQualificationCompleted) aCalculatedStatus = QUALIFICATION_COMPLETE;
		return aCalculatedStatus;
	}
	
	public boolean getCalculatedAnalysisWaitFlg() throws Exception
	{
		if (this.qtxWorkTrackerSet.size() < 1) return false;
		synchronized (this.qtxWorkTrackerSet)
		{
			for (QtxWorkTracker qtxWorkTracker : this.qtxWorkTrackerSet)
			{
				if (!qtxWorkTracker.isWaitForNextAnalysisMethodFlg()) return false;
			}

		}
		return true;
	}
	
	public void clearQtxWorkTrackerList() throws Exception
	{
		synchronized (qtxWorkTrackerSet)
		{
			qtxWorkTrackerSet.clear();
		}
	}
	
	public void deleteQtxWorkTrackers(Set<QtxWorkTracker> QtxWorkTrackerSet) throws Exception
	{
		if (QtxWorkTrackerSet != null && QtxWorkTrackerSet.size() > 0)
		{
			synchronized (qtxWorkTrackerSet)
			{
				qtxWorkTrackerSet.removeAll(QtxWorkTrackerSet);
			}
		}
	}
	public void deleteQtxWorkTracker(QtxWorkTracker QtxWorkTracker) throws Exception
	{
		if (QtxWorkTracker != null)
		{
			synchronized (qtxWorkTrackerSet)
			{
				qtxWorkTrackerSet.remove(QtxWorkTracker);
			}
		}
	}
	
	public void clearQtxCompltedWorkTracker() throws Exception
	{
			synchronized (qtxQualCompletedWorkSet)
			{
				qtxQualCompletedWorkSet.clear();
			}
	}
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_QualtxKey == null) ? 0 : m_QualtxKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null) return false;
		if (this == obj) return true;
		if (getClass() != obj.getClass()) return false;
		QtxTracker other = (QtxTracker) obj;
		if (m_QualtxKey == null)
		{
			if (other.m_QualtxKey != null) return false;
		}
		else if (!m_QualtxKey.equals(other.m_QualtxKey)) return false;
		return true;
	}
}
