package com.ambr.gtm.fta.qts.observer;

import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.QtxTracker;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.platform.utils.log.MessageFormatter;

public class QtxStatusObserver implements Runnable
{

	private static Logger							logger			= LogManager.getLogger(BOMTrackerStatusObserver.class);
	private static TrackerCodes.QualTrackerStatus	INIT			= TrackerCodes.QualTrackerStatus.INIT;
	private static int								THREAD_INTERVAL	= 10;
	private TrackerContainer trackerContainer;


	public QtxStatusObserver(TrackerContainer trackerContainer, int interval)
	{
		this.trackerContainer = trackerContainer;
		THREAD_INTERVAL = interval;
	}

	public void run()
	{
		try
		{
			while (true)
			{
				Set<Long> aQtxTrackerKeySet = this.trackerContainer.getQtxTrackerMapKeys();
				Iterator<Long> qtxTrackerKey = aQtxTrackerKeySet.iterator();
				while (qtxTrackerKey.hasNext())
				{
					Long qtxKey = qtxTrackerKey.next();
					QtxTracker aQtxTracker = this.trackerContainer.getQtxTracker(qtxKey);
					if (!getWaiForNextAnalysisMethodFlg(aQtxTracker) && INIT.equals(aQtxTracker.getQtxStatus())) setEligibleQualtxStatus(aQtxTracker);
				}
				Thread.sleep(THREAD_INTERVAL * 1000);
			}
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "run",theException, "Exception  in QtxStatusObserver ");
		}
	}
	
	public boolean getWaiForNextAnalysisMethodFlg(QtxTracker qtxTracker) throws Exception
	{
		boolean aCalculatedStatus = qtxTracker.getCalculatedAnalysisWaitFlg();
		 qtxTracker.setWaitForNextAnalysisMethodFlg(aCalculatedStatus);
		 return aCalculatedStatus;
	}
	
	public void setEligibleQualtxStatus(QtxTracker qtxTracker) throws Exception
	{
		TrackerCodes.QualTrackerStatus aCurrentStatus = qtxTracker.getQtxStatus();
		TrackerCodes.QualTrackerStatus aCalculatedStatus = qtxTracker.getCalculatedQualtxStatus();
		if (aCalculatedStatus != null && !aCalculatedStatus.equals(aCurrentStatus)) qtxTracker.setQtxStatus(aCalculatedStatus);
	}
	
	
}
