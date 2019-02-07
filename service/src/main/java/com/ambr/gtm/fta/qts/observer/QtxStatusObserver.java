package com.ambr.gtm.fta.qts.observer;

import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	private volatile boolean exit = false;
	private volatile boolean	isShutdownSuccess		= false;
	public QtxStatusObserver(TrackerContainer trackerContainer, int interval)
	{
		this.trackerContainer = trackerContainer;
		THREAD_INTERVAL = interval;
	}

	public void run()
	{
		try
		{
			while (!exit)
			{
				Set<Long> aQtxTrackerKeySet = this.trackerContainer.getQtxTrackerMapKeys();
				Iterator<Long> qtxTrackerKey = aQtxTrackerKeySet.iterator();
				while (qtxTrackerKey.hasNext())
				{
					Long qtxKey = qtxTrackerKey.next();
					QtxTracker aQtxTracker = this.trackerContainer.getQtxTracker(qtxKey);
					if (!getWaiForNextAnalysisMethodFlg(aQtxTracker) && INIT.equals(aQtxTracker.getQtxStatus())) setEligibleQualtxStatus(aQtxTracker);
					if (exit) break;
				}
				if (!exit) Thread.sleep(THREAD_INTERVAL * 1000);
			}
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "run",theException, "Exception  in QtxStatusObserver ");
		}
		isShutdownSuccess = true;
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
	
	public void shutdown()
	{
		exit = true;
		
		while (true)
		{
			if (isShutdownSuccess) break;
			try
			{
				Thread.sleep(5000);
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "run",e , "Exception  in shuttingdown the BOM Status observer ");
			}
		}
	}
	
	public void ensureShutdown()
	{
		while (true)
		{
			if (isShutdownSuccess) break;
			try
			{
				Thread.sleep(5000);
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "run", e, "Exception  in shuttingdown the Qualtx Status observer ");
			}
		}
	}
	
}
