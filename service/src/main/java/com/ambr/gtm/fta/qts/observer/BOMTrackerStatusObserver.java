package com.ambr.gtm.fta.qts.observer;

import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.BOMTracker;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.platform.utils.log.MessageFormatter;

public class BOMTrackerStatusObserver implements Runnable
{
	private static Logger		logger			= LogManager.getLogger(BOMTrackerStatusObserver.class);
	private static int			THREAD_INTERVAL	= 10;
	private TrackerContainer	trackerContainer;
	JdbcTemplate				aTemplate		= null;
	private volatile boolean	exit			= false;
	private volatile boolean	isShutdownSuccess		= false;

	public BOMTrackerStatusObserver(TrackerContainer trackerContainer, JdbcTemplate aTemplate, int interval)
	{
		this.aTemplate = aTemplate;
		this.trackerContainer = trackerContainer;
		THREAD_INTERVAL = interval;
	}


	public void run()
	{
		try
		{
			while (!exit)
			{
				MessageFormatter.trace(logger, "run", "BOMTrackerStatusObserver started to post BOM Post policy works");
				Set<Long> aBOMTrackerKeySet = this.trackerContainer.getBomTrackerMapKeys();
				Iterator<Long> bomTrackerKey = aBOMTrackerKeySet.iterator();
				while (bomTrackerKey.hasNext())
				{
					if(exit) break;
					Long bomKey = bomTrackerKey.next();
					BOMTracker aBomTracker = this.trackerContainer.getBomTracker(bomKey);
					aBomTracker.triggerPostBOMValidationPolicy(aTemplate);
				}
				MessageFormatter.trace(logger, "run", "BOMTrackerStatusObserver Completed to post BOM Post policy works");
				if(!exit) Thread.sleep(THREAD_INTERVAL * 1000);
			}
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "run",theException, "Exception  in  the BOM Status observer : ");
		}
		isShutdownSuccess = true;
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

}
