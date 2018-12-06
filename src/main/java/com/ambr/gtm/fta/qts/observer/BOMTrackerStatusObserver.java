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
	private TrackerContainer trackerContainer;
	JdbcTemplate aTemplate = null;
	
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
		//	JdbcTemplate aTemplate = new JdbcTemplate(this.dataSrc);

			while (true)
			{
				MessageFormatter.trace(logger, "run", "BOMTrackerStatusObserver started to post BOM Post policy works");
				Set<Long> aBOMTrackerKeySet = this.trackerContainer.getBomTrackerMapKeys();
				Iterator<Long> bomTrackerKey = aBOMTrackerKeySet.iterator();
				while (bomTrackerKey.hasNext())
				{
					Long bomKey = bomTrackerKey.next();
					BOMTracker aBomTracker = this.trackerContainer.getBomTracker(bomKey);
					aBomTracker.triggerPostBOMValidationPolicy(aTemplate);
				}
				MessageFormatter.trace(logger, "run", "BOMTrackerStatusObserver Completed to post BOM Post policy works");
				Thread.sleep(THREAD_INTERVAL * 1000);
			}
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "run",theException, "Exception  in  the BOM Status observer : ");
		}
	}

}
