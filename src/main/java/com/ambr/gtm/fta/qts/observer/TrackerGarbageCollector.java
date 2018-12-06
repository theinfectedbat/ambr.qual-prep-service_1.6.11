package com.ambr.gtm.fta.qts.observer;

import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.BOMTracker;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.platform.utils.log.MessageFormatter;

public class TrackerGarbageCollector implements Runnable
{

	static Logger	logger			= LogManager.getLogger(TrackerGarbageCollector.class);
	private int		THREAD_INTERVAL	= 60;
	private TrackerContainer trackerContainer;

	public TrackerGarbageCollector(TrackerContainer trackerContainer, int interval)
	{
		this.trackerContainer = trackerContainer;
		THREAD_INTERVAL = interval;
	}

	public void run()
	{
		try
		{
			// JdbcTemplate aTemplate = new JdbcTemplate(this.dataSrc);

			while (true)
			{
				Set<Long> aBOMTrackerKeySet = this.trackerContainer.getBomTrackerMapKeys();
				Iterator<Long> bomTrackerKey = aBOMTrackerKeySet.iterator();
				while (bomTrackerKey.hasNext())
				{
					Long bomKey = bomTrackerKey.next();
					BOMTracker aBOMTracker = this.trackerContainer.getBomTracker(bomKey);
					this.trackerContainer.deleteBOMTracker(aBOMTracker);
				}

				Thread.sleep(THREAD_INTERVAL * 1000);
			}
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "run",theException, "Error in the Tracker Garbage Collector");
		}
	}
}
