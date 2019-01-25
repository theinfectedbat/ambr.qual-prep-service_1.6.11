package com.ambr.gtm.fta.qts.observer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.QtxWorkTracker;
import com.ambr.gtm.fta.qts.TrackerLoader;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.platform.utils.log.MessageFormatter;

public class ReloadQtxWorkObserver implements Runnable
{
	private static Logger		logger			= LogManager.getLogger(ReloadQtxWorkObserver.class);
	private int				RELOAD_INTERVAL	= 3600;
	private JdbcTemplate		aTemplate		= null;
	private TrackerContainer	trackerContainer;
    private TrackerLoader trackerLoader;
	private volatile boolean	exit			= false;
	public ReloadQtxWorkObserver(TrackerContainer trackerContainer, JdbcTemplate aTemplate, TrackerLoader trackerLoader,  int reloadInterval)

	{
		this.aTemplate = aTemplate;
		this.trackerContainer = trackerContainer;
		this.trackerLoader = trackerLoader;
		RELOAD_INTERVAL = reloadInterval;
	}

	public void run()
	{
		try
		{
			while (!exit)
			{
				Set<QtxWorkTracker> qtxReloadList = new HashSet<>();

				Map<Long, QtxWorkTracker> aQtxWorkTrackerMap = this.trackerContainer.getQtxWorkTrackerMap();
				Set<QtxWorkTracker> aQtxWorkTrackerSet = aQtxWorkTrackerMap.values().stream().collect(Collectors.toSet());
				aQtxWorkTrackerSet.forEach(aQtxWorkTracker -> {
					if (aQtxWorkTracker.isEligibleForReload(RELOAD_INTERVAL)) qtxReloadList.add(aQtxWorkTracker);
				});

				if (!qtxReloadList.isEmpty())
				{
					
					this.trackerLoader.reloadTracker(aTemplate, qtxReloadList);
				}
				if (!exit) Thread.sleep(RELOAD_INTERVAL * 1000);
			}
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "run", theException, "Error while checking reloading Qtx work status");
		}
	}
	
	public void shutdown()
	{
		exit = true;
	}
}
