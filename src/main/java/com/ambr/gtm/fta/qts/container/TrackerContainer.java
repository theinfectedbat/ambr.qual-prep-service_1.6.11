package com.ambr.gtm.fta.qts.container;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qts.BOMTracker;
import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.QtxTracker;
import com.ambr.gtm.fta.qts.QtxWorkTracker;
import com.ambr.platform.utils.log.MessageFormatter;

public class TrackerContainer
{
	static Logger		logger = LogManager.getLogger(TrackerContainer.class);
	private  QTXWorkRepository workRepository;
	private Map<Long, BOMTracker>		bomTrackerMap		= new ConcurrentHashMap<Long, BOMTracker>();
	private Map<Long, QtxWorkTracker>	qtxWorkTrackerMap	= new ConcurrentHashMap<Long, QtxWorkTracker>();
	private Map<Long, QtxTracker>		qtxTrackerMap		= new ConcurrentHashMap<Long, QtxTracker>();
	
	public TrackerContainer(QTXWorkRepository workRepository)
	{
		this.workRepository = workRepository;
	}

	public TrackerContainer()
	{
	}
	public Set<Long> getBomTrackerMapKeys()
	{
		return bomTrackerMap.keySet();
	}

	public BOMTracker getBomTracker(Long bomKey)
	{
		return this.bomTrackerMap.computeIfAbsent(bomKey, k->  new BOMTracker(bomKey, workRepository, this));
	}

	public Set<QtxTracker> getQtxTrackerList(Long bomKey)
	{
		BOMTracker aBOMTracker = getBomTracker(bomKey);
		return aBOMTracker != null ? aBOMTracker.getQtxTrackerList() : null;
	}

	public QtxWorkTracker getWorkQtxTracker(Long theQualtxKey,Long qtxWorkId)
	{
		return this.qtxWorkTrackerMap.computeIfAbsent(qtxWorkId, k->  new QtxWorkTracker(theQualtxKey,qtxWorkId));
	}
	
	public Set<Long> getQtxWorkTrackerMapKeys()
	{
		return qtxWorkTrackerMap.keySet();
	}
	
	public QtxTracker getQtxTracker(Long theQualtxKey)
	{
		return this.qtxTrackerMap.computeIfAbsent(theQualtxKey, k->  new QtxTracker(theQualtxKey, this));
	}
	
	public Set<Long> getQtxTrackerMapKeys()
	{
		return qtxTrackerMap.keySet();
	}
	
	
	public Map<Long, QtxWorkTracker> getQtxWorkTrackerMap()
	{
		return qtxWorkTrackerMap;
	}

	public void addBOMTracker(Long theBOMKey, BOMTracker theBOMTracker) throws Exception
	{
		
		this.bomTrackerMap.putIfAbsent(theBOMKey, theBOMTracker);

	}
	
	public void deleteQtxTrackers(Set<QtxTracker> qtxTrackerList) throws Exception
	{
		if (qtxTrackerList != null && qtxTrackerList.size() > 0)
		{
			for (QtxTracker qtxTracker : qtxTrackerList)
			{
				Set<QtxWorkTracker> aQtxWorkList = qtxTracker.getQtxWorkTrackerList();
				qtxTracker.deleteQtxWorkTrackers(aQtxWorkList);
				deleteQtxWorkTrackers(aQtxWorkList);
				this.qtxTrackerMap.remove(qtxTracker.getQualtxKey());
			}
		}
	}
	
	public void deleteBOMTracker(BOMTracker bomTracker) throws Exception
	{
		if (bomTracker != null)
		{
			MessageFormatter.debug(logger, "deleteBOMTracker", "Deleting bom tacker for :[{0}]",bomTracker.bomKey);
			Set<QtxTracker>	qtxTrackerDeleteEligibleList	= new HashSet<QtxTracker>();
			Set<QtxTracker> aQtxTrackerSet= bomTracker.getQtxTrackerList();
			synchronized (aQtxTrackerSet)
			{
				for (QtxTracker qtxTracker : aQtxTrackerSet)
				{
					if (qtxTracker.isEligibleForDelete)
					{
						qtxTrackerDeleteEligibleList.add(qtxTracker);
						MessageFormatter.debug(logger, "deleteBOMTracker", "Qtx key eligible for delete :[{0}]", qtxTracker.getQualtxKey());
					}

				}
			}
			
			deleteQtxTrackers(qtxTrackerDeleteEligibleList);
			bomTracker.deleteQtxTrackers(qtxTrackerDeleteEligibleList);
			
			if(bomTracker.getQtxTrackerList().isEmpty()) this.bomTrackerMap.remove(bomTracker.bomKey);
			
		}
	}
	
	public void deleteQtxWorkTrackers(Set<QtxWorkTracker> qtxWorkList) throws Exception
	{
		if (qtxWorkList != null && qtxWorkList.size() > 0)
		{
			for (QtxWorkTracker qtxWorkTracker : qtxWorkList)
			{
				this.qtxWorkTrackerMap.remove(qtxWorkTracker.getQualtxWorkId());
			}
		}
	}
	public void clearBOMTracker(BOMTracker bomTracker) throws Exception
	{
		if (bomTracker != null)
		{
			Set<QtxTracker> aQtxTrackerSet= bomTracker.getQtxTrackerList();
			deleteQtxTrackers(aQtxTrackerSet);
			bomTracker.deleteQtxTrackers(aQtxTrackerSet);
			if(bomTracker.getQtxTrackerList().isEmpty()) this.bomTrackerMap.remove(bomTracker.bomKey);
			
		}
	}
}
