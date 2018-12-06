package com.ambr.gtm.fta.qps.gpmclass;

import org.apache.commons.collections4.map.LRUMap;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMClassificationProductContainerCache 
{
	private GPMClassificationUniverse							universe;
	private long												hits;
	private long												misses;
	private LRUMap<Long, GPMClassificationProductContainer> 	containerTable;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 * @param	theMaxSize
	 *************************************************************************************
	 */
	public GPMClassificationProductContainerCache(
		GPMClassificationUniverse 	theUniverse, 
		int 						theMaxSize)
		throws Exception
	{
		this.universe = theUniverse;
		this.containerTable = new LRUMap<>(theMaxSize);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdKey
	 *************************************************************************************
	 */
	public synchronized GPMClassificationProductContainer getGPMClassificationsByProduct(long theProdKey)
		throws Exception
	{
		GPMClassificationProductContainer	aContainer;
		
		aContainer = this.containerTable.get(theProdKey);
		if (aContainer == null) {
			this.misses++;
			aContainer = this.universe.getGPMClassificationsByProduct(theProdKey);
			if (aContainer != null) {
				this.containerTable.put(aContainer.prodKey, aContainer);
			}
		}
		else {
			this.hits++;
		}
		
		return aContainer;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public double getHitRatio()
		throws Exception
	{
		if ((this.hits + this.misses) == 0) {
			return 0;
		}

		return ((double)this.hits) / ((double)(this.hits + this.misses));
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public long getHits()
		throws Exception
	{
		return this.hits;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public long getMisses()
		throws Exception
	{
		return this.misses;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public Object getSize()
		throws Exception
	{
		return this.containerTable.size();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public GPMClassificationUniverse getUniverse()
		throws Exception
	{
		return this.universe;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theRefreshUniverseFlag
	 *************************************************************************************
	 */
	public void refresh(boolean theRefreshUniverseFlag)
		throws Exception
	{
		this.containerTable.clear();
		this.resetCacheStatistics();
		
		if (theRefreshUniverseFlag) {
			this.universe.ensureAvailable();
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void resetCacheStatistics()
		throws Exception
	{
		this.hits = 0;
		this.misses = 0;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxSize
	 *************************************************************************************
	 */
	public GPMClassificationProductContainerCache setMaxSize(int theMaxSize)
		throws Exception
	{
		this.containerTable = new LRUMap<>(theMaxSize);
		return this;
	}
}
