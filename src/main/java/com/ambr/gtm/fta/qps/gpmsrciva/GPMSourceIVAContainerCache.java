package com.ambr.gtm.fta.qps.gpmsrciva;

import org.apache.commons.collections4.map.LRUMap;

import com.ambr.gtm.fta.qps.bom.BOMUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceIVAContainerCache 
{
	private long													hits;
	private long													misses;
	private LRUMap<Long, GPMSourceIVAProductSourceContainer> 		prodSrcContainerTable;
	private LRUMap<Long, GPMSourceIVAProductContainer> 				prodContainerTable;
	private GPMSourceIVAUniverse									universe;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theUniverse
     * @param	theMaxSize
     *************************************************************************************
     */
	public GPMSourceIVAContainerCache(GPMSourceIVAUniverse theUniverse, int theMaxSize)
		throws Exception
	{
		this.universe = theUniverse;
		this.prodContainerTable = new LRUMap<>(theMaxSize);
		this.prodSrcContainerTable = new LRUMap<>(theMaxSize);
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
	public int getSize()
		throws Exception
	{
		return this.prodSrcContainerTable.size();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdKey
     *************************************************************************************
     */
	public synchronized GPMSourceIVAProductContainer getSourceIVAByProduct(Long theProdKey)
		throws Exception
	{
		GPMSourceIVAProductContainer	aContainer;
		
		if (theProdKey == null) {
			return null;
		}
		
		aContainer = this.prodContainerTable.get(theProdKey);
		if (aContainer == null) {
			this.misses++;
			aContainer = this.universe.getSourceIVAByProduct(theProdKey);
			if (aContainer != null) {
				this.prodContainerTable.put(aContainer.prodKey, aContainer);
				
				aContainer.loadSourceContainers(this);
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
     * 
     * @param	theProdSrcKey
     *************************************************************************************
     */
	public synchronized GPMSourceIVAProductSourceContainer getSourceIVABySource(Long theProdSrcKey)
		throws Exception
	{
		GPMSourceIVAProductSourceContainer	aContainer = null;
		
		if (theProdSrcKey == null) {
			return null;
		}
		
		aContainer = this.prodSrcContainerTable.get(theProdSrcKey);
		
		if (aContainer == null) {
			this.misses++;
			aContainer = this.universe.getSourceIVABySource(theProdSrcKey);
			if (aContainer != null) {
				this.prodSrcContainerTable.put(aContainer.prodSrcKey, aContainer);
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
	public GPMSourceIVAUniverse getUniverse()
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
		this.prodContainerTable.clear();
		this.prodSrcContainerTable.clear();
		this.resetCacheStatistics();
		
		if (theRefreshUniverseFlag) {
			this.universe.refresh();
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
	public GPMSourceIVAContainerCache setMaxSize(int theMaxSize)
		throws Exception
	{
		this.prodSrcContainerTable = new LRUMap<>(theMaxSize);
		this.prodContainerTable = new LRUMap<>(theMaxSize);
		return this;
	}
}
