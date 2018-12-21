package com.ambr.gtm.fta.qps.gpmclaimdetail;

import org.apache.commons.collections4.map.LRUMap;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMClaimDetailsCache 
{
	private GPMClaimDetailsUniverse								universe;
	private long												hits;
	private long												misses;
	private LRUMap<Long, GPMClaimDetailsSourceIVAContainer> 	claimDetailsTable;
	private LRUMap<Long, Boolean>								missingClaimDetailsTable;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 * @param	theMaxSize
	 *************************************************************************************
	 */
	public GPMClaimDetailsCache(
		GPMClaimDetailsUniverse 	theUniverse, 
		int 						theMaxSize)
		throws Exception
	{
		this.universe = theUniverse;
		this.claimDetailsTable = new LRUMap<>(theMaxSize);
		this.missingClaimDetailsTable = new LRUMap<>(theMaxSize);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdSrcIVAKey
	 *************************************************************************************
	 */
	public GPMClaimDetailsSourceIVAContainer getClaimDetails(Long theProdSrcIVAKey)
		throws Exception
	{
		GPMClaimDetailsSourceIVAContainer	aClaimDetails;
		Boolean								aMissingFlag;
		
		if (theProdSrcIVAKey == null) {
			return null;
		}
		
		synchronized (this) {
			aClaimDetails = this.claimDetailsTable.get(theProdSrcIVAKey);
			if (aClaimDetails != null) {
				this.hits++;
				return aClaimDetails;
			}

			aMissingFlag = this.missingClaimDetailsTable.get(theProdSrcIVAKey);
			if (aMissingFlag != null) {
				this.hits++;
				return null;
			}
			
			this.misses++;
		}
		
		aClaimDetails = this.universe.getClaimDetails(theProdSrcIVAKey);

		synchronized (this) {
			if (aClaimDetails == null) {
				this.missingClaimDetailsTable.put(theProdSrcIVAKey,  true);
			}
			else {
				this.claimDetailsTable.put(aClaimDetails.prodSrcIVAKey, aClaimDetails);
			}
		}
		
		return aClaimDetails;
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
		return this.claimDetailsTable.size() + this.missingClaimDetailsTable.size();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public GPMClaimDetailsUniverse getUniverse()
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
		this.claimDetailsTable.clear();
		this.missingClaimDetailsTable.clear();
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
	public GPMClaimDetailsCache setMaxSize(int theMaxSize)
		throws Exception
	{
		this.claimDetailsTable = new LRUMap<>(theMaxSize);
		return this;
	}
}
