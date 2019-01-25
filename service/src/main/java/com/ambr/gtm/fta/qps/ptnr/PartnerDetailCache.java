package com.ambr.gtm.fta.qps.ptnr;

import org.apache.commons.collections4.map.LRUMap;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PartnerDetailCache 
{
	private PartnerDetailUniverse			universe;
	private long							hits;
	private long							misses;
	private LRUMap<Long, PartnerDetail> 	ptnrDetailTable;
	private LRUMap<Long, Boolean>			missingPtnrDetailTable;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 * @param	theMaxSize
	 *************************************************************************************
	 */
	public PartnerDetailCache(
		PartnerDetailUniverse 	theUniverse, 
		int						theMaxSize)
		throws Exception
	{
		this.universe = theUniverse;
		this.ptnrDetailTable = new LRUMap<>(theMaxSize);
		this.missingPtnrDetailTable = new LRUMap<>(theMaxSize);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePtnrKey
	 *************************************************************************************
	 */
	public PartnerDetail getPtnrDetails(Long thePtnrKey)
		throws Exception
	{
		PartnerDetail	aPtnrDetails;
		Boolean			aMissingFlag;
		
		if (thePtnrKey == null) {
			return null;
		}
		
		synchronized (this) {
			aPtnrDetails = this.ptnrDetailTable.get(thePtnrKey);
			if (aPtnrDetails != null) {
				this.hits++;
				return aPtnrDetails;
			}

			aMissingFlag = this.missingPtnrDetailTable.get(thePtnrKey);
			if (aMissingFlag != null) {
				this.hits++;
				return null;
			}
			
			this.misses++;
		}
		
		aPtnrDetails = this.universe.getPartnerDetail(thePtnrKey);

		synchronized (this) {
			if (aPtnrDetails == null) {
				this.missingPtnrDetailTable.put(thePtnrKey,  true);
			}
			else {
				this.ptnrDetailTable.put(aPtnrDetails.alt_key_ptnr, aPtnrDetails);
			}
		}
		
		return aPtnrDetails;
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
		return this.ptnrDetailTable.size() + this.missingPtnrDetailTable.size();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public PartnerDetailUniverse getUniverse()
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
		this.ptnrDetailTable.clear();
		this.missingPtnrDetailTable.clear();
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
	public PartnerDetailCache setMaxSize(int theMaxSize)
		throws Exception
	{
		this.ptnrDetailTable = new LRUMap<>(theMaxSize);
		return this;
	}
}
