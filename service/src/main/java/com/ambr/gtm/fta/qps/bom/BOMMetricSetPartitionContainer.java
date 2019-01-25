package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMMetricSetPartitionContainer 
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	public int 							partitionNum;
	public ArrayList<BOMMetricSet>		bomMetricSetList;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMMetricSetPartitionContainer()
		throws Exception
	{
		this.bomMetricSetList = new ArrayList<>();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePartition
     *************************************************************************************
     */
	BOMMetricSetPartitionContainer(BOMUniversePartition thePartition)
		throws Exception
	{
		this();
		
		this.partitionNum = thePartition.getPartitionNum();
		
		for (BOM aBOM : thePartition.getBOMs()) {
			this.bomMetricSetList.add(new BOMMetricSet(aBOM));
		}
	}
}
