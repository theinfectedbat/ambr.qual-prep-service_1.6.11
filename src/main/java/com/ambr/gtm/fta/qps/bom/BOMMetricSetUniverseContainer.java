package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import com.ambr.gtm.fta.qps.UniverseStatusEnum;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMMetricSetUniverseContainer 
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	public int 										partitionCount;
	public int										partitionErrorCount;
	public int										bomCount;
	public UniverseStatusEnum						universeStatus;
	public ArrayList<PrioritizedBOMMetricSet>		metricSetList;
	private HashMap<Long, Integer>					priorityByBOMKeyTable;
	private boolean									priorityInitializedFlag;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMMetricSetUniverseContainer()
		throws Exception
	{
		this.partitionCount = 0;
		this.metricSetList = new ArrayList<>();
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * @param	thePartitionContainer
     *************************************************************************************
     */
	public void addPartition(BOMMetricSetPartitionContainer thePartitionContainer)
		throws Exception
	{
		this.partitionCount++;

		for (BOMMetricSet aMetricSet : thePartitionContainer.bomMetricSetList) {
			this.metricSetList.add(new PrioritizedBOMMetricSet(aMetricSet));
			this.bomCount++;
		}
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	public int getBOMPriority(long theBOMKey)
		throws Exception
	{
		Integer	aPriority = 0;
		
		if (!this.priorityInitializedFlag) {
			synchronized(this)
			{
				this.priorityByBOMKeyTable = new HashMap<>();
				
				for (PrioritizedBOMMetricSet aPrioritizedMetricSet : this.metricSetList) {
					this.priorityByBOMKeyTable.put(aPrioritizedMetricSet.metricSet.bomKey, aPrioritizedMetricSet.priority);
				}
				
				this.priorityInitializedFlag = true;
			}
		}
		
		aPriority = this.priorityByBOMKeyTable.get(theBOMKey);
		if (aPriority == null) {
			aPriority = 0;
		}
		
		return aPriority;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void prioritize()
		throws Exception
	{
		int	aPriority = 1;
		
		this.metricSetList.sort(new BottomUpSorter());
		for (PrioritizedBOMMetricSet aMetricSet : this.metricSetList) {
			aMetricSet.priority = aPriority++;
		}
	}
}
