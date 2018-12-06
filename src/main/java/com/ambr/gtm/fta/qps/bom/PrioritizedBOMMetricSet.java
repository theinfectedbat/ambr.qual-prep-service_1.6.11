package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PrioritizedBOMMetricSet
	implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public int 				priority;
	public BOMMetricSet		metricSet;

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theMetricSet
     *************************************************************************************
     */
	PrioritizedBOMMetricSet(BOMMetricSet theMetricSet)
		throws Exception
	{
		this.metricSet = theMetricSet;
		this.priority = 0;
	}
}
