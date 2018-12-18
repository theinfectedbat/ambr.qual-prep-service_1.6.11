package com.ambr.gtm.fta.qps.bom;

import java.util.Comparator;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BottomUpSorter
	implements Comparator<PrioritizedBOMMetricSet>
{
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM1
     * @param	theBOM2
     *************************************************************************************
     */
	@Override
	public int compare(PrioritizedBOMMetricSet theBOM1, PrioritizedBOMMetricSet theBOM2) 
	{
		int		aDiff;

		try {
			int aPriority1 = (theBOM1.metricSet.priority == null)? 0 : theBOM1.metricSet.priority;
			int aPriority2 = (theBOM2.metricSet.priority == null)? 0 : theBOM2.metricSet.priority;

			// Prioritize higher values over lower values
			aDiff = aPriority2 - aPriority1;
			
			if (aDiff == 0) {
			
				// prioritize "shallow" BOMs over deeply "nested" BOMs
				aDiff = theBOM1.metricSet.depth - theBOM2.metricSet.depth;
				
				if (aDiff == 0) {
					// prioritize BOMs that are referenced more, over BOMs that are referenced less
					aDiff = theBOM2.metricSet.refCnt - theBOM1.metricSet.refCnt;
					if (aDiff == 0) {
						// prioritize smaller BOMs over larger BOMs
						aDiff = theBOM1.metricSet.cmpCnt - theBOM2.metricSet.cmpCnt;
					}
				}
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return aDiff;
	}
}
