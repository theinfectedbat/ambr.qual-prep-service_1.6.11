package com.ambr.gtm.fta.qps.bom.qualstatus;

import java.io.Serializable;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Date;

import com.ambr.platform.utils.queue.TaskQueueProgressSummary;
import com.ambr.platform.utils.queue.TaskQueueThroughputUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualificationPreparationStatusDetailUtility 
	implements Serializable
{
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theStatusDetail
	 * @param	theUtil
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	public static void setDuration(
		QualificationPreparationStatusDetail 	theStatusDetail,
		TaskQueueThroughputUtility 				theUtil, 
		long 									theBOMKey)
		throws Exception
	{
		long		aDuration;
		
		theUtil.measureThroughput(5);
		theUtil.collectProgressSummary(MessageFormat.format("{0,number,#}", theBOMKey));
		
		theStatusDetail.bomSpecificEstimatedTimeRemaining = 0;
		for (TaskQueueProgressSummary aSummary : theUtil.getProgressSummaryList()) {
			aDuration = (long)((((double)aSummary.filteredMaxSubmittedPosInQueue)/aSummary.throughput));
			if (aDuration > theStatusDetail.bomSpecificEstimatedTimeRemaining) {
				theStatusDetail.bomSpecificEstimatedTimeRemaining = aDuration;
			}
		}
	}
}
