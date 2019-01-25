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
public class QualificationPreparationStatusDetail 
	implements Serializable
{
	public String 	statusText;
	public Date		startTime;
	public Date		endTime;
	public long		overallEstimatedTimeRemaining;
	public long		bomSpecificEstimatedTimeRemaining;
	public Date		nextScheduledExecutionTime;
	public long		timeUntilNextExecution;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUtil
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	void setDuration(TaskQueueThroughputUtility theUtil, long theBOMKey)
		throws Exception
	{
		long		aDuration;
		
		theUtil.measureThroughput(5);
		theUtil.collectProgressSummary(MessageFormat.format("{0,number,#}", theBOMKey));
		
		this.bomSpecificEstimatedTimeRemaining = 0;
		for (TaskQueueProgressSummary aSummary : theUtil.getProgressSummaryList()) {
			aDuration = (long)((((double)aSummary.filteredMaxSubmittedPosInQueue)/aSummary.throughput));
			if (aDuration > this.bomSpecificEstimatedTimeRemaining) {
				this.bomSpecificEstimatedTimeRemaining = aDuration;
			}
		}
	}
}
