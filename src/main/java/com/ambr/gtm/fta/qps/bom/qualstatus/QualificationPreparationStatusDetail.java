package com.ambr.gtm.fta.qps.bom.qualstatus;

import java.time.Duration;
import java.util.Date;

public class QualificationPreparationStatusDetail 
{
	public QualificationPreparationStatusEnum 	status;
	public Date									startTime;
	public Date									endTime;
	public Duration								overallEstimatedTimeRemaining;
	public Duration								bomSpecificEstimatedTimeRemaining;
	public Date									nextScheduledExecutionTime;
	public Duration								timeUntilNextExecution;
}
