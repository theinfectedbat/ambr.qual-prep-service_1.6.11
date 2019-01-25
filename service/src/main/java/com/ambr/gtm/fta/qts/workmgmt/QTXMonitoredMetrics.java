package com.ambr.gtm.fta.qts.workmgmt;

import java.util.List;

import com.ambr.gtm.fta.qts.util.StatisticBucket;

public class QTXMonitoredMetrics
{
	public String name;
	
	public long added;
	public long completed;
	public long aggregatedDuration;
	
	public long firstItemAdded;
	public long lastItemAdded;
	public long firstItemCompleted;
	public long lastItemCompleted;
	public long monitorStart;
	public long monitorEnd;
	
	List<StatisticBucket> throughputMetrics;
	
	public QTXMonitoredMetrics(String name)
	{
		this.name = name;
	}
	
	public void start()
	{
		this.monitorStart = System.currentTimeMillis();
	}
	
	public void stop()
	{
		this.monitorEnd = System.currentTimeMillis();
	}
}
