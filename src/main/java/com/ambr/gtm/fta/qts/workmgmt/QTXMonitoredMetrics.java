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
	
	public synchronized void increment()
	{
		long time = System.currentTimeMillis();
		
		if (this.firstItemAdded == 0) this.firstItemAdded = time;
		
		this.lastItemAdded = time;
		this.added++;
	}
	
	public synchronized void add(long itemCount, long duration)
	{
		long time = System.currentTimeMillis();
		
		if (this.firstItemCompleted == 0) this.firstItemCompleted = time;
		this.lastItemCompleted = time;
		
		this.completed = this.completed + itemCount;
		this.aggregatedDuration = this.aggregatedDuration + duration;
	}
}
