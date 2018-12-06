package com.ambr.gtm.fta.qts.util;

public class StatisticMonitor
{
	private String name;
	private int windowSize;
	
	private long totalCount = 0L;
	private long totalValue = 0L;
	
	private long lastCount = 0L;
	private long lastValue = 0L;
	
	private long countSet[];
	private long valueSet[];
	
	public StatisticMonitor(String name, int windowSize)
	{
		this.name = name;
		this.windowSize = windowSize;
		
		this.countSet = new long[this.windowSize];
		this.valueSet = new long[this.windowSize];
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public int getWindowSize()
	{
		return this.windowSize;
	}
	
	public synchronized void addStatistic(long count, long value)
	{
		this.totalCount = this.totalCount + count;
		this.totalValue = this.totalValue + value;
		
		this.countSet[this.windowSize - 1] += count;
		this.valueSet[this.windowSize - 1] += value;
	}
	
	public synchronized void shiftWindow()
	{
		this.lastCount = 0;
		this.lastValue = 0;
		
		for (int i=0; i<(this.windowSize-1); i++)
		{
			this.lastCount = this.lastCount + this.countSet[i];
			this.lastValue = this.lastValue + this.valueSet[i];
			
			this.countSet[i] = this.countSet[i + 1];
			this.valueSet[i] = this.valueSet[i + 1];
		}
		
		this.countSet[this.windowSize - 1] = 0;
		this.valueSet[this.windowSize - 1] = 0;
	}
}
