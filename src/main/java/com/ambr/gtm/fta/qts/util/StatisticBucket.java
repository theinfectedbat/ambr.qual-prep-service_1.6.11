package com.ambr.gtm.fta.qts.util;

public class StatisticBucket
{
	public long timeBucket;
	public long throughput;
	public Statistic statistic;
	
	public StatisticBucket(Long timeBucket, Statistic initialStatistic)
	{
		this.timeBucket = timeBucket;
		this.statistic = initialStatistic;
		this.throughput = 0L;
	}
	
	public void add(long throughputCount, Statistic newStatistic)
	{
		this.throughput += throughputCount;
		this.statistic.add(newStatistic);
	}
}

