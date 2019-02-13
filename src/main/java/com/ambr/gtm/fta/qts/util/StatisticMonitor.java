package com.ambr.gtm.fta.qts.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StatisticMonitor
{
	private static final Logger logger = LogManager.getLogger(StatisticMonitor.class);
	
	private String name;
	private int historySize;
	private long windowSize;
	
	private List<StatisticBucket> buckets = new ArrayList<StatisticBucket>();

	public StatisticMonitor(String name, int historySize, long windowSize)
	{
		this.name = name;
		this.historySize = historySize;
		this.windowSize = windowSize;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public int getHistorySize()
	{
		return this.historySize;
	}
	
	public long getWindowSize()
	{
		return this.windowSize;
	}
	
	public synchronized void addStatistic(long throughputCount, Statistic statistic)
	{
		StatisticBucket lastBucket = null;
		int lastIndex = buckets.size() - 1;
		long timeBucket = System.currentTimeMillis() / this.windowSize;

		if (lastIndex >= 0)
			lastBucket = buckets.get(lastIndex);
		
		if (lastBucket == null || lastBucket.timeBucket != timeBucket)
		{
			lastBucket = new StatisticBucket(timeBucket, statistic.create());
			
			this.buckets.add(lastBucket);
			if (this.buckets.size() > this.historySize)
				this.buckets.remove(0);
		}
		
		lastBucket.add(throughputCount, statistic);
	}
	
	public synchronized List<StatisticBucket> getHistory()
	{
		ArrayList<StatisticBucket> copy = new ArrayList<StatisticBucket>();
		
		copy.addAll(this.buckets);
		
		return copy;
	}
	
	public double getThroughput(long perUnitTime)
	{
		List<StatisticBucket> stats = this.getHistory();
		double throughput = 0;
		long minWindow = Long.MAX_VALUE;
		long maxWindow = 0;
		
		if (stats.size() == 0)
			return 0.0;
		
		for (StatisticBucket stat : stats)
		{
			if (stat.timeBucket < minWindow) minWindow = stat.timeBucket;
			if (stat.timeBucket > maxWindow) maxWindow = stat.timeBucket;
			throughput = throughput + stat.throughput;
		}
		
		logger.debug("TP: throughput total " + throughput + " size " + stats.size());

		throughput = (throughput / ((maxWindow - minWindow + 1) * this.windowSize)) * perUnitTime;
		
		logger.debug("TP: throughput result " + throughput + " per unit time " + perUnitTime);

		return throughput;
	}
}
