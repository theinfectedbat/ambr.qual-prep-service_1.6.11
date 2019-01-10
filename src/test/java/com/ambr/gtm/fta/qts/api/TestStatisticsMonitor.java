package com.ambr.gtm.fta.qts.api;

import org.junit.Test;

import com.ambr.gtm.fta.qts.util.*;

public class TestStatisticsMonitor
{
	public TestStatisticsMonitor()
	{
	}
	
	@Test
	public void test()
	{
		StatisticMonitor statsMonitor = new StatisticMonitor("TEST", 60, 750);
		
		for (int x=0; x<10; x++)
		{
			statsMonitor.addStatistic(5000, new LongStatistic(5000));
			try {Thread.sleep(1000);} catch (Exception e){e.printStackTrace();}
		}
		
		double throughput = statsMonitor.getThroughput(1000);
		
		System.out.println(throughput);
	}
}
