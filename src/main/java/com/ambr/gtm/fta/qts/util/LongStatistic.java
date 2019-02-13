package com.ambr.gtm.fta.qts.util;

public class LongStatistic implements Statistic
{
	public long value;
	
	public LongStatistic()
	{
	}

	public LongStatistic(long initialValue)
	{
		this.value = initialValue;
	}

	@Override
	public void add(Statistic operand)
	{
		this.value = this.value + ((LongStatistic) operand).value;
	}

	@Override
	public Statistic create()
	{
		return new LongStatistic();
	}
}
