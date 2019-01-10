package com.ambr.gtm.fta.qts.util;

public interface Statistic
{
	public abstract void add(Statistic operand);
	public abstract Statistic create();
}
