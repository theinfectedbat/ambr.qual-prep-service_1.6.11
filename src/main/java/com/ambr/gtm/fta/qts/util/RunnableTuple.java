package com.ambr.gtm.fta.qts.util;

import java.util.concurrent.Future;

public class RunnableTuple
{
	public Future<?> future;
	public Runnable runnable;
	
	public RunnableTuple(Future<?> future, Runnable runnable)
	{
		this.future = future;
		this.runnable = runnable;
	}
}
