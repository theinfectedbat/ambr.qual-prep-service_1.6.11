package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.SQLException;
import java.util.ArrayList;

import com.ambr.gtm.fta.qts.RepositoryException;

public abstract class QTXConsumer<T> implements Runnable
{
	protected QTXProducer producer;
	protected ArrayList <T> workList;
	
	public QTXConsumer(ArrayList <T> workList)
	{
		this.setWork(workList);
	}
	
	public QTXConsumer(T work)
	{
		ArrayList<T> list = new ArrayList<T>();
		
		list.add(work);
		
		this.setWork(list);
	}
	
	protected void setProducer(QTXProducer producer)
	{
		this.producer = producer;
	}
	
	private void setWork(ArrayList <T> workList)
	{
		this.workList = workList;
	}
	
	private void processWorkList() throws RepositoryException, SQLException
	{
		long start = System.currentTimeMillis();
		
		try
		{
			try
			{
				this.processWork();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		finally
		{
			if (this.workList != null)
			{
				int size = this.workList.size();
				
				if (size > 0)
					this.producer.recordStats(size, System.currentTimeMillis() - start);
			}
		}
	}
	
	protected abstract void processWork() throws Exception;
	
	public void run()
	{
		try
		{
			this.processWorkList();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
