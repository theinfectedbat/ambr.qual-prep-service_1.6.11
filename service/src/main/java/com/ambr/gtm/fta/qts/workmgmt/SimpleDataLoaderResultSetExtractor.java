package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

public class SimpleDataLoaderResultSetExtractor<T> implements ResultSetExtractor<List<T>>
{
	private DataLoader<T> loader;
	private int maxRows = Integer.MAX_VALUE;
	
	public SimpleDataLoaderResultSetExtractor(DataLoader<T> loader)
	{
		this.loader = loader;
	}

	public SimpleDataLoaderResultSetExtractor(Class<T> classType)
	{
		this.loader = new DataLoader<T>(classType);
	}
	
	public void setMaxRows(int maxRows)
	{
		this.maxRows = maxRows;
	}

	@Override
	public List<T> extractData(ResultSet rs) throws SQLException, DataAccessException
	{
		List<T> list = new ArrayList<T>();
		
		try
		{
			int counter = 0;
			while (rs.next() && counter<this.maxRows)
			{
				T object = this.loader.getObjectFromResultSet(rs);
				
				list.add(object);
				
				counter++;
			}
		}
		catch (Exception e)
		{
			throw new SQLException("Error iterating through dataset", e);
		}
		
		return list;
	}
}
