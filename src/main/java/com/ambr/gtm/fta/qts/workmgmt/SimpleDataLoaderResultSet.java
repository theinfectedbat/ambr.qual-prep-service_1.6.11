package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

public class SimpleDataLoaderResultSet<T>
{
	private static Logger logger = LogManager.getLogger(SimpleDataLoaderResultSet.class);

	protected DataLoader<T> loader;
	
	protected PreparedStatement statement;
	protected ResultSet resultSet;
	protected JdbcTemplate template;
	
	protected T object;
	
	protected long loadCount = 0L;

	public SimpleDataLoaderResultSet(DataLoader<T> loader, JdbcTemplate template)
	{
		this.loader = loader;
		this.template = template;
	}

	public SimpleDataLoaderResultSet(Class<T> classType, JdbcTemplate template)
	{
		this(new DataLoader<T>(classType), template);
	}
	
	public void close()
	{
		try
		{
			if (this.resultSet != null)
				this.resultSet.close();
		}
		catch (Exception e)
		{
			logger.error("Failed to close result set", e);
		}
		finally
		{
			this.resultSet = null;
		}
		
		try
		{
			if (this.statement != null)
				this.statement.close();
		}
		catch (Exception e)
		{
			logger.error("Failed to close statement", e);
		}
		finally
		{
			this.statement = null;
		}
	}
	
	public void execute(String sql, Object[] values) throws SQLException
	{
		this.close();
		
		this.statement = DataSourceUtils.getConnection(this.template.getDataSource()).prepareStatement(sql);
		
		if (values != null)
		{
			for (int i=0; i<values.length; i++)
				this.statement.setObject(i+1, values[i]);
		}
		
		this.resultSet = this.statement.executeQuery();
	}
	
	public T current()
	{
		return this.object;
	}
	
	public long getLoadCount()
	{
		return this.loadCount;
	}

	public T next() throws SQLException
	{
		try
		{
			if (this.resultSet.next())
			{
				this.object = this.loader.getObjectFromResultSet(this.resultSet);
				this.loadCount++;
			}
			else
				this.object = null;
		}
		catch (Exception e)
		{
			throw new SQLException("Error iterating through dataset", e);
		}
		
		return this.object;
	}
}
