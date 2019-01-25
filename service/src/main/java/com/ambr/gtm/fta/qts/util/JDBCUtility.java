package com.ambr.gtm.fta.qts.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCUtility
{
	private static Logger logger = LogManager.getLogger(JDBCUtility.class);
			
	public JDBCUtility()
	{
	}
	
	public static PreparedStatement prepareStatement(String sql, Connection connection) throws SQLException
	{
		logger.trace("Prepared [" + sql + "]");
		
		return connection.prepareStatement(sql);
	}
	
	public static void closeStatement(PreparedStatement stmt)
	{
		try
		{
			if (stmt != null) stmt.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void closeResultSet(ResultSet results) throws SQLException
	{
		if (results != null) results.close();
	}
	
	public static int[] executeBatch(PreparedStatement stmt) throws SQLException
	{
		return stmt.executeBatch();
	}
	
	public static int executeUpdate(PreparedStatement stmt) throws SQLException
	{
//		long time = System.currentTimeMillis();
		
		try
		{
			return stmt.executeUpdate();
		}
		finally
		{
//			long end = System.currentTimeMillis();
//			
//			if ((end - time) > 2000)
//			{
//				System.out.println("SQL Update Time = " + (System.currentTimeMillis() - time));
//				new Exception().printStackTrace();
//			}
		}
	}
	
	public static ResultSet executeQuery(PreparedStatement stmt) throws SQLException
	{
//		long time = System.currentTimeMillis();
		
		try
		{
			return stmt.executeQuery();
		}
		finally
		{
//			long end = System.currentTimeMillis();
//			
//			if ((end - time) > 1000)
//				System.out.println("SQL Query Time = " + (System.currentTimeMillis() - time));
		}
	}
	
	public static int executeUpdate(Connection connection, String sql, Object ... params) throws SQLException
	{
		PreparedStatement stmt = null;
		try
		{
			stmt = connection.prepareStatement(sql);
			
			if (params != null)
			{
				for (int i=0; i<params.length; i++)
					stmt.setObject(i, params[i]);
			}
			
//			long start = System.currentTimeMillis();
			int updateCount = stmt.executeUpdate();
//			long end = System.currentTimeMillis();
			
//			if ((end-start) > 1000)
//			{
//				System.out.println((end-start) + "\t" + sql.replace("\t", " ").replace("\n", " "));
//			}
			
			return updateCount;
		}
		finally
		{
			if (stmt != null) stmt.close();
		}
	}
}
