package com.ambr.gtm.fta.qts.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class OrgUtility
{
	public OrgUtility()
	{
	}
	
	public static ArrayList<String> getOrgCodes(Connection connection) throws Exception
	{
		ArrayList<String> orgCodes = new ArrayList<String>();
		PreparedStatement stmt = null;
		
		try
		{
			stmt = JDBCUtility.prepareStatement("select org_code from mdi_org where org_code<>'SYSTEM'", connection);
			
			ResultSet results = null;
			try
			{
				results = JDBCUtility.executeQuery(stmt);
				while (results.next())
					orgCodes.add(results.getString(1));
				
			}
			finally
			{
				JDBCUtility.closeResultSet(results);
			}
		}
		finally
		{
			JDBCUtility.closeStatement(stmt);
		}

		return orgCodes;
	}
}
