package com.ambr.gtm.fta.qts.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qts.config.QEConfigCache;


public class Utility 
{
	static Logger		logger = LogManager.getLogger(Utility.class);
	private static Map<String,List<String>> flexFieldManager  = new HashMap<>();
	
	public static String prepareIVAsql(QEConfigCache qeConfigCache, String orgCode) throws Exception
	{

		String query = "(mdi_prod_src_iva.fta_code,mdi_prod_src_iva.ctry_of_import) IN (";
		String finalIVAquery = "";
		List<TradeLane> tradeLaneList = qeConfigCache.getQEConfig(orgCode).getTradeLaneList();
		if (tradeLaneList != null)
		{
			for (TradeLane tradeLane : tradeLaneList)
			{
				finalIVAquery = finalIVAquery + " OR " + query + "('" + tradeLane.getFtaCode() + "'" + "," + "'" + tradeLane.getCtryOfImport() + "'))";
			}
		}

		finalIVAquery = finalIVAquery.concat(")");
		finalIVAquery = finalIVAquery.replaceFirst(" OR ", " AND (");

		return finalIVAquery;
	}

	public static String prepareCtryCmplsql(Map<String, List<String>> ftaCoiMap) {

		String query = "mdi_prod_ctry_cmpl.ctry_code =";
		String finalctrycmplquery = "";
		for (String ftaCode : ftaCoiMap.keySet()) {
			List<String> ctryList = ftaCoiMap.get(ftaCode);
			for (String ctryCode : ctryList) {
				finalctrycmplquery = finalctrycmplquery + " OR " + query + "'" + ctryCode + "'";
			}
		}

		finalctrycmplquery = finalctrycmplquery.concat(")");
		finalctrycmplquery = finalctrycmplquery.replaceFirst("OR", "AND (");

		return finalctrycmplquery;
	}
	public static List<String> getFlexFieldMappingColumn(String orgCode, String ftaCodeGroup)
	 throws Exception
	{
		String key = orgCode+"##"+ftaCodeGroup;
		List<String> flexColumnList = flexFieldManager.get(key);
		if(flexColumnList == null)
		{
			key = "SYSTEM##"+ftaCodeGroup;
			flexColumnList = flexFieldManager.get(key);
			if(flexColumnList == null)
			{
				synchronized(Utility.class)
				{
					Connection conn = null;
					PreparedStatement stmt = null;
					ResultSet resultSet = null;
					try {
						conn = Env.getSingleton().getPoolConnection();
						stmt = JDBCUtility.prepareStatement("select ORG_CODE, field_column from mdi_flex_config_field where org_code in (?,?) and flex_name = ? and config_name = ?", conn);
						stmt.setString(1,"SYSTEM");
						stmt.setString(2,orgCode);
						stmt.setString(3,"STP");
						stmt.setString(4,ftaCodeGroup);
						resultSet = stmt.executeQuery();
						while(resultSet.next()) 
						{
							String queryResult_orgCode = resultSet.getString("ORG_CODE");
							List<String> flexColumns = flexFieldManager.get(queryResult_orgCode+"##"+ftaCodeGroup);
							if(flexColumns == null)
							{
								flexColumns = new ArrayList<>();
								flexFieldManager.put(queryResult_orgCode+"##"+ftaCodeGroup,flexColumns);
							}
							flexColumns.add("FLEXFIELD_"+resultSet.getString("FIELD_COLUMN"));
						}
					}
					catch(Exception exe) {
						exe.printStackTrace();
					}
					finally {
						JDBCUtility.closeResultSet(resultSet);
						JDBCUtility.closeStatement(stmt);
						Env.getSingleton().releasePoolConnection(conn);
					}
				}
			}
		}
		key = orgCode+"##"+ftaCodeGroup;
		flexColumnList = flexFieldManager.get(key);
		if(flexColumnList == null)
		{
			key = "SYSTEM##"+ftaCodeGroup;
			flexColumnList = flexFieldManager.get(key);
		}
		return flexColumnList;
	}
}
