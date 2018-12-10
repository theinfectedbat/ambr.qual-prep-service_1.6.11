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
	
}
