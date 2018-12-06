package com.ambr.gtm.fta.qts.config;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.gtm.fta.qts.TrackerLoaderQueries;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.trade.FTACountryContainer;
import com.ambr.gtm.fta.trade.FTAParticipatingCountrySet;
import com.ambr.gtm.fta.trade.OrgFTACountryData;
import com.ambr.gtm.utils.legacy.multiorg.Org;
import com.ambr.gtm.utils.legacy.multiorg.OrgCache;
import com.ambr.platform.utils.log.MessageFormatter;

public class FTACtryConfigCache
{
	static Logger				logger			= LogManager.getLogger(FTACtryConfigCache.class);
	private OrgFTACountryData	orgFTACountryData;
	private Boolean				loadUsingEndPoint;
	private JdbcTemplate		theJdbcTemplate;
	private int					fetchSize;
	private OrgCache			orgCache;
	private Set<String>			reloadOrgList	= new HashSet<String>();

	public FTACtryConfigCache(OrgCache orgCache, JdbcTemplate aJdbcTemplate, int fetchSize, Boolean loadUsingEndPoint) throws Exception
	{
		this.theJdbcTemplate = aJdbcTemplate;
		this.fetchSize = fetchSize;
		this.orgCache = orgCache;
		this.loadUsingEndPoint = loadUsingEndPoint;
	}
	
	public FTACtryConfigCache() {
		
	}
	
	public void load() throws Exception
	{
		if (loadUsingEndPoint)
		{
			loadFtaCtryDataGroupFromTA();
		}
		else
		{
			orgFTACountryData = new OrgFTACountryData();
			loadDataGroupFromDB();
		}
	}
	
	
	public synchronized FTACountryContainer getFTACountryContainer(String orgCode) throws Exception
	{
		FTACountryContainer ftaCountryContainer = this.orgFTACountryData.getFTACountryContainer(orgCode);
		if (ftaCountryContainer == null)
		{
			if (reloadOrgList.contains(orgCode))
			{
				if (loadUsingEndPoint) loadFTACtryDataGroupFromTAForOrg(orgCode);
				else loadDataGroupFromDBForOrg(orgCode);
				
				reloadOrgList.remove(orgCode);
				ftaCountryContainer = this.orgFTACountryData.getFTACountryContainer(orgCode);
				if (ftaCountryContainer != null) return ftaCountryContainer;
			}
			
			Org org = this.orgCache.getOrg(orgCode);
			if (!"SYSTEM".equals(orgCode))
			{
				if (org == null || org.isCompany()) return getFTACountryContainer("SYSTEM");
				else return getFTACountryContainer(org.getParentOrgCode());
			}
		}
		return ftaCountryContainer;
	}
	
	private void loadFtaCtryDataGroupFromTA() throws Exception
	{
		MessageFormatter.trace(logger, "loadFtaCtryDataGroupFromTA", "Loading fta_ctry datagroup from TA for all orgs - START");
		try
		{
			orgFTACountryData = Env.getSingleton().getTradeQualtxClient().loadFtaCtryDatagroup();
			MessageFormatter.info(logger, "loadFtaCtryDataGroupFromTA", "Loading fta_ctry datagroup for all orgs using endpoint  - COMPLETE");
		}
		catch (Exception exec)
		{
			MessageFormatter.error(logger, "loadFtaCtryDataGroupFromTA", exec, "Error while loading the fta_ctry data group from TA for all orgs");
		}

	}

	private void loadFTACtryDataGroupFromTAForOrg(String theOrgCode) throws Exception
	{
		MessageFormatter.trace(logger, "loadFTACtryDataGroupFromTAForOrg", "Started loading fta_ctry datagroup from TA [{0}] for org code ", theOrgCode);

		try
		{
			FTACountryContainer ftaCtryContainer = Env.getSingleton().getTradeQualtxClient().loadFtaCtryDatagroupPerOrg(theOrgCode);
			if (ftaCtryContainer != null)
			{
				orgFTACountryData.setFTACountryContainer(theOrgCode, ftaCtryContainer);
			}
			else
			{
				orgFTACountryData.removeFTACountryContainerver(theOrgCode);
			}
			MessageFormatter.trace(logger, "loadFTACtryDataGroupFromTAForOrg", "Complete loading fta_ctry datagroup from TA [{0}] for org code ", theOrgCode);
		}
		catch (Exception exec)
		{
			MessageFormatter.error(logger, "loadFTACtryDataGroupFromTAForOrg", exec, "Org Code [{0}]: Error while loading the ftaC_ctry data group from TA for the org code ", theOrgCode);
		}
	}
	

	public synchronized void flushCache(String orgCode) throws Exception
	{
		if (orgCode != null)
		{
			orgFTACountryData.getOrgFtaCtryData().remove(orgCode);
			reloadOrgList.add(orgCode);
		}
		
	}
	private void loadDataGroupFromDB() throws Exception
	{
		MessageFormatter.trace(logger, "loadDataGroupFromDB", "Loading fta ctry data group from DB for all orgs START");
		try
		{
			theJdbcTemplate.setFetchSize(fetchSize);

			theJdbcTemplate.query(TrackerLoaderQueries.ftaCtryDataGroupLoadQuery, new DataGroupRowCallbackHandler());

		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException)) { throw e; }
		}
		MessageFormatter.trace(logger, "loadDataGroupFromDB", "Loading fta ctry data group from DB for all orgs COMPLETE");
	}
	
	private void loadDataGroupFromDBForOrg(String aOrgCode) throws Exception
	{
		MessageFormatter.trace(logger, "loadDataGroupFromDBForOrg", "Loading fta ctry data group from DB for the org code [{0}] START ", aOrgCode);

		try
		{
			theJdbcTemplate.setFetchSize(fetchSize);

			theJdbcTemplate.query(TrackerLoaderQueries.ftaCtryDataGroupLoadQueryForOrg, new PreparedStatementSetter()
			{
				public void setValues(PreparedStatement ps) throws SQLException
				{
					ps.setString(1, aOrgCode);
				}
			}, new DataGroupRowCallbackHandler());

		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException)) { throw e; }
		}

		MessageFormatter.trace(logger, "loadDataGroupFromDBForOrg", "Loading fta ctry data group from DB for the org code [{0}] COMPLETE ", aOrgCode);
	}
	
	class DataGroupRowCallbackHandler implements RowCallbackHandler
	{
		/**
		 ************************************************************************************* <P>
		 * </P>
		 * 
		 * @param theResultSet
		 */
		@Override
		public void processRow(ResultSet theResultSet) throws SQLException
		{

			String groupValue = theResultSet.getString("GROUP_VALUE");
			String groupType = theResultSet.getString("GROUP_TYPE");
			String orgCode = theResultSet.getString("ORG_CODE");

			try
			{
				if (orgFTACountryData.getFTACountryContainer(orgCode) == null)
				{
					FTACountryContainer ftaCountryContainer = new FTACountryContainer();
					Map<String, FTAParticipatingCountrySet> ftaParticipatingCtryMap = new ConcurrentHashMap<String, FTAParticipatingCountrySet>();
					FTAParticipatingCountrySet ftaParticipatingCountrySet = new FTAParticipatingCountrySet();
					Set<String> CountryList = new HashSet<String>();
					CountryList.add(groupValue);
					ftaParticipatingCountrySet.setCountryList(CountryList);
					ftaParticipatingCtryMap.put(groupType, ftaParticipatingCountrySet);
					ftaCountryContainer.setFtaParticipatingCtryMap(ftaParticipatingCtryMap);
					Map<String, FTACountryContainer> orgFtaCtryData = new ConcurrentHashMap<String, FTACountryContainer>();
					orgFtaCtryData.put(orgCode, ftaCountryContainer);
					orgFTACountryData.setOrgFtaCtryData(orgFtaCtryData);

				}

				else
				{

					FTACountryContainer ftaCtrycontainer = orgFTACountryData.getFTACountryContainer(orgCode);
					if (ftaCtrycontainer.getFtaParticipatingCtryMap().get(groupType) == null)
					{

						FTAParticipatingCountrySet ftaParticipatingCountrySet = new FTAParticipatingCountrySet();
						Set<String> CountryList = new HashSet<String>();
						CountryList.add(groupValue);
						ftaParticipatingCountrySet.setCountryList(CountryList);
						Map<String, FTAParticipatingCountrySet> ftaParticipatingCtryMap = ftaCtrycontainer.getFtaParticipatingCtryMap();

						ftaParticipatingCtryMap.put(groupType, ftaParticipatingCountrySet);
						ftaCtrycontainer.setFtaParticipatingCtryMap(ftaParticipatingCtryMap);
						Map<String, FTACountryContainer> orgFtaCtryData = orgFTACountryData.getOrgFtaCtryData();
						orgFtaCtryData.put(orgCode, ftaCtrycontainer);
						orgFTACountryData.setOrgFtaCtryData(orgFtaCtryData);

					}

					else
					{
						FTAParticipatingCountrySet ftaParticipatingCountrySet = ftaCtrycontainer.getParticipatingCtrySet(groupType);
						Set<String> CountryList = ftaParticipatingCountrySet.getCountryList();
						CountryList.add(groupValue);
						ftaParticipatingCountrySet.setCountryList(CountryList);
						Map<String, FTAParticipatingCountrySet> ftaParticipatingCtryMap = ftaCtrycontainer.getFtaParticipatingCtryMap();

						ftaParticipatingCtryMap.put(groupType, ftaParticipatingCountrySet);
						ftaCtrycontainer.setFtaParticipatingCtryMap(ftaParticipatingCtryMap);
						Map<String, FTACountryContainer> orgFtaCtryData = orgFTACountryData.getOrgFtaCtryData();
						orgFtaCtryData.put(orgCode, ftaCtrycontainer);
						orgFTACountryData.setOrgFtaCtryData(orgFtaCtryData);

					}

				}
			}
			catch (Exception exec)
			{

				MessageFormatter.error(logger, "DataGroupRowCallbackHandler", exec, "Error while loading the data group ");
			}

		}
	}
	
	void loadORGCache(String orgCode) throws Exception
	{
		MessageFormatter.trace(logger, "loadORGCache", "Loading FTA Ctry data group cach for Org Code [{0}]  START", orgCode);
		try
		{
			theJdbcTemplate.setFetchSize(fetchSize);

			theJdbcTemplate.query(TrackerLoaderQueries.ftaCtryDataGroupLoadQueryForOrg, new PreparedStatementSetter()
			{
				public void setValues(PreparedStatement ps) throws SQLException
				{
					ps.setString(1, orgCode);
				}
			}, new DataGroupRowCallbackHandler());

		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException))
			{
				MessageFormatter.error(logger, "loadORGCache", e, "Exception while Loading FTA Ctry data group cach for the Org Code [{0}]", orgCode);
				throw e;
			}
		}
		MessageFormatter.trace(logger, "loadORGCache", "Loading FTA Ctry data group cach for Org Code [{0}]  COMPLETE", orgCode);
	}

}
