package com.ambr.gtm.fta.qts.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.list.FTAListContainer;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.platform.utils.log.MessageFormatter;

public class FTAHSListCache
{

	static Logger			logger					= LogManager.getLogger(FTAHSListCache.class);
	public FTAListContainer	ftaListContainerCache	= new FTAListContainer();

	public FTAListContainer getFTAList(String hsExcep, String aCompanyCode)
	{

		MessageFormatter.trace(logger, "getFTAList", "Loading FTA HS list cache - START");
		if (ftaListContainerCache.getFTAList(hsExcep, "U", aCompanyCode) == null && ftaListContainerCache.getFTAList(hsExcep, "C", "SYSTEM") == null)
		{
			FTAListContainer ftaListContainer;
			try
			{
				ftaListContainer = Env.getSingleton().getTradeQualtxClient().loadHSFtaList(hsExcep);
				if (ftaListContainer != null && !ftaListContainer.getFtaLists().isEmpty() && ftaListContainer.getFtaLists() != null) ftaListContainerCache.getFtaLists().putAll(ftaListContainer.getFtaLists());

			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "getFTAList", e, "Loading FTA HS list cache for org [{0}]", aCompanyCode);
			}

		}
		return ftaListContainerCache;
	}

	public void flushFTAListCache()
	{
		MessageFormatter.trace(logger, "flushFTAListCache", "flushing FTA HS list cache - START");
		ftaListContainerCache.getFtaLists().clear();
		MessageFormatter.trace(logger, "flushFTAListCache", "flushing FTA HS list cache - COMPLETE");

	}

}
