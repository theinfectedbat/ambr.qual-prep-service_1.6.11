package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class ResetCacheStatisticsServiceAPI 
{
	public static final String		URL_PATH	= "/qps/api/preparation_engine/reset_cache_statistics";
	
	@Autowired
	private PreparationEngineQueueUniverse 	bomProcessorQueueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ResetCacheStatisticsServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@RequestMapping(URL_PATH)
	public void execute()
		throws Exception
	{
		this.bomProcessorQueueUniverse.resetCacheStatistics();
	}
}
