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
public class GetCacheRefreshInformationServiceAPI 
{
	public static final String		URL_PATH					= "/qps/api/preparation_engine/get_refresh_timestamp";
	
	@Autowired private PreparationEngineQueueUniverse		queueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetCacheRefreshInformationServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	@RequestMapping(URL_PATH)
	public CacheRefreshInformation execute()
		throws Exception
	{
		return this.queueUniverse.getCacheRefreshInformation();
	}
}
