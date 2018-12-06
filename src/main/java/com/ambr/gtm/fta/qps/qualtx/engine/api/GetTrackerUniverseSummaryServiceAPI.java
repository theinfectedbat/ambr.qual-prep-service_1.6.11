package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXUniversePreparationProgressManager;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetTrackerUniverseSummaryServiceAPI 
{
	public static final String		URL_PATH		= "/qps/api/preparation_engine/progress_tracker/get_summary";
	
	@Autowired private QualTXUniversePreparationProgressManager		trackerService;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetTrackerUniverseSummaryServiceAPI()
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
	public String execute()
		throws Exception
	{
		return this.trackerService.getTrackerUniverseSummary(null);
	}
}
