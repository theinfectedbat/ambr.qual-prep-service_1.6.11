package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetPerformanceStatusServiceAPI 
{
	public static final String		URL_PATH									= "/qps/api/prep_engine_universe/get_performance_status";
	public static final String		REQUEST_PARAM_NAME_MEASUREMENT_PERIOD		= "measurement_period";

	@Autowired
	private PreparationEngineQueueUniverse 	bomProcessorQueueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetPerformanceStatusServiceAPI()
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
	public String execute
		(
			@RequestParam(name=REQUEST_PARAM_NAME_MEASUREMENT_PERIOD, required=true) Integer theMeasurementPeriod
		)
		throws Exception
	{
		return this.bomProcessorQueueUniverse.getPerformanceStatus(theMeasurementPeriod);
	}
}
