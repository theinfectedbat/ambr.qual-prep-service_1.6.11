package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
public class ShutdownServiceAPI 
{
	public static final String		URL_PATH						= "/qps/api/preparation_engine/shutdown";
	public static final String		REQUEST_PARAM_NAME_IS_ORDERLY	= "is_orderly";
	
	@Autowired
	private PreparationEngineQueueUniverse 	bomProcessorQueueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ShutdownServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@RequestMapping(path = URL_PATH, method = RequestMethod.POST)
	public void execute
		(
			@RequestParam(name=REQUEST_PARAM_NAME_IS_ORDERLY, required=false)	String theIsOrderly
		)
		throws Exception
	{
		this.bomProcessorQueueUniverse.shutdown("Y".equalsIgnoreCase(theIsOrderly));
	}
}
