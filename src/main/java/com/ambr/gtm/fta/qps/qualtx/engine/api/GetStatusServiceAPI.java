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
public class GetStatusServiceAPI 
{
	public static final String		URL_PATH								= "/qps/api/preparation_engine/get_status";
	public static final String		REQUEST_PARAM_NAME_MAX_TASKS_TO_REPORT	= "max_tasks_to_report";
	
	@Autowired
	private PreparationEngineQueueUniverse 	queueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetStatusServiceAPI()
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
			@RequestParam(name=REQUEST_PARAM_NAME_MAX_TASKS_TO_REPORT, required=false)	Integer theMaxTasksToReport
		)
		throws Exception
	{
		return this.queueUniverse.getStatus(theMaxTasksToReport);
	}
}
