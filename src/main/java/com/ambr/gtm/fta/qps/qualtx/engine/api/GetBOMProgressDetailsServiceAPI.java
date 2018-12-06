package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXUniversePreparationProgressManager;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetBOMProgressDetailsServiceAPI 
{
	public static final String		URL_PATH					= "/qps/api/preparation_engine/progress_tracker/get_bom_progress_details";
	public static final String		REQUEST_PARAM_NAME_BOM_KEY	= "bom_key";
	
	@Autowired private QualTXUniversePreparationProgressManager		trackerService;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetBOMProgressDetailsServiceAPI()
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
	public String execute
		(
			@RequestParam(name=REQUEST_PARAM_NAME_BOM_KEY, required=true)	Long theBOMKey
		)
		throws Exception
	{
		return this.trackerService.getBOMStatus(theBOMKey);
	}
}
