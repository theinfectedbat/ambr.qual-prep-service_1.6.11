package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngine;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GenerateUniverseServiceAPI 
{
	public static final String		URL_PATH_GENERATE_UNIVERSE				= "/qps/api/preparation_engine/generate_universe";
	public static final String		URL_PATH_GENERATE_UNIVERSE_WITH_OPTIONS	= "/qps/api/preparation_engine/generate_universe_with_options";
	
	@Autowired
	private PreparationEngine		prepEngine;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GenerateUniverseServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_GENERATE_UNIVERSE)
	public void generate()
		throws Exception
	{
		this.prepEngine.generateUniverse();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theRequest
	 * @return 
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_GENERATE_UNIVERSE_WITH_OPTIONS)
	public void generateWithOptions
		(
			@RequestBody(required = true) QualTXUniverseGenerationRequest theRequest
		)
		throws Exception
	{
		this.prepEngine.generateUniverse(theRequest);
	}
}
