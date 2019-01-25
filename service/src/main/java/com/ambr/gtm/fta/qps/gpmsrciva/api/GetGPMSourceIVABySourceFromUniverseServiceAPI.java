package com.ambr.gtm.fta.qps.gpmsrciva.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetGPMSourceIVABySourceFromUniverseServiceAPI 
{
	public static final String	URL_PATH_VARIABLE_NAME_PROD_SRC_KEY 	= "prod_src_key";
	public static final String	URL_PATH_VARIABLE_SPEC_PROD_SRC_KEY 	= "{" + URL_PATH_VARIABLE_NAME_PROD_SRC_KEY + "}";
	public static final String	URL_PATH_PREFIX							= "/qps/api/gpmsrciva_universe/get_gpmsrciva_by_source/";
	public static final String	URL_PATH_TEMPLATE						= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_PROD_SRC_KEY;

	@Autowired
	private GPMSourceIVAUniverse	universe;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetGPMSourceIVABySourceFromUniverseServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcKey
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_TEMPLATE)
	public GPMSourceIVAProductSourceContainer execute(@PathVariable(name = URL_PATH_VARIABLE_NAME_PROD_SRC_KEY) long theProdSrcKey)
		throws Exception
	{
		return this.universe.getSourceIVABySource(theProdSrcKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcKey
     *************************************************************************************
     */
	public static String GetURLPath(long theProdSrcKey)
		throws Exception
	{
		return URL_PATH_TEMPLATE.replace(URL_PATH_VARIABLE_SPEC_PROD_SRC_KEY, String.valueOf(theProdSrcKey));
	}
}
