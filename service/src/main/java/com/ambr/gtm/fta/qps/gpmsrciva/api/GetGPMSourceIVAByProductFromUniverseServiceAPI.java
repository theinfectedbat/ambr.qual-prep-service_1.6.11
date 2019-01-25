package com.ambr.gtm.fta.qps.gpmsrciva.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetGPMSourceIVAByProductFromUniverseServiceAPI 
{
	public static final String	URL_PATH_VARIABLE_NAME_PROD_KEY 	= "prod_key";
	public static final String	URL_PATH_VARIABLE_SPEC_PROD_KEY 	= "{" + URL_PATH_VARIABLE_NAME_PROD_KEY + "}";
	public static final String	URL_PATH_PREFIX						= "/qps/api/gpmsrciva_universe/get_gpmsrciva_by_product/";
	public static final String	URL_PATH_TEMPLATE					= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_PROD_KEY;

	@Autowired
	private GPMSourceIVAUniverse	universe;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetGPMSourceIVAByProductFromUniverseServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdKey
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_TEMPLATE)
	public GPMSourceIVAProductContainer execute(@PathVariable(name = URL_PATH_VARIABLE_NAME_PROD_KEY) long theProdKey)
		throws Exception
	{
		return this.universe.getSourceIVAByProduct(theProdKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdKey
     *************************************************************************************
     */
	public static String GetURLPath(long theProdKey)
		throws Exception
	{
		return URL_PATH_TEMPLATE.replace(URL_PATH_VARIABLE_SPEC_PROD_KEY, String.valueOf(theProdKey));
	}
}
