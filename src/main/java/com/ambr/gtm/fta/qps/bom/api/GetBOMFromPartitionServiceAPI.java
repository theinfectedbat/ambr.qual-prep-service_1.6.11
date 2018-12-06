package com.ambr.gtm.fta.qps.bom.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMMetricSet;
import com.ambr.gtm.fta.qps.bom.BOMMetricSetUniverseContainer;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.bom.BOMUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetBOMFromPartitionServiceAPI 
{
	public static final String		URL_PATH_VARIABLE_NAME_BOM_KEY	= "bom_key";
	public static final String		URL_PATH_VARIABLE_SPEC_BOM_KEY	= "{" + URL_PATH_VARIABLE_NAME_BOM_KEY + "}";
	public static final String		URL_PATH_PREFIX					= "/qps/api/bom_universe_partition/get_bom/";
	public static final String		URL_PATH_TEMPLATE				= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_BOM_KEY;
	
	@Autowired
	private BOMUniversePartition	partition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetBOMFromPartitionServiceAPI()
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
	@RequestMapping(URL_PATH_TEMPLATE)
	public BOM execute(@PathVariable(name = URL_PATH_VARIABLE_NAME_BOM_KEY) long theBOMKey)
		throws Exception
	{
		return this.partition.getBOM(theBOMKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	public static String GetURLPath(long theBOMKey)
		throws Exception
	{
		return URL_PATH_TEMPLATE.replace(URL_PATH_VARIABLE_SPEC_BOM_KEY, String.valueOf(theBOMKey));
	}
}
