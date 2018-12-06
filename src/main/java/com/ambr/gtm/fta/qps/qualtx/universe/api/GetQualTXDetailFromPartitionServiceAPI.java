package com.ambr.gtm.fta.qps.qualtx.universe.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailBOMContainer;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetQualTXDetailFromPartitionServiceAPI 
{
	public static final String		URL_PATH_VARIABLE_NAME_BOM_KEY	= "bom_key";
	public static final String		URL_PATH_VARIABLE_SPEC_BOM_KEY	= "{" + URL_PATH_VARIABLE_NAME_BOM_KEY + "}";
	public static final String		URL_PATH_PREFIX					= "/qps/api/qualtxdetail_universe_partition/get_qualtxdetail/";
	public static final String		URL_PATH_TEMPLATE				= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_BOM_KEY;
	
	@Autowired
	private QualTXDetailUniversePartition	partition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetQualTXDetailFromPartitionServiceAPI()
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
	public QualTXDetailBOMContainer execute(@PathVariable(name = URL_PATH_VARIABLE_NAME_BOM_KEY) long theBOMKey)
		throws Exception
	{
		return this.partition.getQualTXDetailByBOM(theBOMKey);
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
