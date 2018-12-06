package com.ambr.gtm.fta.qps.gpmclaimdetail.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniversePartition;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetGPMClaimDetailsFromPartitionServiceAPI 
{
	public static final String	URL_PATH_VARIABLE_NAME_PROD_SRC_IVA_KEY 	= "prod_src_iva_key";
	public static final String	URL_PATH_VARIABLE_SPEC_PROD_SRC_IVA_KEY 	= "{" + URL_PATH_VARIABLE_NAME_PROD_SRC_IVA_KEY + "}";
	public static final String	URL_PATH_PREFIX								= "/qps/api/gpmclaimdetails_universe_partition/get_gpmclaimdetails/";
	public static final String	URL_PATH_TEMPLATE							= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_PROD_SRC_IVA_KEY;
	
	@Autowired
	private GPMClaimDetailsUniversePartition	partition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetGPMClaimDetailsFromPartitionServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcIVAKey
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_TEMPLATE)
	public GPMClaimDetails execute(@PathVariable(name = URL_PATH_VARIABLE_NAME_PROD_SRC_IVA_KEY) long theProdSrcIVAKey)
		throws Exception
	{
		return this.partition.getClaimDetails(theProdSrcIVAKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcIVAKey
     *************************************************************************************
     */
	public static String GetURLPath(long theProdSrcIVAKey)
		throws Exception
	{
		return URL_PATH_TEMPLATE.replace(URL_PATH_VARIABLE_SPEC_PROD_SRC_IVA_KEY, String.valueOf(theProdSrcIVAKey));
	}
}
