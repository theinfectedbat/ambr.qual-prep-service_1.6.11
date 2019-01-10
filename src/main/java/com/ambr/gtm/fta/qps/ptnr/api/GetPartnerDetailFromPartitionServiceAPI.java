package com.ambr.gtm.fta.qps.ptnr.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniverse;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetail;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniverse;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetPartnerDetailFromPartitionServiceAPI 
{
	public static final String	URL_PATH_VARIABLE_NAME_PTNR_KEY 	= "ptnr_key";
	public static final String	URL_PATH_VARIABLE_SPEC_PTNR_KEY 	= "{" + URL_PATH_VARIABLE_NAME_PTNR_KEY + "}";
	public static final String	URL_PATH_PREFIX						= "/qps/api/ptnrdetail_universe/get_ptnrdetail_by_key/";
	public static final String	URL_PATH_TEMPLATE					= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_PTNR_KEY;
	
	@Autowired
	private PartnerDetailUniversePartition	universePartition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetPartnerDetailFromPartitionServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePtnrKey
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_TEMPLATE)
	public PartnerDetail execute(@PathVariable(name = URL_PATH_VARIABLE_NAME_PTNR_KEY) long thePtnrKey)
		throws Exception
	{
		return this.universePartition.getPartnerDetail(thePtnrKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePtnrKey
     *************************************************************************************
     */
	public static String GetURLPath(long thePtnrKey)
		throws Exception
	{
		return URL_PATH_TEMPLATE.replace(URL_PATH_VARIABLE_SPEC_PTNR_KEY, String.valueOf(thePtnrKey));
	}
}
