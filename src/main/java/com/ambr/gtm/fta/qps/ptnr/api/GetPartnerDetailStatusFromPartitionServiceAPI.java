package com.ambr.gtm.fta.qps.ptnr.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetPartnerDetailStatusFromPartitionServiceAPI 
{
	public static final String		URL_PATH_VARIABLE_NAME_PADDING_LENGTH	= "padding_length";
	public static final String		URL_PATH_VARIABLE_SPEC_PADDING_LENGTH	= "{" + URL_PATH_VARIABLE_NAME_PADDING_LENGTH + "}";
	public static final String		URL_PATH_PREFIX							= "/qps/api/ptnrdetail_universe_partition/get_status/";
	public static final String		URL_PATH_TEMPLATE						= URL_PATH_PREFIX + URL_PATH_VARIABLE_SPEC_PADDING_LENGTH;
	
	@Autowired
	private PartnerDetailUniversePartition	universe;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetPartnerDetailStatusFromPartitionServiceAPI()
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
	public static String GetURLPath(int thePaddingLength)
		throws Exception
	{
		return URL_PATH_TEMPLATE.replace(URL_PATH_VARIABLE_SPEC_PADDING_LENGTH, String.valueOf(thePaddingLength));
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@RequestMapping(URL_PATH_TEMPLATE)
	public String execute
		(
			@PathVariable(name = URL_PATH_VARIABLE_NAME_PADDING_LENGTH, required = true) int thePaddingLength
		)
		throws Exception
	{
		return this.universe.getStatus(thePaddingLength);
	}
}
