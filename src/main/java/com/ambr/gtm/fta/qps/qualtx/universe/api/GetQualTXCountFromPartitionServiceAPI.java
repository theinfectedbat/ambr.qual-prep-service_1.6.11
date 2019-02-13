package com.ambr.gtm.fta.qps.qualtx.universe.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetQualTXCountFromPartitionServiceAPI 
{
	public static final String		URL_PATH	= "/qps/api/qualtxdetail_universe_partition/get_qualtxcount";
	
	@Autowired
	private QualTXDetailUniversePartition	partition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetQualTXCountFromPartitionServiceAPI()
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
	public int execute()
		throws Exception
	{
		return this.partition.getQualTXCount();
	}
}
