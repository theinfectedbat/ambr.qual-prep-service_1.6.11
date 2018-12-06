package com.ambr.gtm.fta.qps.bom.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMMetricSet;
import com.ambr.gtm.fta.qps.bom.BOMMetricSetUniverseContainer;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetBOMPrioritizedBOMUniverseServiceAPI 
{
	public static final String		URL_PATH	= "/qps/api/bom_universe/get_prioritized_bom_universe";
	
	@Autowired
	private BOMUniverse		bomUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetBOMPrioritizedBOMUniverseServiceAPI()
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
	public BOMMetricSetUniverseContainer execute()
		throws Exception
	{
		return this.bomUniverse.getPrioritizedBOMSet();
	}
}
