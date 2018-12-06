package com.ambr.gtm.fta.qps.bom.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.BOMMetricSetPartitionContainer;
import com.ambr.gtm.fta.qps.bom.BOMUniversePartition;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetAllBOMMetricsFromPartitionServiceAPI 
{
	public static final String		URL_PATH 	= "/qps/api/bom_universe_partition/get_all_bom_metrics";
	
	@Autowired
	private BOMUniversePartition			bomUniversePartition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetAllBOMMetricsFromPartitionServiceAPI()
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
	public BOMMetricSetPartitionContainer getPrioritizedBOMSet()
		throws Exception
	{
		return this.bomUniversePartition.getAllBOMMetrics();
	}
}
