package com.ambr.gtm.fta.qps.bom.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.BOMUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class BOMUniverseRefreshAPI 
{
	public static final String		URL_PATH	= "/qps/api/bom_universe/refresh";
	
	@Autowired
	private BOMUniverse bomUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMUniverseRefreshAPI()
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
	public void bomUniverseRefresh
		(
			@RequestParam(name="partition_count", 	required=false)	Integer thePartitionCount,
			@RequestParam(name="memory_min", 		required=false)	Integer theMemoryMin,
			@RequestParam(name="memory_max", 		required=false)	Integer theMemoryMax,
			@RequestParam(name="fetch_size", 		required=false)	Integer theFetchSize
		)
		throws Exception
	{
		if (thePartitionCount != null) {
			this.bomUniverse.setPartitionCount(thePartitionCount);
		}
		
		if (theMemoryMax != null) {
			this.bomUniverse.setMemoryMax(theMemoryMax);
		}

		if (theMemoryMin != null) {
			this.bomUniverse.setMemoryMin(theMemoryMin);
		}

		if (theFetchSize != null) {
			this.bomUniverse.setFetchSize(theFetchSize);
		}
		
		this.bomUniverse.refresh();
	}
}
