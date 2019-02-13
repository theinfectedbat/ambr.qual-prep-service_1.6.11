package com.ambr.gtm.fta.qps.qualtx.universe.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class QualTXDetailUniverseRefreshAPI 
{
	public static final String		URL_PATH	= "/qps/api/qualtxdetail_universe/refresh";
	
	@Autowired
	private QualTXDetailUniverse universe;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDetailUniverseRefreshAPI()
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
	public void execute
		(
			@RequestParam(name="partition_count", 	required=false)	Integer thePartitionCount,
			@RequestParam(name="memory_min", 		required=false)	Integer theMemoryMin,
			@RequestParam(name="memory_max", 		required=false)	Integer theMemoryMax,
			@RequestParam(name="fetch_size", 		required=false)	Integer theFetchSize
		)
		throws Exception
	{
		if (thePartitionCount != null) {
			this.universe.setPartitionCount(thePartitionCount);
		}
		
		if (theMemoryMax != null) {
			this.universe.setMemoryMax(theMemoryMax);
		}

		if (theMemoryMin != null) {
			this.universe.setMemoryMin(theMemoryMin);
		}

		if (theFetchSize != null) {
			this.universe.setFetchSize(theFetchSize);
		}
		
		this.universe.refresh();
	}
}
