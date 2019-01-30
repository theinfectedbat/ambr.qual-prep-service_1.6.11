package com.ambr.gtm.fta.qps.bom.qualstatus.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.BOMUniversePartition;
import com.ambr.gtm.fta.qps.bom.qualstatus.BOMQualificationStatus;
import com.ambr.gtm.fta.qps.bom.qualstatus.BOMQualificationStatusGenerator;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetBOMQualificationStatusServiceAPI 
{	
	@Autowired
	private BOMUniversePartition	partition;

	@Autowired
	private PreparationEngineQueueUniverse		queueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetBOMQualificationStatusServiceAPI()
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
	@RequestMapping(GetBOMQualificationStatusClientAPI.URL_PATH_TEMPLATE)
	public BOMQualificationStatus execute(@PathVariable(name = GetBOMQualificationStatusClientAPI.URL_PATH_VARIABLE_NAME_BOM_KEY) long theBOMKey)
		throws Exception
	{
		BOMQualificationStatusGenerator		aGenerator;
		
		aGenerator = new BOMQualificationStatusGenerator(this.queueUniverse);
		return aGenerator.generate(theBOMKey);
	}
}
