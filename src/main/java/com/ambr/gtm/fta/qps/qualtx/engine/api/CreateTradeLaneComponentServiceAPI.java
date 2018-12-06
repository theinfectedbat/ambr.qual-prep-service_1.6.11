package com.ambr.gtm.fta.qps.qualtx.engine.api;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.result.CreateTradeLaneComponentResult;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class CreateTradeLaneComponentServiceAPI 
{
	public static final String		URL_PATH								= "/qps/api/preparation_engine/create_trade_lane_component";
	public static final String		REQUEST_PARAM_NAME_BOM_KEY				= "bom_key";
	public static final String		REQUEST_PARAM_NAME_BOM_COMP_KEY			= "bom_comp_key";
	public static final String		REQUEST_PARAM_NAME_QUAL_TX_KEY			= "qtx_key";

	@Autowired private PlatformTransactionManager		txMgr;
	@Autowired private DataSource						dataSrc;
	@Autowired private PreparationEngineQueueUniverse	queueUniverse;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     * @param	theBOMCompKey
     * @param	theQualTXKey
     *************************************************************************************
     */
	@RequestMapping(URL_PATH)
	public CreateTradeLaneComponentResult execute
		(
			@RequestParam(name=REQUEST_PARAM_NAME_BOM_KEY, 		required=true)	Long theBOMKey,
			@RequestParam(name=REQUEST_PARAM_NAME_BOM_COMP_KEY, required=true)	Long theBOMCompKey,
			@RequestParam(name=REQUEST_PARAM_NAME_QUAL_TX_KEY, 	required=false)	Long theQualTXKey
		)
		throws Exception
	{
		CreateTradeLaneComponentResult	aResult = new CreateTradeLaneComponentResult();
		
		return aResult;
	}
}
