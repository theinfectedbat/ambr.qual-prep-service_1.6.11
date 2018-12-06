package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngine;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class SetLogThresholdsServiceAPI 
{
	public static final String		URL_PATH									= "/qps/api/preparation_engine/set_log_thresholds";
	public static final String		REQUEST_PARAM_NAME_BOM_QUEUE_THRESHOLD		= "bom_queue_threshold";
	public static final String		REQUEST_PARAM_NAME_TRADE_LANE_THRESHOLD		= "trade_lane_threshold";
	public static final String		REQUEST_PARAM_NAME_CLASS_QUEUE_THRESHOLD	= "class_queue_threshold";
	public static final String		REQUEST_PARAM_NAME_COMP_QUEUE_THRESHOLD		= "comp_queue_threshold";
	public static final String		REQUEST_PARAM_NAME_COMP_IVA_PULL_QUEUE		= "comp_iva_pull_queue_threshold";

	@Autowired
	private PreparationEngineQueueUniverse 	bomProcessorQueueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public SetLogThresholdsServiceAPI()
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
			@RequestParam(name=REQUEST_PARAM_NAME_BOM_QUEUE_THRESHOLD, required=false)		Integer theBOMQueueThreshold,
			@RequestParam(name=REQUEST_PARAM_NAME_TRADE_LANE_THRESHOLD, required=false)		Integer theTradeLaneQueueThreshold,
			@RequestParam(name=REQUEST_PARAM_NAME_CLASS_QUEUE_THRESHOLD, required=false)	Integer theClassQueueThreshold,
			@RequestParam(name=REQUEST_PARAM_NAME_COMP_QUEUE_THRESHOLD, required=false)		Integer theCompQueueThreshold,
			@RequestParam(name=REQUEST_PARAM_NAME_COMP_IVA_PULL_QUEUE, required=false)		Integer theCompIVAPullQueueThreshold
		)
		throws Exception
	{
		this.bomProcessorQueueUniverse.setLogThresholds(
			theBOMQueueThreshold, 
			theTradeLaneQueueThreshold, 
			theClassQueueThreshold, 
			theCompQueueThreshold, 
			theCompIVAPullQueueThreshold
		);
	}
}
