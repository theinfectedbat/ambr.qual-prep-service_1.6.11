package com.ambr.gtm.fta.qps.qualtx.engine.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.platform.utils.queue.TaskQueueParameters;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class SetQueueParametersServiceAPI 
{
	public static final String	URL_PATH												= "/qps/api/preparation_engine/set_queue_params";
	public static final String	REQUEST_PARAM_NAME_IVA_CACHE_SIZE						= "iva_cache_size";
	public static final String	REQUEST_PARAM_NAME_CLASS_CACHE_SIZE						= "class_cache_size";
	public static final String	REQUEST_PARAM_NAME_MAX_QUEUE_DEPTH						= "max_queue_depth";
	public static final String	REQUEST_PARAM_NAME_THREAD_COUNT							= "thread_count";
	public static final String	REQUEST_PARAM_NAME_MAX_COMPONENT_BATCH_SIZE				= "max_comp_batch_size";
	public static final String	REQUEST_PARAM_NAME_BATCH_INSERT_SIZE					= "batch_insert_size";
	public static final String	REQUEST_PARAM_NAME_BATCH_INSERT_MAX_WAIT_PERIOD 		= "batch_insert_max_wait_period";
	private static final String REQUEST_PARAM_NAME_BATCH_INSERT_CONCURRENT_QUEUE_COUNT 	= "batch_insert_concurrent_queue_count";

	@Autowired
	private PreparationEngineQueueUniverse 	bomProcessorQueueUniverse;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public SetQueueParametersServiceAPI()
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
			@RequestParam(name=REQUEST_PARAM_NAME_IVA_CACHE_SIZE, 						required=false)		Integer theIVACacheSize,
			@RequestParam(name=REQUEST_PARAM_NAME_CLASS_CACHE_SIZE, 					required=false)		Integer theClassCacheSize,
			@RequestParam(name=REQUEST_PARAM_NAME_MAX_QUEUE_DEPTH, 						required=false)		Integer theMaxQueueDepth,
			@RequestParam(name=REQUEST_PARAM_NAME_THREAD_COUNT, 						required=false)		Integer theThreadCount,
			@RequestParam(name=REQUEST_PARAM_NAME_MAX_COMPONENT_BATCH_SIZE, 			required=false)		Integer theMaxComponentBatchSize,
			@RequestParam(name=REQUEST_PARAM_NAME_BATCH_INSERT_SIZE, 					required=false)		Integer theBatchInsertSize,
			@RequestParam(name=REQUEST_PARAM_NAME_BATCH_INSERT_MAX_WAIT_PERIOD,			required=false)		Integer theBatchInsertMaxWaitPeriod,
			@RequestParam(name=REQUEST_PARAM_NAME_BATCH_INSERT_CONCURRENT_QUEUE_COUNT,	required=false)		Integer theBatchInsertConcurrentQueueCount
		)
		throws Exception
	{
		if (theIVACacheSize != null) {
			this.bomProcessorQueueUniverse.ivaCache.setMaxSize(theIVACacheSize);
		}
		
		if (theClassCacheSize != null) {
			this.bomProcessorQueueUniverse.gpmClassCache.setMaxSize(theClassCacheSize);
		}
		
		TaskQueueParameters	aQueueParams = new TaskQueueParameters(theThreadCount, theMaxQueueDepth);
		this.bomProcessorQueueUniverse.setQueueParams(aQueueParams);
		
		if (theMaxComponentBatchSize != null) {
			this.bomProcessorQueueUniverse.setComponentBatchSize(theMaxComponentBatchSize);
		}

		if (theBatchInsertSize != null) {
			this.bomProcessorQueueUniverse.setBatchInsertSize(theBatchInsertSize);
		}

		if (theBatchInsertMaxWaitPeriod != null) {
			this.bomProcessorQueueUniverse.setBatchInsertMaxWaitPeriod(theBatchInsertMaxWaitPeriod);
		}
		
		if (theBatchInsertConcurrentQueueCount != null) {
			this.bomProcessorQueueUniverse.setBatchInsertConcurrentQueueCount(theBatchInsertConcurrentQueueCount);
		}
	}
}
