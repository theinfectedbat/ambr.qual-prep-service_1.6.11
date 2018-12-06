package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMComponentExpansionProcessorQueue
	extends TaskQueue<BOMComponentExpansionTask>
{
	QualTXUniverseGenerationRequest 			request;
	PreparationEngineQueueUniverse				queueUniverse;
	QualTXComponentExpansionProcessorQueue		qualTXComponentExpansionProcessorQueue;	
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theIVACache
     * @param	theQtxDetailUniverse
     * @param	theTradeLaneQueue
     *************************************************************************************
     */
	public BOMComponentExpansionProcessorQueue(QualTXDetailUniverse	theQtxDetailUniverse, QualTXComponentExpansionProcessorQueue theQualTXComponentExpansionProcessorQueue)
		throws Exception
	{
		super("BOM Component Expansion Processor Queue", true);
		this.qualTXComponentExpansionProcessorQueue = theQualTXComponentExpansionProcessorQueue;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueEngine
     *************************************************************************************
     */
	public BOMComponentExpansionProcessorQueue(PreparationEngineQueueUniverse theQueueEngine)
		throws Exception
	{
		this(theQueueEngine.qtxDetailUniverse, theQueueEngine.qualTXComponentExpansionQueue);
		this.queueUniverse = theQueueEngine;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
     *************************************************************************************
     */
	public Future<BOMComponentExpansionTask> put(BOM theBOM)
		throws Exception
	{
		Future<BOMComponentExpansionTask>	aFuture;
		
		
		aFuture = this.execute(new BOMComponentExpansionTask(this, theBOM));
		return aFuture;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theRequest
     *************************************************************************************
     */
	public void setRequest(QualTXUniverseGenerationRequest theRequest)
		throws Exception
	{
		this.request = theRequest;
	}
}
