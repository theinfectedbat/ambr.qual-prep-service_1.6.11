package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetail;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualTXComponentExpansionProcessorQueue
	extends TaskQueue<QualTXComponentExpansionTask>
{
	QualTXUniverseGenerationRequest 			request;
	PreparationEngineQueueUniverse				queueUniverse;
	BOM											bom;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQtxDetailUniverse
     *************************************************************************************
     */
	public QualTXComponentExpansionProcessorQueue(QualTXDetailUniverse theQtxDetailUniverse)
		throws Exception
	{
		super("QualTX Component Expansion Processor Queue", true);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueEngine
     *************************************************************************************
     */
	public QualTXComponentExpansionProcessorQueue(PreparationEngineQueueUniverse theQueueEngine)
		throws Exception
	{
		this(theQueueEngine.qtxDetailUniverse);
		this.queueUniverse = theQueueEngine;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
	 * @param theBOMTracker 
     *************************************************************************************
     */
	public Future<QualTXComponentExpansionTask> put(BOM theBOM, QualTXDetail theQualTXDetail, BOMStatusTracker theBOMTracker)
		throws Exception
	{
		Future<QualTXComponentExpansionTask>	aFuture;
		
		
		aFuture = this.execute(new QualTXComponentExpansionTask(this, theBOM, theQualTXDetail, theBOMTracker));
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
