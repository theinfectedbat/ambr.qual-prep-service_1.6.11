package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class ComponentProcessorQueue 
	extends TaskQueue<ComponentProcessorTask>
{
	ClassificationProcessorQueue				gpmClassQueue;
	ComponentIVAPullProcessorQueue				compIVAPullQueue;
	private long								componentProcessedCount;
	QualTXUniverseGenerationRequest 			request;
	PreparationEngineQueueUniverse				queueUniverse;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGPMClassQueue
     * @param	theCompIVAPullQueue
     *************************************************************************************
     */
	public ComponentProcessorQueue(
		ClassificationProcessorQueue 	theGPMClassQueue,
		ComponentIVAPullProcessorQueue	theCompIVAPullQueue)
		throws Exception
	{
		super("Component Queue", true);
		this.gpmClassQueue = theGPMClassQueue;
		this.compIVAPullQueue = theCompIVAPullQueue;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueUniverse
     *************************************************************************************
     */
	public ComponentProcessorQueue(PreparationEngineQueueUniverse theQueueUniverse)
		throws Exception
	{
		this(theQueueUniverse.classificationQueue, theQueueUniverse.compIVAPullQueue);
		this.queueUniverse = theQueueUniverse;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public long getCompletedWorkCount()
		throws Exception
	{
		return this.componentProcessedCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theCount
	 *************************************************************************************
	 */
	synchronized void incrementComponentProcessedCount(int theCount)
		throws Exception
	{
		this.componentProcessedCount += theCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theComponentBatch
	 *************************************************************************************
	 */
	public Future<?> put(ComponentBatch theComponentBatch)
		throws Exception
	{
		Future<ComponentProcessorTask>	aFuture;
		
		aFuture = this.execute(new ComponentProcessorTask(this, theComponentBatch));
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
