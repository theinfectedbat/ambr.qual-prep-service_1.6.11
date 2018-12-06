package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class ComponentIVAPullProcessorQueue 
	extends TaskQueue<ComponentIVAPullProcessorTask>
{
	PreparationEngineQueueUniverse				queueUniverse;
	GPMSourceIVAContainerCache		ivaCache;
	GPMClaimDetailsCache						claimDetailsCache;
	long 										ivaPullCount;
	QualTXUniverseGenerationRequest 			request;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theIVACache
	 * @param	theClaimDetailsCache
	 *************************************************************************************
	 */
	public ComponentIVAPullProcessorQueue(
		GPMSourceIVAContainerCache 	theIVACache,
		GPMClaimDetailsCache		theClaimDetailsCache)
		throws Exception
	{
		super("Component IVA Queue", true);
		this.ivaCache = theIVACache;
		this.claimDetailsCache = theClaimDetailsCache;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param theQueueUniverse
	 *************************************************************************************
	 */
	public ComponentIVAPullProcessorQueue(PreparationEngineQueueUniverse theQueueUniverse)
		throws Exception
	{
		this(theQueueUniverse.ivaCache, theQueueUniverse.gpmClaimDetailsCache);
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
		return this.ivaPullCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theCount
	 *************************************************************************************
	 */
	synchronized void incrementIVAPullCount(int theCount)
		throws Exception
	{
		this.ivaPullCount += theCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMComp
	 * @param	theQualTXComp
	 *************************************************************************************
	 */
	public Future<ComponentIVAPullProcessorTask> put(ComponentBatch theComponentBatch)
		throws Exception
	{
		Future<ComponentIVAPullProcessorTask>	aFuture;
		
		aFuture = this.execute(new ComponentIVAPullProcessorTask(this, theComponentBatch));
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
