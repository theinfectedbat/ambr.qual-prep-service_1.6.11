package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.platform.utils.queue.TaskInterface;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class ClassificationProcessorQueue
	extends TaskQueue<TaskInterface>
{
	static Logger	logger = LogManager.getLogger(ClassificationProcessorQueue.class);

	PreparationEngineQueueUniverse				queueUniverse;
	GPMClassificationProductContainerCache		gpmClassCache;
	private long								classificationCount;
	private QualTXUniverseGenerationRequest 	request;

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGPMClassCache
     *************************************************************************************
     */
	public ClassificationProcessorQueue(GPMClassificationProductContainerCache theGPMClassCache)
		throws Exception
	{
		super("Classification Queue", true);
		this.gpmClassCache = theGPMClassCache;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param theQueueUniverse
	 *************************************************************************************
	 */
	public ClassificationProcessorQueue(PreparationEngineQueueUniverse theQueueUniverse)
		throws Exception
	{
		this(theQueueUniverse.gpmClassCache);
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
		return this.classificationCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theCount
	 *************************************************************************************
	 */
	synchronized void incrementClassificationCount(int theCount)
		throws Exception
	{
		this.classificationCount += theCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 * @param	theQualTX
	 * @param	theStatusTracker
	 *************************************************************************************
	 */
	public Future<TaskInterface> put(BOM theBOM, QualTX theQualTX, TradeLaneStatusTracker theStatusTracker)
		throws Exception
	{
		Future<TaskInterface>	aFuture;
		
		aFuture = this.execute(new QualTXClassificationProcessorTask(this, theBOM, theQualTX, theStatusTracker));
		return aFuture;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theComponentBatch
	 *************************************************************************************
	 */
	public Future<TaskInterface> put(ComponentBatch theComponentBatch)
		throws Exception
	{
		Future<TaskInterface>	aFuture;
		
		aFuture = this.execute(new QualTXComponentClassificationProcessorTask(this, theComponentBatch));
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
