package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMProcessorQueue
	extends TaskQueue<BOMProcessorTask>
{
	GPMSourceIVAContainerCache		ivaCache;
	TradeLaneProcessorQueue						tradeLaneQueue;
	private int									cumulativeComponentCount;
	QualTXUniverseGenerationRequest 			request;
	QualTXDetailUniverse						qtxDetailUniverse;
	PreparationEngineQueueUniverse				queueUniverse;
	boolean										ignoreTradeLandConfigurationFlag;
	
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
	private BOMProcessorQueue(
		GPMSourceIVAContainerCache		theIVACache,
		QualTXDetailUniverse			theQtxDetailUniverse,
		TradeLaneProcessorQueue 		theTradeLaneQueue)
		throws Exception
	{
		super("BOM Processor Queue", true);
		this.ivaCache = theIVACache; 
		this.qtxDetailUniverse = theQtxDetailUniverse;
		this.tradeLaneQueue = theTradeLaneQueue;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueEngine
     *************************************************************************************
     */
	public BOMProcessorQueue(PreparationEngineQueueUniverse theQueueEngine)
		throws Exception
	{
		this(theQueueEngine.ivaCache, theQueueEngine.qtxDetailUniverse, theQueueEngine.tradeLaneQueue);
		this.queueUniverse = theQueueEngine;
		this.ignoreTradeLandConfigurationFlag = "Y".equalsIgnoreCase(
			this.queueUniverse.propertyResolver.getPropertyValue(QPSProperties.IGNORE_TRADE_LANE_CONFIGURATION, "N")
		);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getCumulativeComponentCount()
		throws Exception
	{
		return this.cumulativeComponentCount;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
     *************************************************************************************
     */
	public Future<BOMProcessorTask> put(BOM theBOM)
		throws Exception
	{
		Future<BOMProcessorTask>	aFuture;
		
		if (theBOM == null) {
			throw new IllegalArgumentException("The BOM object must be specified");
		}
		
		this.cumulativeComponentCount += theBOM.componentCount;
		
		aFuture = this.execute(new BOMProcessorTask(this, theBOM));
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
