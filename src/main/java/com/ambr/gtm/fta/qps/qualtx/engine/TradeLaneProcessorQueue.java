package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneProcessorQueue
	extends TaskQueue<TradeLaneProcessorTask>
{
	static Logger						logger = LogManager.getLogger(TradeLaneProcessorQueue.class);

	GPMSourceIVAContainerCache								ivaCache;
	ClassificationProcessorQueue							gpmClassQueue;
	ComponentProcessorQueue 								compQueue;
	TypedPersistenceQueue<QualTX>							qualTXPersistenceQueue;
	TypedPersistenceQueue<QualTXComponent>					qualTXComponentPersistenceQueue;
	TypedPersistenceQueue<QualTXPrice>						qualTXPricePersistenceQueue;
	DataExtensionPersistenceQueue<QualTXDataExtension>		qualTXDataExtQueue;
	public int												maxCompBatchSize;
	UniversalObjectIDGenerator								idGenerator;
	QualTXUniverseGenerationRequest 						request;
	DataExtensionConfigurationRepository					dataExtRepos;
	PreparationEngineQueueUniverse							queueUniverse;
	private HashMap<String, GroupNameSpecification>			targetDEMap;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theIVACache
     * @param	theClassQueue
     * @param	theCompQueue
     * @param	theQualTXPersistenceQueue
     * @param	theQualTXPersistenceQueue
     * @param	theQualTXComponentPersistenceQueue
	 * @param 	theQualTXdataExtQueue 
	 * @param 	theIDGenerator 
     * @param	theDataExtRepos
     *************************************************************************************
     */
	public TradeLaneProcessorQueue(
		GPMSourceIVAContainerCache				theIVACache,
		ClassificationProcessorQueue						theClassQueue,
		ComponentProcessorQueue								theCompQueue,
		TypedPersistenceQueue<QualTX>						theQualTXPersistenceQueue,
		TypedPersistenceQueue<QualTXComponent>				theQualTXComponentPersistenceQueue,
		TypedPersistenceQueue<QualTXPrice>					theQualTXPricePersistenceQueue,
		DataExtensionPersistenceQueue<QualTXDataExtension> 	theQualTXdataExtQueue, 
		UniversalObjectIDGenerator 							theIDGenerator,
		DataExtensionConfigurationRepository				theDataExtRepos)
		throws Exception
	{
		super("Trade Lane Queue", true);
		
		this.ivaCache = theIVACache;
		this.gpmClassQueue = theClassQueue;
		this.compQueue = theCompQueue;
		this.qualTXPersistenceQueue = theQualTXPersistenceQueue;
		this.qualTXComponentPersistenceQueue = theQualTXComponentPersistenceQueue;
		this.qualTXPricePersistenceQueue = theQualTXPricePersistenceQueue;
		this.qualTXDataExtQueue = theQualTXdataExtQueue;
		this.maxCompBatchSize = 100;
		this.idGenerator = theIDGenerator;
		this.dataExtRepos = theDataExtRepos;
		
		GroupNameSpecification aSpec;
		
		this.targetDEMap = new HashMap<>();
		
		aSpec = new GroupNameSpecification("IMPL_BOM_PROD_FAMILY:TEXTILES");
		this.targetDEMap.put(aSpec.groupName, aSpec);

		aSpec = new GroupNameSpecification("IMPL_BOM_PROD_FAMILY:AUTOMOBILE");
		this.targetDEMap.put(aSpec.groupName, aSpec);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueUniverse
     *************************************************************************************
     */
	public TradeLaneProcessorQueue(PreparationEngineQueueUniverse theQueueUniverse)
		throws Exception
	{
		this(
			theQueueUniverse.ivaCache,
			theQueueUniverse.classificationQueue,
			theQueueUniverse.compQueue,
			theQueueUniverse.qualTXQueue,
			theQueueUniverse.qualTXComponentQueue,
			theQueueUniverse.qualTXPriceQueue,
			theQueueUniverse.qualTXdataExtQueue,
			theQueueUniverse.idGenerator,
			theQueueUniverse.dataExtCfgRepos
		);
		
		this.queueUniverse = theQueueUniverse;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGroupName
     *************************************************************************************
     */
	public boolean isDECopyEnabled(String theGroupName)
		throws Exception
	{
		if (theGroupName == null) {
			return false;
		}
		
		return this.targetDEMap.get(theGroupName) != null;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
     * @param	theSrcIVA
     *************************************************************************************
     */
	public Future<TradeLaneProcessorTask> put(BOM theBOM, GPMSourceIVA theSrcIVA)
		throws Exception
	{
		Future<TradeLaneProcessorTask>	aFuture;
		
		aFuture = this.execute(new TradeLaneProcessorTask(this, theBOM, theSrcIVA, this.idGenerator.generate().getLSB()));
		return aFuture;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theMaxBatchSize
     *************************************************************************************
     */
	public void setComponentBatchSize(int theMaxBatchSize)
		throws Exception
	{
		MessageFormatter.info(logger, "setComponentBatchSize", "Current [{0}] Target [{1}]", this.maxCompBatchSize, theMaxBatchSize);
		this.maxCompBatchSize = theMaxBatchSize;
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
