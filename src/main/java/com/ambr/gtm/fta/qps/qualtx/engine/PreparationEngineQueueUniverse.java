package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverse;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniverse;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverse;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailCache;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.api.CacheRefreshInformation;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXPrepLogDtlEntry;
import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXUniversePreparationProgressManager;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetail;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;
import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.rdbms.schema.SchemaDescriptor;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;
import com.ambr.platform.utils.queue.TaskQueue;
import com.ambr.platform.utils.queue.TaskQueueParameters;
import com.ambr.platform.utils.queue.TaskQueueThroughputUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PreparationEngineQueueUniverse 
{
	static final Logger		logger = LogManager.getLogger(PreparationEngineQueueUniverse.class);
	
	private TaskQueueParameters									bomQueueParams;
	private TaskQueueParameters									tradeLaneQueueParams;
	private TaskQueueParameters									classificationQueueParams;
	private TaskQueueParameters									compIVAPullQueueParams;
	private TaskQueueParameters									compQueueParams;
	private TaskQueueParameters									qualTXQueueParams;
	private TaskQueueParameters									qualTXComponentQueueParams;
	private TaskQueueParameters									qualTXComponentPriceQueueParams;
	private TaskQueueParameters									qualTXPriceQueueParams;
	private TaskQueueParameters									qualTXPrepLogQueueParams;
	private TaskQueueParameters									deQueueParams;
	private TaskQueueParameters									bomComponentExpansionQueueParams;
	private TaskQueueParameters									qualTXComponentExpansionQueueParams;
	private TaskQueueParameters									persistenceRetryQueueParams;

	private int													localIVACacheSize;
	private int													localGPMClassCacheSize;
	private int													localGPMClaimDetailsCacheSize;
	private int													localPtnrDetailCacheSize;

	private int													componentMaxBatchSize;
	private int 												batchInsertConcurrentQueueCount;
	private int													batchInsertMaxWaitPeriodInMillis;
	private int													batchInsertSize;

	public BOMProcessorQueue											bomQueue;
	public ComponentProcessorQueue										compQueue;
	public TradeLaneProcessorQueue										tradeLaneQueue;
	public ClassificationProcessorQueue									classificationQueue;
	public ComponentIVAPullProcessorQueue								compIVAPullQueue;
	public TypedPersistenceQueue<QualTX>								qualTXQueue;
	public TypedPersistenceQueue<QualTXPrice>							qualTXPriceQueue;
	public TypedPersistenceQueue<QualTXComponent>						qualTXComponentQueue;
	public TypedPersistenceQueue<QualTXComponentPrice>					qualTXComponentPriceQueue;
	public TypedPersistenceQueue<QualTXPrepLogDtlEntry>					qualTXPrepLogQueue;
	public DataExtensionPersistenceQueue<QualTXDataExtension>			qualTXdataExtQueue;
	public DataExtensionPersistenceQueue<QualTXComponentDataExtension>	qualTXComponentdataExtQueue;
	public BOMComponentExpansionProcessorQueue							bomComponentExpansionQueue;
	public QualTXComponentExpansionProcessorQueue						qualTXComponentExpansionQueue;
	public PersistenceRetryQueue										persistenceRetryQueue;

	public GPMClassificationProductContainerCache				gpmClassCache;
	public GPMSourceIVAContainerCache							ivaCache;
	public GPMClaimDetailsCache									gpmClaimDetailsCache;
	public PartnerDetailCache									ptnrDetailCache;
	
	public BOMUniverse											bomUniverse;
	public QualTXDetailUniverse									qtxDetailUniverse;
	public DataSource											dataSrc;
	public PlatformTransactionManager							txMgr;
	public SchemaDescriptor										schemaDesc;
	public UniversalObjectIDGenerator 							idGenerator;
	public QualTXUniverseGenerationRequest						request;
	public DataExtensionConfigurationRepository					dataExtCfgRepos;
	public ConfigurationPropertyResolver						propertyResolver;
	public TrackerClientAPI										trackerClientAPI;
	public QualTXBusinessLogicProcessor                         qtxBusinessLogicProcessor;
	public QEConfigCache                                        qeConfigCache;
	public QualTXUniversePreparationProgressManager				qtxPrepProgressMgr;
	private long												cacheRefreshStart;
	private long												cacheRefreshComplete;

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePropertyResolver
	 * @param theDataSrc 
     *************************************************************************************
     */
	public PreparationEngineQueueUniverse(
		ConfigurationPropertyResolver thePropertyResolver,
		DataSource theDataSrc)
		throws Exception
	{
		this.propertyResolver = thePropertyResolver;
		this.dataSrc = theDataSrc;
		this.localIVACacheSize = 100;
		this.bomQueueParams = new TaskQueueParameters(5,  100);
		this.tradeLaneQueueParams = new TaskQueueParameters(5,  100);
		this.classificationQueueParams = new TaskQueueParameters(5,  100);
		this.compIVAPullQueueParams = new TaskQueueParameters(5,  100);
		this.compQueueParams = new TaskQueueParameters(5,  100);
		this.deQueueParams = new TaskQueueParameters(5,  100);
		this.bomComponentExpansionQueueParams = new TaskQueueParameters(5, 100);
		this.qualTXComponentExpansionQueueParams =   new TaskQueueParameters(5, 100);
		this.persistenceRetryQueueParams =   new TaskQueueParameters(5, 100);

		this.componentMaxBatchSize = 100;
		this.batchInsertConcurrentQueueCount = 5;
		this.batchInsertMaxWaitPeriodInMillis = 5000;
		this.batchInsertSize = 250;
		this.cacheRefreshStart = System.currentTimeMillis();
		this.cacheRefreshComplete = this.cacheRefreshStart;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getBOMComponent()
		throws Exception
	{
		return this.bomQueue.getCumulativeComponentCount();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getBOMCount()
		throws Exception
	{
		if (this.bomQueue == null) {
			throw new IllegalStateException("BOM Queue has not been initialized");
		}
		return this.bomQueue.getCumulativeTaskCount();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public BOMUniverse getBOMUniverse()
		throws Exception
	{
		return this.bomUniverse;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public CacheRefreshInformation getCacheRefreshInformation()
		throws Exception
	{
		return new CacheRefreshInformation(this.cacheRefreshStart, this.cacheRefreshComplete);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMeasurmentPeriodInSecs
	 *************************************************************************************
	 */
	public String getPerformanceStatus(int theMeasurmentPeriodInSecs)
		throws Exception
	{
		ParameterizedMessageUtility		aMsgUtil = new ParameterizedMessageUtility();
		TaskQueueThroughputUtility		aThroughputUtil;
		
		aMsgUtil.format("BOM Processor Queue Universe Performance : BOMs [{0}] Components [{1}] measurement period (secs) [{2}]", false, true,
			this.bomQueue.getCumulativeTaskCount(),
			this.bomQueue.getCumulativeComponentCount(),
			theMeasurmentPeriodInSecs
		);
		
		aThroughputUtil = new TaskQueueThroughputUtility();
		aThroughputUtil.addQueue(this.bomQueue);
		aThroughputUtil.addQueue(this.tradeLaneQueue);
		aThroughputUtil.addQueue(this.compQueue);
		aThroughputUtil.addQueue(this.compIVAPullQueue);
		aThroughputUtil.addQueue(this.classificationQueue);
		aThroughputUtil.addQueue(this.persistenceRetryQueue);
		aThroughputUtil.addQueues(this.qualTXQueue.getInternalQueues());
		aThroughputUtil.addQueues(this.qualTXPriceQueue.getInternalQueues());
		aThroughputUtil.addQueues(this.qualTXComponentQueue.getInternalQueues());
		aThroughputUtil.addQueues(this.qualTXComponentPriceQueue.getInternalQueues());
		
		for (TypedPersistenceQueue<?> aQueue : this.qualTXdataExtQueue.getInternalQueues()) {
			aThroughputUtil.addQueues(aQueue.getInternalQueues());
		}
	
		for (TypedPersistenceQueue<?> aQueue : this.qualTXComponentdataExtQueue.getInternalQueues()) {
			aThroughputUtil.addQueues(aQueue.getInternalQueues());
		}
		
		aThroughputUtil.measureThroughput(theMeasurmentPeriodInSecs);
	
		for (String aQueueName : aThroughputUtil.getQueueNames()) {
			aMsgUtil.format("   Queue [{0}]: throughput [{1,number,0.##}/sec]", false, true, 
				aQueueName, 
				aThroughputUtil.getThroughput(aQueueName)
			);
		}
		
		return aMsgUtil.getMessage();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxTasksToReport
	 *************************************************************************************
	 */
	public String getStatus(int theMaxTasksToReport)
		throws Exception
	{
		int								aPaddingLength = 3;
		ParameterizedMessageUtility		aMsgUtil = new ParameterizedMessageUtility();
		
		aMsgUtil.format("___________________", false, true);
		aMsgUtil.format("BOM Processor Queue Universe Status: BOMs [{0}] Components [{1}]", false, true,
			this.bomQueue.getCumulativeTaskCount(),
			this.bomQueue.getCumulativeComponentCount()
		);
		
		aMsgUtil.format(this.bomQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.tradeLaneQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.compQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.compIVAPullQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.classificationQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.qualTXQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.qualTXPriceQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.qualTXComponentQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.qualTXComponentPriceQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.bomComponentExpansionQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.qualTXComponentExpansionQueue.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.persistenceRetryQueue.getStatus(aPaddingLength), true, false);
		
		
		for (TypedPersistenceQueue<?> aQueue : this.qualTXdataExtQueue.getInternalQueues()) {
			aMsgUtil.format(aQueue.getStatus(aPaddingLength), true, false);
		}

		for (TypedPersistenceQueue<?> aQueue : this.qualTXComponentdataExtQueue.getInternalQueues()) {
			aMsgUtil.format(aQueue.getStatus(aPaddingLength), true, false);
		}
		
		aMsgUtil.format("   GPM Source IVA Cache: Current Size [{0}] Hits [{1}] Misses [{2}] Hit Ratio [{3,number,percent}]", false, true,
			this.ivaCache.getSize(),
			this.ivaCache.getHits(), 
			this.ivaCache.getMisses(),
			this.ivaCache.getHitRatio()
		);
		
		aMsgUtil.format("   GPM Classification Cache: Current Size [{0}] Hits [{1}] Misses [{2}] Hit Ratio [{3,number,percent}]", false, true,
			this.gpmClassCache.getSize(),
			this.gpmClassCache.getHits(), 
			this.gpmClassCache.getMisses(),
			this.gpmClassCache.getHitRatio()
		);

		aMsgUtil.format("   GPM Claim Details Cache: Current Size [{0}] Hits [{1}] Misses [{2}] Hit Ratio [{3,number,percent}]", false, true,
			this.gpmClaimDetailsCache.getSize(),
			this.gpmClaimDetailsCache.getHits(), 
			this.gpmClaimDetailsCache.getMisses(),
			this.gpmClaimDetailsCache.getHitRatio()
		);

		aMsgUtil.format("   Partner Details Cache: Current Size [{0}] Hits [{1}] Misses [{2}] Hit Ratio [{3,number,percent}]", false, true,
			this.ptnrDetailCache.getSize(),
			this.ptnrDetailCache.getHits(), 
			this.ptnrDetailCache.getMisses(),
			this.ptnrDetailCache.getHitRatio()
		);
		
		aMsgUtil.format(this.bomUniverse.getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.gpmClassCache.getUniverse().getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.ivaCache.getUniverse().getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.gpmClaimDetailsCache.getUniverse().getStatus(aPaddingLength), true, false);
		aMsgUtil.format(this.ptnrDetailCache.getUniverse().getStatus(aPaddingLength), true, false);
		
		aMsgUtil.format("___________________", false, true);
		
		return aMsgUtil.getMessage();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public TaskQueueThroughputUtility getThroughputUtility()
		throws Exception
	{
		TaskQueueThroughputUtility		aUtil;
		
		aUtil = new TaskQueueThroughputUtility();
		aUtil.addQueue(this.bomQueue);
		aUtil.addQueue(this.tradeLaneQueue);
		aUtil.addQueue(this.compQueue);
		aUtil.addQueue(this.classificationQueue);
		aUtil.addQueue(this.compIVAPullQueue);
		aUtil.addQueue(this.persistenceRetryQueue);
		
		aUtil.addQueues(this.qualTXQueue.getInternalQueues());
		aUtil.addQueues(this.qualTXPriceQueue.getInternalQueues());
		aUtil.addQueues(this.qualTXComponentQueue.getInternalQueues());
		aUtil.addQueues(this.qualTXComponentPriceQueue.getInternalQueues());
		
		for (TypedPersistenceQueue<?> aQueue : this.qualTXdataExtQueue.getInternalQueues()) {
			aUtil.addQueues(aQueue.getInternalQueues());
		}

		for (TypedPersistenceQueue<?> aQueue : this.qualTXComponentdataExtQueue.getInternalQueues()) {
			aUtil.addQueues(aQueue.getInternalQueues());
		}
		
		return aUtil;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param theRequest 
	 * @param theTracker 
	 *************************************************************************************
	 */
	public synchronized void initialize(QualTXUniverseGenerationRequest theRequest)
		throws Exception
	{
		MessageFormatter.info(logger, "initialize", "starting initialization.");
		
		if (theRequest != null) {
			MessageFormatter.info(logger, "initialize", "{0}", theRequest.toString());
		}
		
		this.request = theRequest;
		if (this.request == null) {
			this.request = new QualTXUniverseGenerationRequest();
		}
		
		if (this.request.refreshCachesFlag) {
			this.refreshCaches();
		}
		
		this.shutdown(false);
		
		this.classificationQueue = new ClassificationProcessorQueue(this);
		this.classificationQueue.setQueueParemeters(this.classificationQueueParams);
		this.classificationQueue.setRequest(this.request);
		this.classificationQueue.start();
		
		this.compIVAPullQueue = new ComponentIVAPullProcessorQueue(this);
		this.compIVAPullQueue.setQueueParemeters(this.compIVAPullQueueParams);
		this.compIVAPullQueue.setRequest(this.request);
		this.compIVAPullQueue.start();
		
		this.compQueue = new ComponentProcessorQueue(this);
		this.compQueue.setQueueParemeters(this.compQueueParams);
		this.compQueue.setRequest(this.request);
		this.compQueue.start();
		
		this.qualTXQueue = new TypedPersistenceQueue<QualTX>(this.dataSrc, this.txMgr, "DB Queue - QTX", QualTX.class);	
		this.qualTXQueue.setQueueParams(this.qualTXQueueParams);
		this.qualTXQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXQueue.setBatchSize(this.batchInsertSize);
		this.qualTXQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		this.qualTXQueue.start();
	
		this.qualTXComponentQueue = new TypedPersistenceQueue<QualTXComponent>(this.dataSrc, this.txMgr, "DB Queue - QTX Component", QualTXComponent.class);	
		this.qualTXComponentQueue.setQueueParams(this.qualTXComponentQueueParams);
		this.qualTXComponentQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXComponentQueue.setBatchSize(this.batchInsertSize);
		this.qualTXComponentQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		this.qualTXComponentQueue.start();

		this.qualTXComponentPriceQueue = new TypedPersistenceQueue<QualTXComponentPrice>(this.dataSrc, this.txMgr, "DB Queue - QTX Component Price", QualTXComponentPrice.class);	
		this.qualTXComponentPriceQueue.setQueueParams(this.qualTXComponentPriceQueueParams);
		this.qualTXComponentPriceQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXComponentPriceQueue.setBatchSize(this.batchInsertSize);
		this.qualTXComponentPriceQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		this.qualTXComponentPriceQueue.start();
		
		this.qualTXPriceQueue = new TypedPersistenceQueue<QualTXPrice>(this.dataSrc, this.txMgr, "DB Queue - QTX Price", QualTXPrice.class);
		this.qualTXPriceQueue.setQueueParams(this.qualTXPriceQueueParams);
		this.qualTXPriceQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXPriceQueue.setBatchSize(this.batchInsertSize);
		this.qualTXPriceQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		this.qualTXPriceQueue.start();
		
		this.qualTXdataExtQueue = new DataExtensionPersistenceQueue<QualTXDataExtension>(this.dataSrc,  this.txMgr);
		this.qualTXdataExtQueue.setQueueParams(this.deQueueParams);
		this.qualTXdataExtQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXdataExtQueue.setBatchSize(this.batchInsertSize);
		this.qualTXdataExtQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);

		this.qualTXComponentdataExtQueue = new DataExtensionPersistenceQueue<QualTXComponentDataExtension>(this.dataSrc,  this.txMgr);
		this.qualTXComponentdataExtQueue.setQueueParams(this.deQueueParams);
		this.qualTXComponentdataExtQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXComponentdataExtQueue.setBatchSize(this.batchInsertSize);
		this.qualTXComponentdataExtQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		
		this.qualTXPrepLogQueue = new TypedPersistenceQueue<QualTXPrepLogDtlEntry>(this.dataSrc, this.txMgr, "DB Queue - QTX Prep Log", QualTXPrepLogDtlEntry.class);	
		this.qualTXPrepLogQueue.setQueueParams(this.qualTXQueueParams);
		this.qualTXPrepLogQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
		this.qualTXPrepLogQueue.setBatchSize(this.batchInsertSize);
		this.qualTXPrepLogQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		this.qualTXPrepLogQueue.start();
		
		this.tradeLaneQueue = new TradeLaneProcessorQueue(this);
		this.tradeLaneQueue.setQueueParemeters(this.tradeLaneQueueParams);
		this.tradeLaneQueue.setComponentBatchSize(this.componentMaxBatchSize);
		this.tradeLaneQueue.setRequest(this.request);
		this.tradeLaneQueue.start();
		
		this.bomQueue = new BOMProcessorQueue(this);
		this.bomQueue.setRequest(this.request);
		this.bomQueue.setQueueParemeters(this.bomQueueParams);
		this.bomQueue.start();
		
		this.persistenceRetryQueue = new PersistenceRetryQueue(this);
		this.persistenceRetryQueue.setQueueParemeters(this.persistenceRetryQueueParams);
		this.persistenceRetryQueue.start();
		
		this.qualTXComponentExpansionQueue = new QualTXComponentExpansionProcessorQueue(this);
		this.qualTXComponentExpansionQueue.setRequest(this.request);
		this.qualTXComponentExpansionQueue.setQueueParemeters(this.qualTXComponentExpansionQueueParams);
		this.qualTXComponentExpansionQueue.start();
		
		this.bomComponentExpansionQueue = new BOMComponentExpansionProcessorQueue(this);
		this.bomComponentExpansionQueue.setRequest(this.request);
		this.bomComponentExpansionQueue.setQueueParemeters(this.bomComponentExpansionQueueParams);
		this.bomComponentExpansionQueue.start();
		
		MessageFormatter.info(logger, "initialize", "initialization complete.");
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 *************************************************************************************
	 */
	public void processBOM(BOM theBOM)
		throws Exception
	{
		if (theBOM == null) {
			throw new IllegalArgumentException("The BOM object must be specified");
		}
		this.bomQueue.put(theBOM);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 *************************************************************************************
	 */
	public void processBOMExpansion(BOM theBOM)
			throws Exception
	{
		if (theBOM == null) {
			throw new IllegalArgumentException("The BOM object must be specified");
		}
		this.bomComponentExpansionQueue.put(theBOM);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 * @param	theQualTXDetail
	 * @param	theBOMTracker
	 *************************************************************************************
	 */
	public void processQualTXExpansion(
		BOM 				theBOM, 
		QualTXDetail 		theQualTXDetail, 
		BOMStatusTracker 	theBOMTracker)
			throws Exception
	{
		if (theBOM == null) {
			throw new IllegalArgumentException("The BOM object must be specified");
		}
		
		if (theQualTXDetail == null) {
			throw new IllegalArgumentException("The QualTXDetail object must be specified");
		}
		
		if (theBOMTracker == null) {
			throw new IllegalArgumentException("The BOM Tracker object must be specified");
		}
		this.qualTXComponentExpansionQueue.put(theBOM, theQualTXDetail, theBOMTracker);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void refreshCaches()
		throws Exception
	{
		ExecutorService aExecService = Executors.newFixedThreadPool(6);

		MessageFormatter.info(logger, "refreshCaches", "start.");
		this.cacheRefreshStart = System.currentTimeMillis();

		if (this.bomUniverse != null) {
			aExecService.submit(()->{try {this.bomUniverse.ensureAvailable();} catch (Exception e) {
				MessageFormatter.error(logger, "refreshCaches", e, "BOM Universe unavailable");}}
			);
		}

		if (this.gpmClassCache != null) {
			aExecService.submit(()->{try {this.gpmClassCache.refresh(true);} catch (Exception e) {
				MessageFormatter.error(logger, "refreshCaches", e, "GPM Classification Cache unavailable");}}
			);
		}

		if (this.ivaCache != null) {
			aExecService.submit(()->{try {this.ivaCache.refresh(true);} catch (Exception e) {
				MessageFormatter.error(logger, "refreshCaches", e, "GPM Source IVA Cache unavailable");}}
			);
		}
		
		if (this.gpmClaimDetailsCache != null) {
			aExecService.submit(()->{try {this.gpmClaimDetailsCache.refresh(true);} catch (Exception e) {
				MessageFormatter.error(logger, "refreshCaches", e, "GPM Claim Details Cache unavailable");}}
			);
		}
		
		if (this.qtxDetailUniverse != null) {
			aExecService.submit(()->{try {this.qtxDetailUniverse.ensureAvailable();} catch (Exception e) {
				MessageFormatter.error(logger, "refreshCaches", e, "QTX Detail Universe unavailable");}}
			);
		}

		if (this.ptnrDetailCache != null) {
			aExecService.submit(()->{try {this.ptnrDetailCache.refresh(true);} catch (Exception e) {
				MessageFormatter.error(logger, "refreshCaches", e, "Partner Details Cache unavailable");}}
			);
		}
		
		aExecService.shutdown();
		while (!aExecService.isTerminated()) {
			try {
				aExecService.awaitTermination(1, TimeUnit.MINUTES);
			}
			catch (Exception e) {
				MessageFormatter.debug(logger, "completeInitialization", e, "exception while waiting for cache refresh tasks to complete");
			}
		}
		
		this.cacheRefreshComplete = System.currentTimeMillis();
		
		MessageFormatter.info(logger, "refreshCaches", "complete.");
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void resetCacheStatistics()
		throws Exception
	{
		this.ivaCache.resetCacheStatistics();
		this.gpmClassCache.resetCacheStatistics();
		this.gpmClaimDetailsCache.resetCacheStatistics();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theConcurrentQueueCount
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setBatchInsertConcurrentQueueCount(int theConcurrentQueueCount)
		throws Exception
	{
		MessageFormatter.info(logger, "setBatchInsertConcurrentQueueCount", "Current [{0}] Target [{1}]", this.batchInsertConcurrentQueueCount, theConcurrentQueueCount);
		this.batchInsertConcurrentQueueCount = theConcurrentQueueCount;
		
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theWaitPeriod		If value is between 1 and 999, it is interpreted to be
	 * 								specified in seconds.  Otherwise, it is interpreted to
	 * 								be in milliseconds.
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setBatchInsertMaxWaitPeriod(int theWaitPeriod)
		throws Exception
	{
		MessageFormatter.info(logger, "setBatchInsertMaxWaitPeriod", "Current [{0}] Target [{1}]", this.batchInsertMaxWaitPeriodInMillis, theWaitPeriod);
		
		this.batchInsertMaxWaitPeriodInMillis = theWaitPeriod;
		
		TypedPersistenceQueue<?>[] aTypedQueueList = new TypedPersistenceQueue<?>[] {
			this.qualTXQueue,
			this.qualTXComponentQueue,
			this.qualTXComponentPriceQueue,
			this.qualTXPriceQueue,
			this.qualTXPrepLogQueue
		};

		for (TypedPersistenceQueue<?> aQueue : aTypedQueueList) {
			if (aQueue == null) {continue;}
			aQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		}
		
		DataExtensionPersistenceQueue<?>[] aDataExtQueueList = new DataExtensionPersistenceQueue<?>[] {
			this.qualTXdataExtQueue,
			this.qualTXComponentdataExtQueue
		};

		for (DataExtensionPersistenceQueue<?> aQueue : aDataExtQueueList) {
			if (aQueue == null) {continue;}
			aQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInMillis);
		}
		
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBatchInsertSize
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setBatchInsertSize(int theBatchInsertSize)
		throws Exception
	{
		MessageFormatter.info(logger, "setBatchInsertSize", "Current [{0}] Target [{1}]", this.batchInsertSize, theBatchInsertSize);
		this.batchInsertSize = theBatchInsertSize;
		return this;
	}

	public PreparationEngineQueueUniverse setBOMComponentExpansionQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setBOMComponentExpansionQueueParams", this.bomComponentExpansionQueueParams);
		this.bomComponentExpansionQueueParams = theParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxQueueDepth
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setBOMQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setBOMQueueParams", this.bomQueueParams);
		this.bomQueueParams = theParams;

		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setBOMUniverse(BOMUniverse theUniverse)
		throws Exception
	{
		this.bomUniverse = theUniverse;
		return this;
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
		MessageFormatter.info(logger, "setComponentBatchSize", "Current [{0}] Target [{1}]", this.componentMaxBatchSize, theMaxBatchSize);
		this.componentMaxBatchSize = theMaxBatchSize;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setClassificationQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setClassificationQueueParams", this.classificationQueueParams);
		this.classificationQueueParams = theParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setCompIVAPullQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setCompIVAPullQueueParams", this.compIVAPullQueueParams);
		this.compIVAPullQueueParams = theParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setCompQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setCompQueueParams", this.compQueueParams);
		this.compQueueParams = theParams;
	
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theRepos
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setDataExtensionConfigurationRepository(DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		this.dataExtCfgRepos = theRepos;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueueParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setDataExtensionQueueParams(TaskQueueParameters theQueueParams)
		throws Exception
	{
		theQueueParams.log(logger, "setDataExtensionQueueParams", this.deQueueParams);
		this.deQueueParams = theQueueParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theDataSrc
	 * @param	theTxMgr
	 * @param	theSchemaDesc
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setDataSource(
		DataSource 					theDataSrc, 
		PlatformTransactionManager 	theTxMgr,
		SchemaDescriptor			theSchemaDesc)
		throws Exception
	{
		this.dataSrc = theDataSrc;
		this.txMgr = theTxMgr;
		this.schemaDesc = theSchemaDesc;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 * @param	theLocalCacheSize
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setGPMClaimDetailsUniverse(
		GPMClaimDetailsUniverse theUniverse,
		int						theLocalCacheSize)
		throws Exception
	{
		this.localGPMClaimDetailsCacheSize = theLocalCacheSize;
		this.gpmClaimDetailsCache = new GPMClaimDetailsCache(theUniverse, this.localGPMClaimDetailsCacheSize);
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setGPMClassificationUniverse(
		GPMClassificationUniverse 	theUniverse, 
		int 						theLocalCacheSize)
		throws Exception
	{
		this.localGPMClassCacheSize = theLocalCacheSize;
		this.gpmClassCache = new GPMClassificationProductContainerCache(theUniverse, this.localGPMClassCacheSize);
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setGPMSourceIVAUniverse(
		GPMSourceIVAUniverse 	theUniverse, 
		int 					theLocalCacheSize)
		throws Exception
	{
		this.localIVACacheSize = theLocalCacheSize;
		this.ivaCache = new GPMSourceIVAContainerCache(theUniverse, this.localIVACacheSize);
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theIDGenerator
	 *************************************************************************************
	 */
	public void setIDGenerator(UniversalObjectIDGenerator theIDGenerator)
		throws Exception
	{
		this.idGenerator = theIDGenerator;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMQueueLogThreshold
	 * @param	theTradeLaneQueueLogThreshold
	 * @param	theClassQueueLogThreshold
	 * @param	theCompQueueLogThreshold
	 * @param	theCompIVAPullQueueLogThreshold
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setLogThresholds(
		Integer theBOMQueueLogThreshold,
		Integer theTradeLaneQueueLogThreshold,
		Integer theClassQueueLogThreshold,
		Integer theCompQueueLogThreshold,
		Integer theCompIVAPullQueueLogThreshold)
		throws Exception
	{
		if (theBOMQueueLogThreshold != null) {
			this.bomQueue.setLogThreshold(theBOMQueueLogThreshold);
		}
		
		if (theClassQueueLogThreshold != null) {
			this.classificationQueue.setLogThreshold(theClassQueueLogThreshold);
		}
	
		if (theCompIVAPullQueueLogThreshold != null) {
			this.compIVAPullQueue.setLogThreshold(theCompIVAPullQueueLogThreshold);
		}
		
		if (theCompQueueLogThreshold != null) {
			this.compQueue.setLogThreshold(theCompQueueLogThreshold);
		}
		
		if (theTradeLaneQueueLogThreshold != null) {
			this.tradeLaneQueue.setLogThreshold(theTradeLaneQueueLogThreshold);
		}
		
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUniverse
	 * @param	theLocalCacheSize
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setPartnerDetailUniverse(
		PartnerDetailUniverse 	theUniverse, 
		int 					theLocalCacheSize)
		throws Exception
	{
		this.localPtnrDetailCacheSize = theLocalCacheSize;
		this.ptnrDetailCache = new PartnerDetailCache(theUniverse, this.localPtnrDetailCacheSize);
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setPersistenceRetryQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setPersistenceRetryQueueParams", this.persistenceRetryQueueParams);
		this.persistenceRetryQueueParams = theParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXComponentExpansionQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setQualTXComponentExpansionQueueParams", this.qualTXComponentExpansionQueueParams);
		this.qualTXComponentExpansionQueueParams = theParams;
	
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXComponentQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setQualTXComponentQueueParams", this.qualTXComponentQueueParams);
		this.qualTXComponentQueueParams = theParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueueParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXComponentPriceQueueParams(TaskQueueParameters theQueueParams)
		throws Exception
	{
		theQueueParams.log(logger, "setQualTXComponentPriceQueueParams", this.qualTXComponentPriceQueueParams);
		this.qualTXComponentPriceQueueParams = theQueueParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXDetailUniverse
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXDetailUniverse(QualTXDetailUniverse theQualTXDetailUniverse)
		throws Exception
	{
		this.qtxDetailUniverse = theQualTXDetailUniverse;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueueParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXPrepLogQueueParams(TaskQueueParameters theQueueParams)
		throws Exception
	{
		theQueueParams.log(logger, "setQualTXPrepLogQueueParams", this.qualTXPrepLogQueueParams);
		this.qualTXPrepLogQueueParams = theQueueParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueueParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXPriceQueueParams(TaskQueueParameters theQueueParams)
		throws Exception
	{
		theQueueParams.log(logger, "setQualTXPriceQueueParams", this.qualTXPriceQueueParams);
		this.qualTXPriceQueueParams = theQueueParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQualTXQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setQualTXQueueParams", this.qualTXQueueParams);
		this.qualTXQueueParams = theParams;
	
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueueParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQueueParams(TaskQueueParameters theQueueParams)
		throws Exception
	{
		this.setBOMQueueParams(theQueueParams);
		this.setClassificationQueueParams(theQueueParams);
		this.setCompIVAPullQueueParams(theQueueParams);
		this.setCompQueueParams(theQueueParams);
		this.setQualTXQueueParams(theQueueParams);
		this.setQualTXPriceQueueParams(theQueueParams);
		this.setQualTXComponentQueueParams(theQueueParams);
		this.setQualTXComponentPriceQueueParams(theQueueParams);
		this.setTradeLaneQueueParams(theQueueParams);
		this.setDataExtensionQueueParams(theQueueParams);
		this.setBOMComponentExpansionQueueParams(theQueueParams);
		this.setQualTXComponentExpansionQueueParams(theQueueParams);
		this.setPersistenceRetryQueueParams(theQueueParams);
		this.setQualTXPrepLogQueueParams(theQueueParams);
		
		return this;
	}
	
	public PreparationEngineQueueUniverse setQtxBusinessLogicProcessor(QualTXBusinessLogicProcessor theBusinessLogicProcessor)
			throws Exception
	{
		this.qtxBusinessLogicProcessor = theBusinessLogicProcessor;
		this.qtxBusinessLogicProcessor.determineComponentCOO.setPtnrDetailsCache(this.ptnrDetailCache);
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theAPI
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setTrackerClientAPI(TrackerClientAPI theAPI)
		throws Exception
	{
		this.trackerClientAPI = theAPI;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProgressMgr
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setQtxPrepProgressMgr(QualTXUniversePreparationProgressManager theProgressMgr)
		throws Exception
	{
		this.qtxPrepProgressMgr = theProgressMgr;
		return this;
	}

	public PreparationEngineQueueUniverse setQEConfigCache(QEConfigCache qeConfigCache) throws Exception
	{
		this.qeConfigCache = qeConfigCache;
		return this;
	}
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParams
	 *************************************************************************************
	 */
	public PreparationEngineQueueUniverse setTradeLaneQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setTradeLaneQueueParams", this.tradeLaneQueueParams);
		this.tradeLaneQueueParams = theParams;
	
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * This method is invoked by the spring framework subsystem when the bean is being shutdown
	 * and "cleaned" up.
	 * </P>
	 *************************************************************************************
	 */
	public void shutdown()
		throws Exception
	{
		MessageFormatter.info(logger, "shutdown", "shutting down Preparation Engine Queue Universe");
		this.shutdown(true);
		MessageFormatter.info(logger, "shutdown", "Preparation Engine Queue Universe shutdown complete.");
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theOrderlyFlag
	 *************************************************************************************
	 */
	public void shutdown(boolean theOrderlyFlag)
		throws Exception
	{
		MessageFormatter.info(logger, "shutdown", "Initiating shutdown. Orderly [{0}]", theOrderlyFlag);
	
		// Clean up the task queues.  It is necessary to prevent a memory leak
		
		TaskQueue<?>[] aTaskQueueList = new TaskQueue<?>[] {
			this.classificationQueue,
			this.compIVAPullQueue,
			this.compQueue,
			this.tradeLaneQueue,
			this.bomQueue,
			this.bomComponentExpansionQueue,
			this.qualTXComponentExpansionQueue,
			this.persistenceRetryQueue
		};
		
		for (TaskQueue<?> aQueue : aTaskQueueList) {
			if (aQueue == null) {continue;}
			aQueue.shutdown(theOrderlyFlag);
		}

		// Clean up the typed persistence queues.  It is necessary to prevent a memory leak

		TypedPersistenceQueue<?>[] aTypedQueueList = new TypedPersistenceQueue<?>[] {
			this.qualTXQueue,
			this.qualTXComponentQueue,
			this.qualTXComponentPriceQueue,
			this.qualTXPriceQueue,
			this.qualTXPrepLogQueue
		};

		for (TypedPersistenceQueue<?> aQueue : aTypedQueueList) {
			if (aQueue == null) {continue;}
			aQueue.shutdown(theOrderlyFlag);
		}

		// Clean up the data extension persistence queues.  It is necessary to prevent a memory leak
		
		DataExtensionPersistenceQueue<?>[] aDataExtQueueList = new DataExtensionPersistenceQueue<?>[] {
			this.qualTXdataExtQueue,
			this.qualTXComponentdataExtQueue
		};

		for (DataExtensionPersistenceQueue<?> aQueue : aDataExtQueueList) {
			if (aQueue == null) {continue;}
			aQueue.shutdown(theOrderlyFlag);
		}
		
		MessageFormatter.info(logger, "shutdown", "Shutdown complete");
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void waitForCompletion()
		throws Exception
	{
		this.bomQueue.waitForCompletion();
		this.tradeLaneQueue.waitForCompletion();
		this.compQueue.waitForCompletion();
		this.classificationQueue.waitForCompletion();
		this.compIVAPullQueue.waitForCompletion();
		this.qualTXQueue.waitForCompletion();
		this.qualTXPriceQueue.waitForCompletion();
		this.qualTXComponentQueue.waitForCompletion();
		this.qualTXComponentPriceQueue.waitForCompletion();
		this.qualTXdataExtQueue.waitForCompletion();
		this.qualTXComponentdataExtQueue.waitForCompletion();
		this.bomComponentExpansionQueue.waitForCompletion();
		this.qualTXComponentExpansionQueue.waitForCompletion();
		this.persistenceRetryQueue.waitForCompletion();
		this.qualTXPrepLogQueue.waitForCompletion();
	}
}
