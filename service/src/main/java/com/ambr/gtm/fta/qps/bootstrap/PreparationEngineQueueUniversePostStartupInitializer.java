package com.ambr.gtm.fta.qps.bootstrap;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.bom.BOMUniverseProperties;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverse;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverseProperties;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniverse;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniverseProperties;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverse;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverseProperties;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniverse;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniverseProperties;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXUniversePreparationProgressManager;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;
import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.rdbms.bootstrap.PrimaryDataSourceConfiguration;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;
import com.ambr.platform.utils.queue.TaskQueueParameters;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
public class PreparationEngineQueueUniversePostStartupInitializer 
{
	static Logger						logger = LogManager.getLogger(PreparationEngineQueueUniversePostStartupInitializer.class);

	@Autowired @Qualifier(PrimaryDataSourceConfiguration.TX_MGR_BEAN_NAME)		private PlatformTransactionManager txMgr;
	@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME)	private DataSource dataSrc;
	
    @Autowired	private ConfigurationPropertyResolver propertyResolver;
	@Autowired	private PreparationEngineQueueUniverse queueUniverse;
	@Autowired	private BOMUniverse	bomUniverse;
	@Autowired 	private GPMSourceIVAUniverse gpmSrcIVAUniverse;
	@Autowired	private GPMClassificationUniverse gpmClassUniverse;
	@Autowired	private UniversalObjectIDGenerator idGenerator;
	@Autowired  private GPMClaimDetailsUniverse gpmClaimDetailsUniverse;
	@Autowired	private DataExtensionConfigurationRepository repos;
	@Autowired	private QualTXDetailUniverse qtxDetailUniverse;
	@Autowired	private TrackerClientAPI trackerClientAPI;
	@Autowired  private QualTXBusinessLogicProcessor qtxBusinessLogicProcessor;
	@Autowired  private SchemaDescriptorService schemaDescService;
	@Autowired  private QEConfigCache qeConfigCache;
	@Autowired  private QualTXUniversePreparationProgressManager qtxUniversePrepProgressMgr;
	@Autowired	private PartnerDetailUniverse ptnrDetailUniverse;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public PreparationEngineQueueUniversePostStartupInitializer()
		throws Exception
	{
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@EventListener(ApplicationReadyEvent.class)
	public void completeInitialization()
		throws Exception
	{
		int			aClassCacheSize;
		int			aIVACacheSize;
		int			aClaimDetailsCacheSize;
		int			aPtnrDetailCacheSize;
		int 		aThreadCount;
		int			aMaxQueueDepth;
		int			aBatchInsertSize;
		int			aMaxWaitPeriod;
		int			aBatchInsertConcurrencyCount;

		MessageFormatter.info(logger, "completeInitialization", "start.");

		if ("Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(BOMUniverseProperties.UNIVERSE_ENABLED, "N")) == false) {
			// We don't need to initialize this JVM with a functional BOM Universe
			MessageFormatter.info(logger, "completeInitialization", "Queue Universe does not require initialization.");
			return;
		}

		aClassCacheSize = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClassificationUniverseProperties.LOCAL_CACHE_SIZE, "100000"));
		aIVACacheSize = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMSourceIVAUniverseProperties.LOCAL_CACHE_SIZE, "100000"));
		aClaimDetailsCacheSize = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.LOCAL_CACHE_SIZE, "100000"));
		aPtnrDetailCacheSize = Integer.valueOf(this.propertyResolver.getPropertyValue(PartnerDetailUniverseProperties.LOCAL_CACHE_SIZE, "100000"));
		aThreadCount = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.THREAD_COUNT, "10"));
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "1000"));
		aBatchInsertSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.BATCH_INSERT_SIZE, "250"));
		aMaxWaitPeriod = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.BATCH_INSERT_MAX_WAIT_PERIOD, "5"));
		aBatchInsertConcurrencyCount = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.BATCH_INSERT_CONCURRENCY_COUNT, "5"));
		
		this.queueUniverse.setIDGenerator(this.idGenerator);
		this.queueUniverse.setBOMUniverse(this.bomUniverse);
		this.queueUniverse.setGPMClaimDetailsUniverse(this.gpmClaimDetailsUniverse, aClaimDetailsCacheSize);
		this.queueUniverse.setGPMClassificationUniverse(this.gpmClassUniverse, aClassCacheSize);
		this.queueUniverse.setGPMSourceIVAUniverse(this.gpmSrcIVAUniverse, aIVACacheSize);
		this.queueUniverse.setPartnerDetailUniverse(this.ptnrDetailUniverse, aPtnrDetailCacheSize);
		this.queueUniverse.setQueueParams(new TaskQueueParameters(aThreadCount, aMaxQueueDepth));
		this.queueUniverse.setDataSource(this.dataSrc, this.txMgr, this.schemaDescService.getPrimarySchemaDescriptor());
		this.queueUniverse.setBatchInsertSize(aBatchInsertSize);
		this.queueUniverse.setBatchInsertMaxWaitPeriod(aMaxWaitPeriod);
		this.queueUniverse.setBatchInsertConcurrentQueueCount(aBatchInsertConcurrencyCount);
		this.queueUniverse.setDataExtensionConfigurationRepository(this.repos);
		this.queueUniverse.setQualTXDetailUniverse(this.qtxDetailUniverse);
		this.queueUniverse.setTrackerClientAPI(trackerClientAPI);
		this.queueUniverse.setQEConfigCache(qeConfigCache);
		this.queueUniverse.setQtxBusinessLogicProcessor(qtxBusinessLogicProcessor);
		this.queueUniverse.setQtxPrepProgressMgr(this.qtxUniversePrepProgressMgr);
		
		try {
//			this.queueUniverse.initialize(null);
			MessageFormatter.info(logger, "completeInitialization", "complete.");
		}
		catch (Exception e) {
			MessageFormatter.error(logger, "completeInitialization", e, "BOM Processor Queue Universe failed to initialize.");
		}
	}
}
