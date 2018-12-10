package com.ambr.gtm.fta.qps.bootstrap;

import java.net.URL;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ambr.gtm.fta.qps.CommandEnum;
import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.bom.BOMUniversePartition;
import com.ambr.gtm.fta.qps.bom.BOMUniverseProperties;
import com.ambr.gtm.fta.qps.bom.api.BOMUniverseBOMClientAPI;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverse;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniversePartition;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverseProperties;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniverse;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniversePartition;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationUniverseProperties;
import com.ambr.gtm.fta.qps.gpmclass.api.GetGPMClassificationsByProductFromUniverseClientAPI;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverse;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniversePartition;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAUniverseProperties;
import com.ambr.gtm.fta.qps.gpmsrciva.api.GetGPMSourceIVAByProductFromUniverseClientAPI;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngine;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.api.GetCacheRefreshInformationClientAPI;
import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXUniversePreparationProgressManager;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverse;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniversePartition;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailUniverseProperties;
import com.ambr.gtm.fta.qps.util.CumulationComputationRule;
import com.ambr.gtm.fta.qps.util.CurrencyExchangeRateManager;
import com.ambr.gtm.fta.qps.util.DetermineComponentCOO;
import com.ambr.gtm.fta.qts.QTSProperties;
import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.TrackerLoader;
import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.qts.config.FTACtryConfigCache;
import com.ambr.gtm.fta.qts.config.FTAHSListCache;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.gtm.fta.qts.trade.MDIQualTxRepository;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.qts.workmgmt.QTXCompWorkProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXStageProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkPersistenceProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkProducer;
import com.ambr.gtm.utils.legacy.multiorg.OrgCache;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.platform.rdbms.bootstrap.PrimaryDataSourceConfiguration;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordInsertSQLStatement;
import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordPersistenceQueue;
import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordTask;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.cache.CacheManagerService;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;
import com.ambr.platform.utils.subservice.SubordinateServiceConnector;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
@EnableTransactionManagement
public class ApplicationConfiguration 
{
	static Logger						logger = LogManager.getLogger(ApplicationConfiguration.class);

    @Value("${com.ambr.gtm.fta.qps.command}") 										public String command;
    
	@Autowired   																	private ApplicationContext appContext;
    @Autowired 																		private ConfigurationPropertyResolver propertyResolver;
    @Autowired 	@Qualifier(PrimaryDataSourceConfiguration.TX_MGR_BEAN_NAME)			private PlatformTransactionManager txMgr;
	@Autowired	@Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME)	private DataSource dataSrc;
	@Autowired
	Environment									environment;
    private TrackerLoader						trackerLoader;
	private UniversalObjectIDGenerator			idGenerator;
    private BOMUniverse							bomUniverse;
    private BOMUniversePartition				bomUniversePartition;
    private GPMSourceIVAUniverse				gpmIVAUniverse;
    private GPMSourceIVAUniversePartition		gpmIVAUniversePartition;
    private GPMClassificationUniverse			gpmClassUniverse;
    private GPMClassificationUniversePartition	gpmClassUniversePartition;
    private PreparationEngineQueueUniverse		queueUniverse;
    private GPMClaimDetailsUniverse				gpmClaimDetailsUniverse;
    private GPMClaimDetailsUniversePartition	gpmClaimDetailsUniversePartition;
    private QualTXDetailUniverse				qualTXDetailUniverse;
    private QualTXDetailUniversePartition		qualTXDetailUniversePartition;
    private QualTXUniversePreparationProgressManager				qualTXUniversePreparationProgressManager;
    private Env env;
    private TrackerClientAPI aClientAPI;
    private QualTXBusinessLogicProcessor qualTXBusinessLogicProcessor;
            
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@EventListener(ApplicationReadyEvent.class)
	public void afterStartupEventHandler()
		throws Exception
	{
		CommandEnum						aCommandEnum;
		ExitCodeGenerator				aDefaultGenerator;
		SubordinateServiceConnector		aConnector;
		
		aDefaultGenerator = new ExitCodeGenerator()	{
			public int getExitCode() 
			{
				return 0;
			}
		};
		
		aConnector = new SubordinateServiceConnector(this.propertyResolver, this.appContext);
		aConnector.persistServiceContext();
		aConnector.monitorParentProcess();
		//TODO as it is cyclic dependency setting in post startup, this has to be changed 
		aCommandEnum = CommandEnum.valueOf(this.command);
		if (aCommandEnum == CommandEnum.SERVICE) {

			System.gc();
			MessageFormatter.info(logger, "afterStartupEventHandler", "Service statup complete. Memory:  Total [{0}] Free [{1}] Max [{2}] Used [{3}]",
				Runtime.getRuntime().totalMemory(),
				Runtime.getRuntime().freeMemory(),
				Runtime.getRuntime().maxMemory(),
				Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()
			);
			
			return;
		}
		
		try {
			switch (aCommandEnum) {
				case TEST:
				{
					this.test();
					break;
				}
			}
			
			Runtime.getRuntime().gc();
			MessageFormatter.info(logger, "afterStartupEventHandler", "Total [{0}] Max [{1}] Free [{2}] Utilized [{3}]", 
				Runtime.getRuntime().totalMemory(),
				Runtime.getRuntime().maxMemory(),
				Runtime.getRuntime().freeMemory(),
				Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()
			);
		}
		finally {
			MessageFormatter.info(logger, "afterStartupEventHandler", "Shutting down...");
			SpringApplication.exit(this.appContext,	aDefaultGenerator);
			MessageFormatter.info(logger, "afterStartupEventHandler", "Shutdown complete.");
		}
	}
	
	@Bean MDIQualTxRepository beanMDIQualTxRepository(@Autowired UniversalObjectIDGenerator idGenerator)
	{
		MDIQualTxRepository qualTxRepository = new MDIQualTxRepository(idGenerator);
		
		return qualTxRepository;
	}
	
	@Bean
	public Env beanEnv() throws Exception
	{
		 this.env = new Env(			 
				this.propertyResolver.getPropertyValue(QTSProperties.TRACKER_SERVICE_URL, ""), 
				this.propertyResolver.getPropertyValue(QTSProperties.TA_SERVICE_URL, ""));
		this.env.setSingleton(this.env);
		String taServiceKey = this.propertyResolver.getPropertyValue(QTSProperties.TRADE_SERVICE_CLIENT_KEY, "");
		this.env.getTradeQualtxClient().setTradekey(taServiceKey);
		return this.env;
	}
	
	@Bean
	public TrackerContainer beanTrackerContainer(@Autowired QTXWorkRepository qtxWorkRepository) throws Exception
	{
		TrackerContainer trackerContainer = new TrackerContainer(qtxWorkRepository);

		return trackerContainer;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@Bean(destroyMethod = "shutdown")
	public BOMUniverse beanBOMUniverse()
		throws Exception
	{
		// Just initialize a basic BOM Universe object.  The remainder of the initialization process
		// will occur when the Spring Application is fully initialized.  This is necessary because
		// we need to know the server port the JVM is using.
		this.bomUniverse = new BOMUniverse();
		
		return this.bomUniverse;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public BOMUniversePartition beanBOMUniversePartition(
		@Autowired ConfigurationPropertyResolver thePropertyResolver,
		@Autowired DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		int			aMaxQueueDepth;
		int			aFetchSize;
		String 		aTargetSchema = null;
		boolean		aUniversePartitionEnabledFlag;
		int 		aPartitionCount;
		int			aPartitionNum;
		String		aFilterOrgCode;
	
		aUniversePartitionEnabledFlag = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(BOMUniverseProperties.UNIVERSE_PARTITION_ENABLED, "N"));
	
		if (!aUniversePartitionEnabledFlag) {
			// We don't intended to utilize this JVM as a BOM Universe partition
			// We will initialize a "blank" object, but we won't ever load it.
			this.bomUniversePartition = new BOMUniversePartition();
			return this.bomUniversePartition;
		}
		
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(BOMUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aPartitionNum = Integer.valueOf(this.propertyResolver.getPropertyValue(BOMUniverseProperties.UNIVERSE_PARTITION_NUM, "1"));
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
		
		this.bomUniversePartition = new BOMUniversePartition(aPartitionCount, aPartitionNum, aFilterOrgCode);
		this.bomUniversePartition.setFetchSize(aFetchSize);
		this.bomUniversePartition.setMaxCursorDepth(aMaxQueueDepth);
		this.bomUniversePartition.setTargetSchema(aTargetSchema);
		this.bomUniversePartition.setDataExtensionConfigRepos(theRepos);
		this.bomUniversePartition.load(new JdbcTemplate(this.dataSrc));
	
		return this.bomUniversePartition;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean(destroyMethod = "shutdown")
	public GPMClaimDetailsUniverse beanGPMClaimDetailsUniverse()
		throws Exception
	{
		// Just initialize a basic GPM Claim Universe object.  The remainder of the initialization process
		// will occur when the Spring Application is fully initialized.  This is necessary because
		// we need to know the server port the JVM is using.
		this.gpmClaimDetailsUniverse = new GPMClaimDetailsUniverse();
		
		return this.gpmClaimDetailsUniverse;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public GPMClaimDetailsUniversePartition beanGPMClaimDetailsUniversePartition
		(
			@Autowired ConfigurationPropertyResolver thePropertyResolver,
			@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME) DataSource theDataSrc
		)
		throws Exception
	{
		int						aMaxQueueDepth;
		int						aFetchSize;
		String 					aTargetSchema = null;
		boolean					aPartitionEnabledFlag;
		int 					aPartitionCount;
		int						aPartitionNum;
		String					aFilterOrgCode;
	
		aPartitionEnabledFlag = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_ENABLED, "N"));
	
		if (!aPartitionEnabledFlag) {
			// We don't intended to utilize this JVM as a BOM Universe partition
			// We will initialize a "blank" object, but we won't ever load it.
			this.gpmClaimDetailsUniversePartition = new GPMClaimDetailsUniversePartition();
			return this.gpmClaimDetailsUniversePartition;
		}
		
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aPartitionNum = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_NUM, "1"));
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
		
		this.gpmClaimDetailsUniversePartition = new GPMClaimDetailsUniversePartition(aPartitionCount, aPartitionNum, aFilterOrgCode);
		this.gpmClaimDetailsUniversePartition.setFetchSize(aFetchSize);
		this.gpmClaimDetailsUniversePartition.setMaxCursorDepth(aMaxQueueDepth);
		this.gpmClaimDetailsUniversePartition.setTargetSchema(aTargetSchema);
		this.gpmClaimDetailsUniversePartition.load(new JdbcTemplate(this.dataSrc));
	
		return this.gpmClaimDetailsUniversePartition;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@Bean(destroyMethod = "shutdown")
	public GPMClassificationUniverse beanGPMClassificationUniverse()
		throws Exception
	{
		// Just initialize a basic GPM Classification Universe object.  The remainder of the initialization process
		// will occur when the Spring Application is fully initialized.  This is necessary because
		// we need to know the server port the JVM is using.
		this.gpmClassUniverse = new GPMClassificationUniverse();
		
		return this.gpmClassUniverse;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public GPMClassificationUniversePartition beanGPMClassificationUniversePartition
		(
			@Autowired ConfigurationPropertyResolver thePropertyResolver,
			@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME) DataSource theDataSrc
		)
		throws Exception
	{
		int			aMaxQueueDepth;
		int			aFetchSize;
		String 		aTargetSchema = null;
		int 		aPartitionCount;
		int			aPartitionNum;
		String		aFilterOrgCode;
	
		if ("Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(GPMClassificationUniverseProperties.UNIVERSE_PARTITION_ENABLED, "N")) == false) {
			// We don't intended to utilize this JVM as a GPM Classification Universe partition
			// We will initialize a "blank" object, but we won't ever load it.
			this.gpmClassUniversePartition = new GPMClassificationUniversePartition();
			return this.gpmClassUniversePartition;
		}
		
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClassificationUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aPartitionNum = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClassificationUniverseProperties.UNIVERSE_PARTITION_NUM, "1"));
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
		
		this.gpmClassUniversePartition = new GPMClassificationUniversePartition(aPartitionCount, aPartitionNum, aFilterOrgCode);
		this.gpmClassUniversePartition.setFetchSize(aFetchSize);
		this.gpmClassUniversePartition.setMaxCursorDepth(aMaxQueueDepth);
		this.gpmClassUniversePartition.setTargetSchema(aTargetSchema);
		this.gpmClassUniversePartition.load(new JdbcTemplate(this.dataSrc));
	
		return this.gpmClassUniversePartition;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public GPMSourceIVAContainerCache beanGPMSourceIVAProductSourceContainerCache(@Autowired GPMSourceIVAUniverse theUniverse)
		throws Exception
	{
		GPMSourceIVAContainerCache		aCache;
		
		aCache = new GPMSourceIVAContainerCache(theUniverse, 1000);
		return aCache;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean(destroyMethod = "shutdown")
	public GPMSourceIVAUniverse beanGPMSourceIVAUniverse()
		throws Exception
	{
		// Just initialize a basic GPM Source IVA Universe object.  The remainder of the initialization process
		// will occur when the Spring Application is fully initialized.  This is necessary because
		// we need to know the server port the JVM is using.
		this.gpmIVAUniverse = new GPMSourceIVAUniverse();
		
		return this.gpmIVAUniverse;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public GPMSourceIVAUniversePartition beanGPMSourceIVAUniversePartition
		(
			@Autowired ConfigurationPropertyResolver thePropertyResolver,
			@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME) DataSource theDataSrc
		)
		throws Exception
	{
		int			aMaxQueueDepth;
		int			aFetchSize;
		String 		aTargetSchema = null;
		boolean		aPartitionEnabledFlag;
		int 		aPartitionCount;
		int			aPartitionNum;
		String		aFilterOrgCode;
	
		aPartitionEnabledFlag = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(GPMSourceIVAUniverseProperties.UNIVERSE_PARTITION_ENABLED, "N"));
	
		if (!aPartitionEnabledFlag) {
			// We don't intended to utilize this JVM as a BOM Universe partition
			// We will initialize a "blank" object, but we won't ever load it.
			this.gpmIVAUniversePartition = new GPMSourceIVAUniversePartition();
			return this.gpmIVAUniversePartition;
		}
		
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMSourceIVAUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aPartitionNum = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMSourceIVAUniverseProperties.UNIVERSE_PARTITION_NUM, "1"));
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
		
		this.gpmIVAUniversePartition = new GPMSourceIVAUniversePartition(aPartitionCount, aPartitionNum, aFilterOrgCode);
		this.gpmIVAUniversePartition.setFetchSize(aFetchSize);
		this.gpmIVAUniversePartition.setMaxCursorDepth(aMaxQueueDepth);
		this.gpmIVAUniversePartition.setTargetSchema(aTargetSchema);
		this.gpmIVAUniversePartition.load(new JdbcTemplate(this.dataSrc));
	
		return this.gpmIVAUniversePartition;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@Bean
	public PreparationEngine beanPreparationEngine()
		throws Exception
	{
		PreparationEngine		aPrepEngine;
		
		aPrepEngine = new PreparationEngine();
		return aPrepEngine;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public PreparationEngineQueueUniverse beanPreparationEngineQueueUniverse
		(
			@Autowired ConfigurationPropertyResolver thePropertyResolver,
			@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME) DataSource theDataSrc
		)
		throws Exception
	{
		this.queueUniverse = new PreparationEngineQueueUniverse(this.propertyResolver, theDataSrc);
		return this.queueUniverse;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public QualTXUniversePreparationProgressManager beanQualTXUniversePreparationProgressManager(
		@Autowired
		PreparationEngineQueueUniverse 	theQueueUniverse,
		
		@Autowired
		@Qualifier(StatusCacheConfiguration.STATUS_TRACKER_CACHE_MANAGER_SERVICE_BEAN_NAME)	
		CacheManagerService theCacheMgrService,
		
		@Autowired
		ConfigurationPropertyResolver	thePropertyResolver)
		throws Exception
	{
		return new QualTXUniversePreparationProgressManager(theQueueUniverse, theCacheMgrService, thePropertyResolver);
	}
	
	@Bean
	public QEConfigCache beanQEConfigCache(@Autowired DataSource dataSrc, 
			@Autowired OrgCache orgCache,
			@Autowired Env env,
			@Autowired FTACtryConfigCache ftaCtryCache) throws Exception
	{
		boolean loadQEConfigCacheEnabledFlag = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.LOAD_QE_CONFIG_CACHE, "N"));
		QEConfigCache qeConfigCache = null;
		if (loadQEConfigCacheEnabledFlag)
		{
			qeConfigCache = new QEConfigCache(orgCache, new JdbcTemplate(dataSrc), 1000, ftaCtryCache);
			qeConfigCache.loadFullQEConfigCache();
		}
		else
		{
			qeConfigCache = new QEConfigCache();
		}
		return qeConfigCache;
	}
	
	@Bean
	public FTACtryConfigCache beanFTACtryConfigCache(@Autowired DataSource dataSrc, 
			@Autowired OrgCache orgCache
			) throws Exception
	{
		boolean loadUsingTAService = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.LOAD_FTA_CTRY_DG_USING_TA_SERVICE, "N"));
		boolean loadFTACtryCacheEnabledFlag = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.FTA_CTRY_DG_CACHE, "N"));
		FTACtryConfigCache ftaCtryConfigCache = null;
		if (loadFTACtryCacheEnabledFlag)
		{
			ftaCtryConfigCache = new FTACtryConfigCache(orgCache, new JdbcTemplate(dataSrc), 1000, loadUsingTAService);
			ftaCtryConfigCache.load();
		}
		else
		{
			ftaCtryConfigCache = new FTACtryConfigCache();
		}
		return ftaCtryConfigCache;
	}
	
	@Bean
	public FTAHSListCache beanFTAHSListCache() throws Exception
	{
		FTAHSListCache ftaHSListCache = new FTAHSListCache();
		return ftaHSListCache;
	}
	
   
	@Bean
	public QTXWorkProducer beanQTXWorkProducer(
			@Autowired PlatformTransactionManager txMgr, 
			@Autowired DataSource dataSrc, 
			@Autowired BOMUniverse bomUniverse, 
			@Autowired UniversalObjectIDGenerator idGenerator, 
			@Autowired MDIQualTxRepository qualTxRepository, 
			@Autowired QTXWorkRepository workRepository,
			@Autowired SchemaDescriptorService schemaService) throws Exception
	{
		boolean requalServiceRequired = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.REQUAL_SERVICE_START, "N"));
		QTXWorkProducer qtxWorkProducer = new QTXWorkProducer(schemaService, txMgr, new JdbcTemplate(dataSrc));

		if (requalServiceRequired)
		{
			qtxWorkProducer.init(
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".threads", "20")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".read_ahead", "5000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".fetch_size", "1000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".batch_size", "1")),
			Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".sleep_interval", "" + 5*60*1000)),
			qualTxRepository,
			workRepository,
			idGenerator);
			
			String bomClientURL = this.propertyResolver.getPropertyValue(BOMUniverseBOMClientAPI.class.getName() + ".url", "");
			BOMUniverseBOMClientAPI bomUniverseBOMClientAPI = new BOMUniverseBOMClientAPI(new URL(bomClientURL));
			
			String gpmClassClientUrl = this.propertyResolver.getPropertyValue(GetGPMClassificationsByProductFromUniverseClientAPI.class.getName() + ".url", "");
			GetGPMClassificationsByProductFromUniverseClientAPI gpmClassificationsByProductFromUniverseClientAPI = new GetGPMClassificationsByProductFromUniverseClientAPI(new URL(gpmClassClientUrl));

			String gpmIVAClientUrl = this.propertyResolver.getPropertyValue(GetGPMSourceIVAByProductFromUniverseClientAPI.class.getName() + ".url", "");
			GetGPMSourceIVAByProductFromUniverseClientAPI gpmSourceIVAByProductFromUniverseClientAPI = new GetGPMSourceIVAByProductFromUniverseClientAPI(new URL(gpmIVAClientUrl));
		
			String cacheRefreshUrl = this.propertyResolver.getPropertyValue(GetCacheRefreshInformationClientAPI.class.getName() + ".url", "");
			GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI = new GetCacheRefreshInformationClientAPI(new URL(cacheRefreshUrl));
			
			qtxWorkProducer.setAPI(bomUniverseBOMClientAPI, gpmClassificationsByProductFromUniverseClientAPI, gpmSourceIVAByProductFromUniverseClientAPI, cacheRefreshInformationClientAPI);
		}
		
		return qtxWorkProducer;
	}
    
	@Bean
	public QTXStageProducer beanQTXStageProducer(
			@Autowired QTXWorkProducer workProducer, 
			@Autowired PlatformTransactionManager txMgr, 
			@Autowired DataSource dataSrc, 
			@Autowired BOMUniverse bomUniverse, 
			@Autowired UniversalObjectIDGenerator idGenerator, 
			@Autowired MDIQualTxRepository qualTxRepository, 
			@Autowired QTXWorkRepository workRepository, 
			@Autowired TrackerClientAPI trackerClientAPI,
			@Autowired SchemaDescriptorService schemaService,
			@Autowired QEConfigCache qeConfigCache) throws Exception
	{
		boolean requalServiceRequired = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.REQUAL_SERVICE_START, "N"));
		QTXStageProducer qtxStageProducer = new QTXStageProducer(schemaService, txMgr, new JdbcTemplate(dataSrc));

		if (requalServiceRequired)
		{
			qtxStageProducer.init(
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".threads", "20")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".read_ahead", "5000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".fetch_size", "1000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".batch_size", "1")),
			Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkProducer.class.getName() + ".sleep_interval", "" + 5*60*1000)),
			qualTxRepository,
			workRepository,
			idGenerator,
			bomUniverse,qeConfigCache);
			
			String cacheRefreshUrl = this.propertyResolver.getPropertyValue(GetCacheRefreshInformationClientAPI.class.getName() + ".url", "");
			GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI = new GetCacheRefreshInformationClientAPI(new URL(cacheRefreshUrl));
			
			qtxStageProducer.setAPI(cacheRefreshInformationClientAPI, trackerClientAPI);
		}
		
		return qtxStageProducer;
	}
    
	@Bean
	public QTXWorkPersistenceProducer beanQTXWorkPersistenceProducer(
			@Autowired PlatformTransactionManager txMgr, 
			@Autowired DataSource dataSrc, 
			@Autowired UniversalObjectIDGenerator idGenerator, 
			@Autowired MDIQualTxRepository qualTxRepository, 
			@Autowired QTXWorkRepository workRepository,
			@Autowired SchemaDescriptorService schemaService) throws Exception
	{
		boolean requalServiceRequired = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.REQUAL_SERVICE_START, "N"));

		QTXWorkPersistenceProducer qtxWorkPersistenceProducer = new QTXWorkPersistenceProducer(schemaService, txMgr, new JdbcTemplate(dataSrc));
		
		if (requalServiceRequired)
		{
			qtxWorkPersistenceProducer.init(
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkPersistenceProducer.class.getName() + ".threads", "20")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkPersistenceProducer.class.getName() + ".read_ahead", "5000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkPersistenceProducer.class.getName() + ".fetch_size", "1000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkPersistenceProducer.class.getName() + ".batch_size", "1")),
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkPersistenceProducer.class.getName() + ".sleep_interval", "" + 5*60*1000)),
					qualTxRepository,
					workRepository,
					idGenerator);
		}
		
		return qtxWorkPersistenceProducer;
	}
	
	@Bean
	public QTXWorkRepository beanQTXWorkRepository(UniversalObjectIDGenerator idGenerator, @Autowired DataSource dataSrc) throws NumberFormatException, Exception
	{
		int batchSize = Integer.parseInt(this.propertyResolver.getPropertyValue(QTXWorkRepository.class.getName() + ".batch_size", "1000"));
		
		QTXWorkRepository workRepository = new QTXWorkRepository(idGenerator, new JdbcTemplate(dataSrc), batchSize);
		
		return workRepository;
	}
    
	@Bean
	public QTXCompWorkProducer beanQTXCompWorkProducer(
		@Autowired PlatformTransactionManager 		theTxMgr, 
		@Autowired DataSource 						theDataSrc,
		@Autowired UniversalObjectIDGenerator 		theIDGenerator, 
		@Autowired MDIQualTxRepository 				theQualTXRepository, 
		@Autowired QTXWorkRepository 				theWorkRepository,
		@Autowired PreparationEngineQueueUniverse	theQueueUniverse,
		@Autowired DataExtensionConfigurationRepository deRepos,
		@Autowired QualTXBusinessLogicProcessor businsessLogicProcessor,
		@Autowired SchemaDescriptorService schemaDescriptorService
		) 
		throws Exception
	{
		boolean requalServiceRequired = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.REQUAL_SERVICE_START, "N"));

		QTXCompWorkProducer qtxCompWorkProducer = new QTXCompWorkProducer(deRepos, theTxMgr, new JdbcTemplate(theDataSrc), schemaDescriptorService);
		qtxCompWorkProducer.setQtxBusinessLogicProcessor(businsessLogicProcessor);
		
		
		if (requalServiceRequired)
		{
			qtxCompWorkProducer.init(
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXCompWorkProducer.class.getName() + ".threads", "20")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXCompWorkProducer.class.getName() + ".read_ahead", "5000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXCompWorkProducer.class.getName() + ".fetch_size", "1000")), 
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXCompWorkProducer.class.getName() + ".batch_size", "1")),
					Integer.parseInt(this.propertyResolver.getPropertyValue(QTXCompWorkProducer.class.getName() + ".sleep_interval", "" + 5*60*1000)),
					theQualTXRepository,
					theWorkRepository,
					theIDGenerator);
			qtxCompWorkProducer.setQueueUniverse(theQueueUniverse);
		}
		
		return qtxCompWorkProducer;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean(destroyMethod = "shutdown")
	public QualTXDetailUniverse beanQualTXDetailUniverse()
		throws Exception
	{
		// Just initialize a basic Qual TX Detail Universe object.  The remainder of the initialization process
		// will occur when the Spring Application is fully initialized.  This is necessary because
		// we need to know the server port the JVM is using.
		this.qualTXDetailUniverse = new QualTXDetailUniverse();
		
		return this.qualTXDetailUniverse;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Bean
	public QualTXDetailUniversePartition beanQualTXDetailUniversePartition
		(
			@Autowired ConfigurationPropertyResolver thePropertyResolver,
			@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME) DataSource theDataSrc
		)
		throws Exception
	{
		int			aMaxQueueDepth;
		int			aFetchSize;
		String 		aTargetSchema = null;
		boolean		aUniversePartitionEnabledFlag;
		int 		aPartitionCount;
		int			aPartitionNum;
		String		aFilterOrgCode;
	
		aUniversePartitionEnabledFlag = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QualTXDetailUniverseProperties.UNIVERSE_PARTITION_ENABLED, "N"));
	
		if (!aUniversePartitionEnabledFlag) {
			// We don't intended to utilize this JVM as a BOM Universe partition
			// We will initialize a "blank" object, but we won't ever load it.
			this.qualTXDetailUniversePartition = new QualTXDetailUniversePartition();
			return this.qualTXDetailUniversePartition;
		}
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(QualTXDetailUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aPartitionNum = Integer.valueOf(this.propertyResolver.getPropertyValue(QualTXDetailUniverseProperties.UNIVERSE_PARTITION_NUM, "1"));
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
		
		this.qualTXDetailUniversePartition = new QualTXDetailUniversePartition(aPartitionCount, aPartitionNum, aFilterOrgCode);
		this.qualTXDetailUniversePartition.setFetchSize(aFetchSize);
		this.qualTXDetailUniversePartition.setMaxCursorDepth(aMaxQueueDepth);
		this.qualTXDetailUniversePartition.setTargetSchema(aTargetSchema);
		this.qualTXDetailUniversePartition.load(new JdbcTemplate(this.dataSrc));
	
		return this.qualTXDetailUniversePartition;
	}

	@Bean
	public QualTXBusinessLogicProcessor beanQtxBusinessLogicProcessor(@Autowired QEConfigCache qeConfigCache, 
			@Autowired SimplePropertySheetManager propertySheetManager,@Autowired DataExtensionConfigurationRepository theRepos,@Autowired FTAHSListCache ftaHSListCache) throws Exception
	{
		this.qualTXBusinessLogicProcessor = new QualTXBusinessLogicProcessor(qeConfigCache,ftaHSListCache); 
		CurrencyExchangeRateManager currencyExchangeRateManager = new CurrencyExchangeRateManager();
		CumulationComputationRule computationRule = new CumulationComputationRule(currencyExchangeRateManager, qeConfigCache, propertySheetManager, theRepos, ftaHSListCache);
		this.qualTXBusinessLogicProcessor.setCurrencyExchangeRateManager(currencyExchangeRateManager);
		this.qualTXBusinessLogicProcessor.setCumulationComputationRule(computationRule);
		this.qualTXBusinessLogicProcessor.setDetermineComponentCOO(new DetermineComponentCOO());
		this.qualTXBusinessLogicProcessor.setPropertySheetManager(propertySheetManager);
		this.qualTXBusinessLogicProcessor.setDataExtensionConfigRepos(theRepos);

		return this.qualTXBusinessLogicProcessor;
	}
	
	@Bean
	public TrackerClientAPI beanTrackerClient() throws Exception {
		
		String trackerServiceURL = this.propertyResolver.getPropertyValue(QTSProperties.TRACKER_SERVICE_URL, null);
		this.aClientAPI = new TrackerClientAPI(trackerServiceURL);
		
		return this.aClientAPI;
	}

	@Bean
	public TrackerLoader beanLoadTrackerUniverse(@Autowired TrackerContainer trackerContainer) throws Exception
	{
		int aFetchSize;
		String aTargetSchema = null;
		boolean trackerServiceRequired = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.TRACKER_SERVICE_START, "N"));
	
		if (!trackerServiceRequired)
		{
			this.trackerLoader = new TrackerLoader();
			MessageFormatter.info(logger, "beanLoadTrackerUniverse", "Tracker service starting is not enabled.");
			return this.trackerLoader;
		}
			
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QTSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
	
		this.trackerLoader = new TrackerLoader(this.propertyResolver, this.dataSrc);
		this.trackerLoader.setFetchSize(aFetchSize);
		this.trackerLoader.setTargetSchema(aTargetSchema);
		// TODO Convert this to multithreading to have parallel processing.
		this.trackerLoader.loadTracker(trackerContainer);
		MessageFormatter.info(logger, "beanLoadTrackerUniverse", "Tracker service is started successfully.");
		return this.trackerLoader;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void test()
		throws Exception
	{
		Object[] aValue = new MessageFormat("BOM.{0,number}|QTX.{1,number}").parse("BOM.3031677479|QTX.-6335854562173191080");
		System.out.println("done" + aValue[0]);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void testBatch()
		throws Exception
	{
		TransactionStatus 							aStatus = this.txMgr.getTransaction(new DefaultTransactionDefinition());
		BatchDataRecordPersistenceQueue				aBatchPersistenceQueue;
		BatchDataRecordInsertSQLStatement			aSqlStatement;
		ArrayList<QualTX>							aList;
		PerformanceTracker							aPerfTracker = new PerformanceTracker(logger, Level.INFO, "testBatch");
		
		aSqlStatement = new BatchDataRecordInsertSQLStatement(QualTX.class);
		
		aList = new ArrayList<>();
		for (int aIndex = 1; aIndex <= 199; aIndex++) {
			QualTX	aQualTX = new QualTX(this.idGenerator, 0);
			
			aQualTX.ctry_of_import = "US";
			aQualTX.effective_from = new Timestamp(System.currentTimeMillis());
			aQualTX.effective_to = new Timestamp(System.currentTimeMillis());
			aQualTX.fta_code = "NAFTA";
			aQualTX.org_code = "REN";
			aQualTX.hs_num = "HSNUM" + aIndex;
			aList.add(aQualTX);
		}
		
		int aBatchSize = 10000;
		aBatchPersistenceQueue = new BatchDataRecordPersistenceQueue("Test Queue", aBatchSize, 5, aSqlStatement, this.txMgr, this.dataSrc);
		aBatchPersistenceQueue.setMaxQueueDepth(1000);
		aBatchPersistenceQueue.start();
		
	
		ArrayList<Future<BatchDataRecordTask>> aFutureList = new ArrayList<>();
	
		aPerfTracker.start();
		for (QualTX aQualTX : aList) {
			Future<BatchDataRecordTask> aFuture = aBatchPersistenceQueue.put(aQualTX);
			aFutureList.add(aFuture);
		}
		
		for (Future<BatchDataRecordTask> aFuture : aFutureList) {
			
			try {
				BatchDataRecordTask aBatchDataRecTask = aFuture.get();
				
				aBatchDataRecTask.waitForCompletion();
	
				MessageFormatter.info(logger, "test", "Saved Qual TX [{0,number,#}]", 
					((QualTX)(aBatchDataRecTask.getDataRecord())).alt_key_qualtx
				);
			}
			catch (ExecutionException e) {
				MessageFormatter.trace(logger, "test", e, "Saved failed");
			}
		}
		
		aPerfTracker.stop("Persisted [{0}] qual_tx records", new Object[]{aList.size()});
	}
}
