package com.ambr.gtm.fta.qps.gpmsrciva;

import java.io.File;
import java.text.MessageFormat;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.CommandEnum;
import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.UniverseStatusEnum;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.gpmsrciva.api.GetGPMSourceIVAByProductFromPartitionClientAPI;
import com.ambr.gtm.fta.qps.gpmsrciva.api.GetGPMSourceIVABySourceFromPartitionClientAPI;
import com.ambr.gtm.fta.qps.gpmsrciva.api.GetGPMSourceIVAStatusFromPartitionClientAPI;
import com.ambr.platform.rdbms.bootstrap.PrimaryDataSourceConfiguration;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;
import com.ambr.platform.utils.subservice.SubordinateServiceManager;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;
import com.ambr.platform.utils.subservice.exception.ServerUnavailableException;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceIVAUniverse 
{
	static Logger		logger = LogManager.getLogger(GPMSourceIVAUniverse.class);

	private int											partitionCount;
	private int											fetchSize;
	int													maxCursorDepth;
	int													memoryMax;
	int													memoryMin;
	private String										targetSchema;
	private String										serviceJarFileName;
	private SubordinateServiceManager					serviceMgr;
	private GPMSourceIVAUniversePartitionEventHandler	eventHandler;
	private int											serverPort;
	private String										dbURL;
	private String										dbUserName;
	private String										dbTargetSchema;
	private String										dbPassword;
	UniverseStatusEnum									status;
	GPMSourceIVAUniversePartition						localPartition;

	private ConfigurationPropertyResolver		propertyResolver;

    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMSourceIVAUniverse()
		throws Exception
	{
		this.fetchSize = 10000;
		this.maxCursorDepth = -1;
		this.memoryMax = 512;
		this.memoryMin = 256;
		this.serverPort = 80;
		this.setPartitionCount(0);
		this.status = UniverseStatusEnum.PENDING_STARTUP;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void ensureAvailable()
		throws Exception
	{
		int	aAttemptCount = 1;
		
		MessageFormatter.info(logger, "ensureAvailable", "Start");
		while (true) {
			try {
				this.refresh();
				break;
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "ensureAvailable", e, "Refresh failed: Attempt [{0}]", aAttemptCount);
				aAttemptCount++;
			}
		}
		
		MessageFormatter.info(logger, "ensureAvailable", "Complete");
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getFetchSize()
		throws Exception
	{
		return this.fetchSize;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getMaxCursorDepth()
		throws Exception
	{
		return this.maxCursorDepth;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getMemoryMax()
		throws Exception
	{
		return this.memoryMax;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getMemoryMin()
		throws Exception
	{
		return this.memoryMin;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getPartitionCount()
		throws Exception
	{
		return this.partitionCount;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdSrcKey
	 *************************************************************************************
	 */
	public GPMSourceIVAProductContainer getSourceIVAByProduct(long theProdKey)
		throws Exception
	{
		int									aErrorCount = 0;
		PerformanceTracker					aPerfTracker = new PerformanceTracker(logger, Level.DEBUG, "getSourceIVAByProduct");
		final GPMSourceIVAProductContainer	aUniverseContainer = new GPMSourceIVAProductContainer(theProdKey);
		GPMSourceIVAProductContainer		aPartitionContainer;
		
		aPerfTracker.start();
		
		if (this.localPartition != null) {
			aPartitionContainer = this.localPartition.getSourceIVAByProduct(theProdKey);
			if (aPartitionContainer != null) {
				aUniverseContainer.prodSrcKeyList.addAll(aPartitionContainer.prodSrcKeyList);
			}
		}
		else {
		
			for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
				try {
					GetGPMSourceIVAByProductFromPartitionClientAPI	aAPI = new GetGPMSourceIVAByProductFromPartitionClientAPI(aServiceRef);
		
					aPartitionContainer = aAPI.execute(theProdKey);
					if (aPartitionContainer != null) {
						aUniverseContainer.prodSrcKeyList.addAll(aPartitionContainer.prodSrcKeyList);
					}
				}
				catch (Exception e) {
					aErrorCount++;
					MessageFormatter.error(logger, "getSourceIVAByProduct", e, "Error receiving result from partition [{0}]", aServiceRef.getInstanceID());
				}
			}
		}
		
		aPerfTracker.stop("Product [{0,number,#}] Source [{1}] Errors [{2}]", new Object[]
			{
				aUniverseContainer.prodKey, 
				aUniverseContainer.prodSrcKeyList.size(),
				aErrorCount
			}
		);
		
		return aUniverseContainer;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdSrcKey
	 *************************************************************************************
	 */
	public GPMSourceIVAProductSourceContainer getSourceIVABySource(long theProdSrcKey)
		throws Exception
	{
		int									aErrorCount = 0;
		PerformanceTracker					aPerfTracker = new PerformanceTracker(logger, Level.DEBUG, "getSourceIVABySource");
		GPMSourceIVAProductSourceContainer	aUniverseContainer = new GPMSourceIVAProductSourceContainer();
		GPMSourceIVAProductSourceContainer	aPartitionContainer;
		
		aUniverseContainer.prodSrcKey = theProdSrcKey;
		aPerfTracker.start();
		
		if (this.localPartition != null) {
			aPartitionContainer = this.localPartition.getSourceIVABySource(theProdSrcKey);
			if (aPartitionContainer != null) {
				aUniverseContainer.merge(aPartitionContainer);
			}
		}
		else {
		
			for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
				try {
					GetGPMSourceIVABySourceFromPartitionClientAPI	aAPI = new GetGPMSourceIVABySourceFromPartitionClientAPI(aServiceRef);
		
					aPartitionContainer = aAPI.execute(theProdSrcKey);
					if (aPartitionContainer != null) {
						aUniverseContainer.merge(aPartitionContainer);
					}
				}
				catch (Exception e) {
					aErrorCount++;
					MessageFormatter.error(logger, "getSourceIVAByProduct", e, "Error receiving result from partition [{0}]", aServiceRef.getInstanceID());
				}
			}
		}
		
		aPerfTracker.stop("Product [{0,number,#}] Source IVAs [{1}] Errors [{2}]", new Object[]
			{
				aUniverseContainer.prodSrcKey, 
				aUniverseContainer.ivaList.size(),
				aErrorCount
			}
		);
		
		return aUniverseContainer;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param thePaddingLength 
	 *************************************************************************************
	 */
	public String getStatus(int thePaddingLength)
		throws Exception
	{
		ParameterizedMessageUtility		aMsgUtil = new ParameterizedMessageUtility(thePaddingLength);
		
		if (this.localPartition != null) {
			return this.localPartition.getStatus(thePaddingLength);
		}
		
		aMsgUtil.format("GPM Source IVA Universe", false, true);
		for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
			try {
				GetGPMSourceIVAStatusFromPartitionClientAPI	aAPI = new GetGPMSourceIVAStatusFromPartitionClientAPI(aServiceRef);
	
				aMsgUtil.format(aAPI.execute(thePaddingLength+3), true, true);
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "getStatus", e, "Error receiving result from partition [{0}]", aServiceRef.getInstanceID());
			}
		}
		
		return aMsgUtil.getMessage();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void refresh()
		throws Exception
	{
		PerformanceTracker aPerfTracker = new PerformanceTracker(logger, Level.INFO, "refresh");

		try {
			aPerfTracker.start();
			this.status = UniverseStatusEnum.REFRESH_IN_PROGRESS;
			this.shutdown();

			this.status = UniverseStatusEnum.REFRESH_IN_PROGRESS;
			this.startup();
		}
		finally {
			aPerfTracker.stop("GPM Source IVA Universe status [{0}].", new Object[]{this.status.name()});
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theURL
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setDBURL(String theURL)
		throws Exception
	{
		this.dbURL = theURL;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theUserName
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setDBUserName(String theUserName)
		throws Exception
	{
		this.dbUserName = theUserName;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePassword
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setDBPassword(String thePassword)
		throws Exception
	{
		this.dbPassword = thePassword;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theFetchSize
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setFetchSize(int theFetchSize)
		throws Exception
	{
		MessageFormatter.debug(logger, "setFetchSize", "Current [{0}]: Target [{1}]", this.fetchSize, theFetchSize);
		this.fetchSize = theFetchSize;

		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	public void setLocalPartition(GPMSourceIVAUniversePartition thePartition)
		throws Exception
	{
		this.localPartition = thePartition;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxCursorDepth
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setMaxCursorDepth(int theMaxCursorDepth)
		throws Exception
	{
		this.maxCursorDepth = theMaxCursorDepth;
		MessageFormatter.debug(logger, "setMaxCursorDepth", "Max Cursor Depth: [{0}]", this.maxCursorDepth);
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMemInMB
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setMemoryMax(int theMemInMB)
		throws Exception
	{
		MessageFormatter.debug(logger, "setMemoryMax", "Current [{0}]: Target [{1}]", this.memoryMax, theMemInMB);
		this.memoryMax = theMemInMB;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMemInMB
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setMemoryMin(int theMemInMB)
		throws Exception
	{
		MessageFormatter.debug(logger, "setMemoryMin", "Current [{0}]: Target [{1}]", this.memoryMin, theMemInMB);
		this.memoryMin = theMemInMB;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theJarFileName
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setServiceJarFileName(String theJarFileName)
		throws Exception
	{
		this.serviceJarFileName = theJarFileName;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartitionCount
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setPartitionCount(int thePartitionCount)
		throws Exception
	{
		MessageFormatter.debug(logger, "setPartitionCount", "Current [{0}]: Target [{1}]", this.partitionCount, thePartitionCount);
		this.partitionCount = thePartitionCount;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePropertyResolver
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setPropertyResolver(ConfigurationPropertyResolver thePropertyResolver)
		throws Exception
	{
		this.propertyResolver = thePropertyResolver;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theServerPort
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setServerPort(int theServerPort)
		throws Exception
	{
		this.serverPort = theServerPort;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTargetSchema
	 *************************************************************************************
	 */
	public GPMSourceIVAUniverse setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.debug(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void shutdown()
		throws Exception
	{
		if (this.localPartition != null) {
			return;
		}
		
		if (this.status == UniverseStatusEnum.SHUTDOWN) {
			return;
		}
		
		if (this.serviceMgr != null) {
			MessageFormatter.debug(logger, "shutdown", "Initiating shutdown");
			this.serviceMgr.shutdown();
			this.status = UniverseStatusEnum.SHUTDOWN;
			MessageFormatter.debug(logger, "shutdown", "Shutdown complete.");
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTemplate
	 *************************************************************************************
	 */
	public void startup()
		throws Exception
	{
		SubordinateServiceReference		aServiceRef;
		
		if (this.localPartition != null) {
			return;
		}
		
		if (this.serviceJarFileName == null) {
			throw new IllegalStateException("Service JAR file name must be specialized.");
		}
		
		if (this.partitionCount == 0) {
			throw new IllegalStateException("No partitions have been allocated for this universe.");
		}
		
		this.serviceMgr = new SubordinateServiceManager("GPMSourceIVAUniverse", this.serverPort, this.propertyResolver);
		this.serviceMgr.setMemoryMax(this.memoryMax);
		this.serviceMgr.setMemoryMin(this.memoryMin);
		this.serviceMgr.setTargetLibrary(new File(this.serviceJarFileName));
		this.serviceMgr.setEventHandler(new GPMSourceIVAUniversePartitionEventHandler(this));
				
		// Start the desired number of subprocesses
		
		for (int aPartitionNum = 1; aPartitionNum <= this.partitionCount; aPartitionNum++) {
			aServiceRef = this.serviceMgr.createSubordinateService();
			
			// When we start the subprocess, we DON'T want a GPMClassificationUniverse bean to be instantiated
			// It will cause a recursive launch of processes
			aServiceRef.setProperty(GPMSourceIVAUniverseProperties.UNIVERSE_ENABLED, "N");
			
			// We DO want a BOM Universe Partition object to be created
			aServiceRef.setProperty(GPMSourceIVAUniverseProperties.UNIVERSE_PARTITION_ENABLED, "Y");
			
			// This is primarily for information purposes to the subprocess
			aServiceRef.setProperty(
				GPMSourceIVAUniverseProperties.UNIVERSE_PARTITION_COUNT,
				String.valueOf(this.partitionCount)
			);
			
			// We need to let the subprocess know which partition it is
			aServiceRef.setProperty(GPMSourceIVAUniverseProperties.UNIVERSE_PARTITION_NUM, String.valueOf(aPartitionNum));
			
			// The process should run as a SERVICE, which means it will remain online until intentionally shut down
			aServiceRef.setProperty(QPSProperties.COMMAND, CommandEnum.SERVICE.name());
			
			// Set Data Source properties
			aServiceRef.setProperty(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_ENABLED_FLG, "Y");
			aServiceRef.setProperty(QPSProperties.MAX_FETCH_SIZE, String.valueOf(this.fetchSize));
			aServiceRef.setProperty(
				MessageFormat.format("{0}.url", PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_DATA_SOURCE), 
				this.dbURL
			);
			aServiceRef.setProperty(
				MessageFormat.format("{0}.username", PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_DATA_SOURCE), 
				this.dbUserName
			);
			aServiceRef.setProperty(
				MessageFormat.format("{0}.password", PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_DATA_SOURCE), 
				this.dbPassword
			);
			
			aServiceRef.setProperty(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, this.targetSchema);
	
			// Start the process asynchronously
			aServiceRef.start(true);
		}
		
		try {
			this.serviceMgr.waitForServers();
			this.status = UniverseStatusEnum.AVAILABLE;
		}
		catch (ServerUnavailableException e) {
			this.status = UniverseStatusEnum.STARTUP_FAILED;
			throw e;
		}
	}
}
