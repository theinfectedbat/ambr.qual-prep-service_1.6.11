package com.ambr.gtm.fta.qps.gpmclaimdetail;

import java.io.File;
import java.text.MessageFormat;

import javax.sql.DataSource;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.CommandEnum;
import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.UniverseStatusEnum;
import com.ambr.gtm.fta.qps.gpmclaimdetail.api.GetGPMClaimDetailsFromPartitionClientAPI;
import com.ambr.gtm.fta.qps.gpmclaimdetail.api.GetGPMClaimDetailsStatusFromPartitionClientAPI;
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
public class GPMClaimDetailsUniverse 
{
	static Logger		logger = LogManager.getLogger(GPMClaimDetailsUniverse.class);
	
	public static final String		UNIVERSE_NAME 		= "GPM Claim Detail Universe";

	private int												partitionCount;
	private int												fetchSize;
	int														maxCursorDepth;
	int														memoryMax;
	int														memoryMin;
	private String											targetSchema;
	private String											serviceJarFileName;
	private SubordinateServiceManager						serviceMgr;
	private GPMClaimDetailsUniversePartitionEventHandler	eventHandler;
	private int												serverPort;
	private String											dbURL;
	private String											dbUserName;
	private String											dbTargetSchema;
	private String											dbPassword;
	UniverseStatusEnum										status;
	GPMClaimDetailsUniversePartition						localPartition;
	private ConfigurationPropertyResolver					propertyResolver;
	private DataSource										dataSrc;

    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClaimDetailsUniverse()
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
	 * @param	theProdSrcIvaKey
	 *************************************************************************************
	 */
	public GPMClaimDetailsSourceIVAContainer getClaimDetails(long theProdSrcIvaKey)
		throws Exception
	{
		int										aErrorCount = 0;
		GPMClaimDetailsSourceIVAContainer		aClaimDetailContainer = null;
		PerformanceTracker						aPerfTracker = new PerformanceTracker(logger, Level.DEBUG, "getClaimDetails");
		
		aPerfTracker.start();
		
		if (this.localPartition != null) {
			return this.localPartition.getClaimDetails(theProdSrcIvaKey);
		}
		else {
		
			this.waitUntilAvailable();
			for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
				try {
					GetGPMClaimDetailsFromPartitionClientAPI	aAPI = new GetGPMClaimDetailsFromPartitionClientAPI(aServiceRef);
		
					aClaimDetailContainer = aAPI.execute(theProdSrcIvaKey);
					if (aClaimDetailContainer != null) {
						break;
					}
				}
				catch (Exception e) {
					aErrorCount++;
					MessageFormatter.error(logger, "getClaimDetails", e, "Error receiving result from partition [{0}]", aServiceRef.getInstanceID());
				}
			}
		}
		
		aPerfTracker.stop("Claim Detail for Product Src IVA [{0,number,#}] Errors [{2}]", new Object[]
			{
				(aClaimDetailContainer == null)? -1 : aClaimDetailContainer.prodSrcIVAKey, 
				aErrorCount
			}
		);
		
		return aClaimDetailContainer;
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
		else {
			aMsgUtil.format(UNIVERSE_NAME, false, true);
			for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
				try {
					GetGPMClaimDetailsStatusFromPartitionClientAPI	aAPI = new GetGPMClaimDetailsStatusFromPartitionClientAPI(aServiceRef);
		
					aMsgUtil.format(aAPI.execute(thePaddingLength+3), true, true);
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "getStatus", e, "Error receiving result from partition [{0}]", aServiceRef.getInstanceID());
				}
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
			aPerfTracker.stop("{0} status [{1}].", new Object[]{UNIVERSE_NAME, this.status.name()});
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theDataSrc
	 *************************************************************************************
	 */
	public GPMClaimDetailsUniverse setDataSource(DataSource theDataSrc)
		throws Exception
	{
		this.dataSrc = theDataSrc;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theURL
	 *************************************************************************************
	 */
	public GPMClaimDetailsUniverse setDBURL(String theURL)
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
	public GPMClaimDetailsUniverse setDBUserName(String theUserName)
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
	public GPMClaimDetailsUniverse setDBPassword(String thePassword)
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
	public GPMClaimDetailsUniverse setFetchSize(int theFetchSize)
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
	public void setLocalPartition(GPMClaimDetailsUniversePartition thePartition)	
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
	public GPMClaimDetailsUniverse setMaxCursorDepth(int theMaxCursorDepth)
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
	public GPMClaimDetailsUniverse setMemoryMax(int theMemInMB)
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
	public GPMClaimDetailsUniverse setMemoryMin(int theMemInMB)
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
	public GPMClaimDetailsUniverse setServiceJarFileName(String theJarFileName)
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
	public GPMClaimDetailsUniverse setPartitionCount(int thePartitionCount)
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
	public GPMClaimDetailsUniverse setPropertyResolver(ConfigurationPropertyResolver thePropertyResolver)
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
	public GPMClaimDetailsUniverse setServerPort(int theServerPort)
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
	public GPMClaimDetailsUniverse setTargetSchema(String theTargetSchema)
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
			MessageFormatter.info(logger, "startup", "Local Partition enabled");
			this.localPartition.load(new JdbcTemplate(this.dataSrc));
			return;
		}
		
		if (this.serviceJarFileName == null) {
			throw new IllegalStateException("Service JAR file name must be specialized.");
		}
		
		if (this.partitionCount == 0) {
			throw new IllegalStateException("No partitions have been allocated for this universe.");
		}
		
		this.serviceMgr = new SubordinateServiceManager("GPMClaimDetailUniverse", this.serverPort, this.propertyResolver);
		this.serviceMgr.setMemoryMax(this.memoryMax);
		this.serviceMgr.setMemoryMin(this.memoryMin);
		this.serviceMgr.setTargetLibrary(new File(this.serviceJarFileName));
		this.serviceMgr.setEventHandler(new GPMClaimDetailsUniversePartitionEventHandler(this));
				
		// Start the desired number of subprocesses
		
		for (int aPartitionNum = 1; aPartitionNum <= this.partitionCount; aPartitionNum++) {
			aServiceRef = this.serviceMgr.createSubordinateService();
			
			// When we start the subprocess, we DON'T want a GPMClaimDetailsUniverse bean to be instantiated
			// It will cause a recursive launch of processes
			aServiceRef.setProperty(GPMClaimDetailsUniverseProperties.UNIVERSE_ENABLED, "N");
			
			// We DO want a GPMClaimDetail Universe Partition object to be created
			aServiceRef.setProperty(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_ENABLED, "Y");
			
			// This is primarily for information purposes to the subprocess
			aServiceRef.setProperty(
					GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_COUNT,
				String.valueOf(this.partitionCount)
			);
			
			// We need to let the subprocess know which partition it is
			aServiceRef.setProperty(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_NUM, String.valueOf(aPartitionNum));
			
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

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void waitUntilAvailable()
		throws Exception
	{
		MessageFormatter.trace(logger, "waitUntilAvailable", "checking universe status");
	
		while (this.status != UniverseStatusEnum.AVAILABLE) {
			MessageFormatter.trace(logger, "waitUntilAvailable", "universe status [{0}], waiting", this.status);
			Thread.sleep(1000);
		}
		
		MessageFormatter.trace(logger, "waitUntilAvailable", "universe available");
	}
}
