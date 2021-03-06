package com.ambr.gtm.fta.qps.qualtx.universe;

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
import com.ambr.gtm.fta.qps.bom.api.GetBOMStatusFromPartitionClientAPI;
import com.ambr.gtm.fta.qps.qualtx.universe.api.GetQualTXCountFromPartitionClientAPI;
import com.ambr.gtm.fta.qps.qualtx.universe.api.GetQualTXDetailFromPartitionClientAPI;
import com.ambr.platform.rdbms.bootstrap.PrimaryDataSourceConfiguration;
import com.ambr.platform.utils.log.LoggingProperties;
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
public class QualTXDetailUniverse 
{
	static Logger		logger = LogManager.getLogger(QualTXDetailUniverse.class);

	private int											partitionCount;
	private int											fetchSize;
	int													maxCursorDepth;
	int													memoryMax;
	int													memoryMin;
	private String										targetSchema;
	private String										serviceJarFileName;
	private SubordinateServiceManager					serviceMgr;
	private QualTXDetailUniversePartitionEventHandler	eventHandler;
	private int											serverPort;
	private String										dbURL;
	private String										dbUserName;
	private String										dbTargetSchema;
	private String										dbPassword;
	UniverseStatusEnum									status;
	QualTXDetailUniversePartition						localPartition;
	private ConfigurationPropertyResolver				propertyResolver;
	private DataSource									dataSrc;

    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDetailUniverse()
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
	public int getQualTXCount()
		throws Exception
	{
		int		aCount = 0;
		
		if (this.localPartition != null) {
			aCount += this.localPartition.getQualTXCount();
		}
		else {
			this.waitUntilAvailable();
			for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
				try {
					GetQualTXCountFromPartitionClientAPI		aAPI = new GetQualTXCountFromPartitionClientAPI(aServiceRef);
		
					aCount += aAPI.execute();
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "getBOM", e, "Error receiving result from partition [{0}]", aServiceRef.getInstanceID());
				}
			}
		}
	
		return aCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	public QualTXDetailBOMContainer getQualTXDetailContainer(long theBOMKey)
		throws Exception
	{
		QualTXDetailBOMContainer	aContainer = null;
		PerformanceTracker			aPerfTracker = new PerformanceTracker(logger, Level.DEBUG, "getQualTXDetailContainer");
		
		aPerfTracker.start();
		
		this.waitUntilAvailable();
		if (this.localPartition != null) {
			aContainer = this.localPartition.getQualTXDetailByBOM(theBOMKey);
		}
		else {
			for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
				try {
					GetQualTXDetailFromPartitionClientAPI		aAPI = new GetQualTXDetailFromPartitionClientAPI(aServiceRef);
		
					aContainer = aAPI.execute(theBOMKey);
					if (aContainer != null) {
						break;
					}
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "getBOM", e, "BOM [{0}]: Error receiving result from partition [{0}]", theBOMKey, aServiceRef.getInstanceID());
				}
			}
		}
		
		aPerfTracker.stop("BOM [{0,number,#}] found [{1}]", new Object[]
			{
				theBOMKey, 
				(aContainer != null)
			}
		);
		
		return aContainer;
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
		
		aMsgUtil.format("Qual TX Detail Universe", false, true);
		for (SubordinateServiceReference aServiceRef : this.serviceMgr.getServiceReferences()) {
			try {
				GetBOMStatusFromPartitionClientAPI		aAPI = new GetBOMStatusFromPartitionClientAPI(aServiceRef);
	
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
	 * 
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	public int getTargetPartition(long theBOMKey)
		throws Exception
	{
		int	aPartitionNum = (int)(theBOMKey % this.partitionCount);
	
		return aPartitionNum;
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
			aPerfTracker.stop("Qual TX Detail Universe status [{0}].", new Object[]{this.status.name()});
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
	public QualTXDetailUniverse setDataSource(DataSource theDataSrc)
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
	public QualTXDetailUniverse setDBURL(String theURL)
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
	public QualTXDetailUniverse setDBUserName(String theUserName)
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
	public QualTXDetailUniverse setDBPassword(String thePassword)
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
	public QualTXDetailUniverse setFetchSize(int theFetchSize)
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
	public void setLocalPartition(QualTXDetailUniversePartition thePartition)
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
	public QualTXDetailUniverse setMaxCursorDepth(int theMaxCursorDepth)
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
	public QualTXDetailUniverse setMemoryMax(int theMemInMB)
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
	public QualTXDetailUniverse setMemoryMin(int theMemInMB)
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
	public QualTXDetailUniverse setServiceJarFileName(String theJarFileName)
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
	public QualTXDetailUniverse setPartitionCount(int thePartitionCount)
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
	public QualTXDetailUniverse setPropertyResolver(ConfigurationPropertyResolver thePropertyResolver)
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
	public QualTXDetailUniverse setServerPort(int theServerPort)
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
	public QualTXDetailUniverse setTargetSchema(String theTargetSchema)
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
			this.status = UniverseStatusEnum.AVAILABLE;
			return;
		}
		
		if (this.serviceJarFileName == null) {
			throw new IllegalStateException("Service JAR file name must be specialized.");
		}
		
		if (this.partitionCount == 0) {
			throw new IllegalStateException("No partitions have been allocated for this universe.");
		}
		
		this.serviceMgr = new SubordinateServiceManager("QualTXDetailUniverse", this.serverPort, this.propertyResolver);
		this.serviceMgr.setMemoryMax(this.memoryMax);
		this.serviceMgr.setMemoryMin(this.memoryMin);
		this.serviceMgr.setTargetLibrary(new File(this.serviceJarFileName));
		this.serviceMgr.setLogFileCleanup(true);
		this.serviceMgr.setEventHandler(new QualTXDetailUniversePartitionEventHandler(this));
				
		// Start the desired number of subprocesses
		
		for (int aPartitionNum = 1; aPartitionNum <= this.partitionCount; aPartitionNum++) {
			aServiceRef = this.serviceMgr.createSubordinateService();
			
			// When we start the subprocess, we DON'T want a GPMClassificationUniverse bean to be instantiated
			// It will cause a recursive launch of processes
			aServiceRef.setProperty(QualTXDetailUniverseProperties.UNIVERSE_ENABLED, "N");
			
			// We DO want a BOM Universe Partition object to be created
			aServiceRef.setProperty(QualTXDetailUniverseProperties.UNIVERSE_PARTITION_ENABLED, "Y");
			
			// This is primarily for information purposes to the subprocess
			aServiceRef.setProperty(
				QualTXDetailUniverseProperties.UNIVERSE_PARTITION_COUNT,
				String.valueOf(this.partitionCount)
			);
			
			// We need to let the subprocess know which partition it is
			aServiceRef.setProperty(QualTXDetailUniverseProperties.UNIVERSE_PARTITION_NUM, String.valueOf(aPartitionNum));
			
			// The process should run as a SERVICE, which means it will remain online until intentionally shut down
			aServiceRef.setProperty(QPSProperties.COMMAND, CommandEnum.SERVICE.name());
			
			// Set Data Source properties
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
			aServiceRef.setProperty(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_ENABLED_FLG, "Y");
	
			aServiceRef.setProperty(LoggingProperties.LOGGING_FILE, QualTXDetailUniverseProperties.LOG_FILE_NAME);
			
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
			try {
				Thread.sleep(1000);
			}
			catch (Exception e) {
				MessageFormatter.debug(logger, "waitUntilAvailable", e, "universe status [{0}], error while waiting", this.status);
			}
		}
		
		MessageFormatter.trace(logger, "waitUntilAvailable", "universe available");
	}
}
