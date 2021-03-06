package com.ambr.gtm.fta.qps.bootstrap;

import java.text.MessageFormat;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverse;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniversePartition;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsUniverseProperties;
import com.ambr.platform.rdbms.bootstrap.PrimaryDataSourceConfiguration;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;
import com.ambr.platform.utils.propertyresolver.UnresolvedPropertyReferenceException;
import com.ambr.platform.utils.subservice.SubordinateServiceConnector;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
public class GPMClaimDetailsUniversePostStartupInitializer implements Runnable
{
	static Logger	logger = LogManager.getLogger(GPMClaimDetailsUniversePostStartupInitializer.class);

	@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME)				private DataSource	dataSrc;
	@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_PROPERTIES_BEAN_NAME) 	private DataSourceProperties dataSrcProperties;

    @Autowired private ConfigurationPropertyResolver 	propertyResolver;
	@Autowired private GPMClaimDetailsUniverse			gpmClaimDetailsUniverse;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClaimDetailsUniversePostStartupInitializer()
		throws Exception
	{
	}
	
	public void run()
	{
		try
		{
			this.completeInitialization();
		}
		catch (Exception e)
		{
			logger.error("Failed to start", e);
		}
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void completeInitialization()
		throws Exception
	{
		int			aMaxQueueDepth;
		int			aFetchSize;
		String 		aTargetSchema;
		int 		aPartitionCount;
		String		aQPSJarFile;
		int			aServerPort;
		int			aMemMax;
		int			aMemMin;
		String		aLocalPartitionEnabledFlag;
		String		aFilterOrgCode;

		MessageFormatter.info(logger, "completeInitialization", "start.");

		if ("Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_ENABLED, "N")) == false) {
			// We don't need to initialize this JVM with a functional GPM Classification Universe
			MessageFormatter.info(logger, "completeInitialization", "GPM Claim Details Universe does not require initialization.");
			return;
		}
				
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aServerPort = Integer.valueOf(this.propertyResolver.getPropertyValue(SubordinateServiceConnector.PROPERTY_NAME_LOCAL_SERVER_PORT, "80"));
		aMemMax = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_MEM_MAX, String.valueOf(this.gpmClaimDetailsUniverse.getMemoryMax())));
		aMemMin = Integer.valueOf(this.propertyResolver.getPropertyValue(GPMClaimDetailsUniverseProperties.UNIVERSE_PARTITION_MEM_MIN, String.valueOf(this.gpmClaimDetailsUniverse.getMemoryMin())));
		aLocalPartitionEnabledFlag = this.propertyResolver.getPropertyValue(QPSProperties.IS_LOCAL_UNIVERSE_ENABLED, "N");
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
		
		try {
			aQPSJarFile = this.propertyResolver.getPropertyValue(QPSProperties.QPS_JAR_FILE);
		}
		catch (UnresolvedPropertyReferenceException e) {
			throw new IllegalStateException(MessageFormat.format("Property [{0}] must be specified", QPSProperties.QPS_JAR_FILE));
		}
		
		this.gpmClaimDetailsUniverse.setMemoryMax(aMemMax);
		this.gpmClaimDetailsUniverse.setMemoryMin(aMemMin);
		this.gpmClaimDetailsUniverse.setDBURL(this.dataSrcProperties.getUrl());
		this.gpmClaimDetailsUniverse.setDBUserName(this.dataSrcProperties.getUsername());
		this.gpmClaimDetailsUniverse.setDBPassword(this.dataSrcProperties.getPassword());
		this.gpmClaimDetailsUniverse.setTargetSchema(aTargetSchema);
		this.gpmClaimDetailsUniverse.setFetchSize(aFetchSize);
		this.gpmClaimDetailsUniverse.setMaxCursorDepth(aMaxQueueDepth);
		this.gpmClaimDetailsUniverse.setServiceJarFileName(aQPSJarFile);
		this.gpmClaimDetailsUniverse.setPartitionCount(aPartitionCount);
		this.gpmClaimDetailsUniverse.setServerPort(aServerPort);
		this.gpmClaimDetailsUniverse.setPropertyResolver(this.propertyResolver);
		
		if ("Y".equalsIgnoreCase(aLocalPartitionEnabledFlag)) {
			GPMClaimDetailsUniversePartition	aPartition = new GPMClaimDetailsUniversePartition(1, 1, aFilterOrgCode);
			
			this.gpmClaimDetailsUniverse.setLocalPartition(aPartition);
			this.gpmClaimDetailsUniverse.setDataSource(this.dataSrc);
		}
		
		try {
			this.gpmClaimDetailsUniverse.startup();
			MessageFormatter.info(logger, "completeInitialization", "complete.");
		}
		catch (Exception e) {
			MessageFormatter.error(logger, "completeInitialization", e, "GPM Claim Details Universe failed to initialize.");
		}
	}
}
