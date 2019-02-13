package com.ambr.gtm.fta.qps.bootstrap;

import java.text.MessageFormat;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniverse;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniversePartition;
import com.ambr.gtm.fta.qps.ptnr.PartnerDetailUniverseProperties;
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
public class PartnerDetailUniversePostStartupInitializer implements Runnable
{
	static Logger	logger = LogManager.getLogger(PartnerDetailUniversePostStartupInitializer.class);

	@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_BEAN_NAME)				private DataSource dataSrc;
	@Autowired @Qualifier(PrimaryDataSourceConfiguration.DATA_SOURCE_PROPERTIES_BEAN_NAME) 	private DataSourceProperties dataSrcProperties;

    @Autowired private ConfigurationPropertyResolver 	propertyResolver;
	@Autowired private PartnerDetailUniverse			universe;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public PartnerDetailUniversePostStartupInitializer()
		throws Exception
	{
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
	
		if ("Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(PartnerDetailUniverseProperties.UNIVERSE_ENABLED, "N")) == false) {
			// We don't need to initialize this JVM with a functional Partner Detail Universe
			MessageFormatter.info(logger, "completeInitialization", "Partner Detail Universe does not require initialization.");
			return;
		}
		
		aMaxQueueDepth = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_QUEUE_DEPTH, "-1"));
		aFetchSize = Integer.valueOf(this.propertyResolver.getPropertyValue(QPSProperties.MAX_FETCH_SIZE, "1000"));
		aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA, null);
		aPartitionCount = Integer.valueOf(this.propertyResolver.getPropertyValue(PartnerDetailUniverseProperties.UNIVERSE_PARTITION_COUNT, "1"));
		aServerPort = Integer.valueOf(this.propertyResolver.getPropertyValue(SubordinateServiceConnector.PROPERTY_NAME_LOCAL_SERVER_PORT, "80"));
		aMemMax = Integer.valueOf(this.propertyResolver.getPropertyValue(PartnerDetailUniverseProperties.UNIVERSE_PARTITION_MEM_MAX, String.valueOf(this.universe.getMemoryMax())));
		aMemMin = Integer.valueOf(this.propertyResolver.getPropertyValue(PartnerDetailUniverseProperties.UNIVERSE_PARTITION_MEM_MIN, String.valueOf(this.universe.getMemoryMin())));
		aLocalPartitionEnabledFlag = this.propertyResolver.getPropertyValue(QPSProperties.IS_LOCAL_UNIVERSE_ENABLED, "N");
		aFilterOrgCode = this.propertyResolver.getPropertyValue(QPSProperties.FILTER_ORG_CODE, null);
	
		try {
			aQPSJarFile = this.propertyResolver.getPropertyValue(QPSProperties.QPS_JAR_FILE);
		}
		catch (UnresolvedPropertyReferenceException e) {
			throw new IllegalStateException(MessageFormat.format("Property [{0}] must be specified", QPSProperties.QPS_JAR_FILE));
		}
		
		this.universe.setMemoryMax(aMemMax);
		this.universe.setMemoryMin(aMemMin);
		this.universe.setDBURL(this.dataSrcProperties.getUrl());
		this.universe.setDBUserName(this.dataSrcProperties.getUsername());
		this.universe.setDBPassword(this.dataSrcProperties.getPassword());
		this.universe.setTargetSchema(aTargetSchema);
		this.universe.setFetchSize(aFetchSize);
		this.universe.setMaxCursorDepth(aMaxQueueDepth);
		this.universe.setServiceJarFileName(aQPSJarFile);
		this.universe.setPartitionCount(aPartitionCount);
		this.universe.setServerPort(aServerPort);
		this.universe.setPropertyResolver(this.propertyResolver);
		
		if ("Y".equalsIgnoreCase(aLocalPartitionEnabledFlag)) {
			PartnerDetailUniversePartition	aPartition = new PartnerDetailUniversePartition(1, 1, aFilterOrgCode);
			
			this.universe.setLocalPartition(aPartition);
			this.universe.setDataSource(this.dataSrc);
		}
		
		try {
			this.universe.startup();
			MessageFormatter.info(logger, "completeInitialization", "complete.");
		}
		catch (Exception e) {
			MessageFormatter.error(logger, "completeInitialization", e, "Partner Detail Universe failed to initialize.");
		}
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
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
}
