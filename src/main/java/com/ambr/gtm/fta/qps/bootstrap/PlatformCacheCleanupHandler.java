package com.ambr.gtm.fta.qps.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import com.ambr.gtm.fta.qps.bom.BOMUniverseProperties;
import com.ambr.platform.utils.cache.CacheManagerService;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
public class PlatformCacheCleanupHandler 
{
	static Logger	logger = LogManager.getLogger(PlatformCacheCleanupHandler.class);

	@Autowired ConfigurationPropertyResolver	propertyResolver;

	@Autowired
	@Qualifier(StatusCacheConfiguration.STATUS_TRACKER_CACHE_MANAGER_SERVICE_BEAN_NAME)	
	CacheManagerService cacheMgrService;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@EventListener(ApplicationReadyEvent.class)
    public void cleanup()
    	throws Exception
	{
		// The cache directory cleanup should only be done from the "main" service JVM.  We determine whether this is 
		// the main service JVM based on whether the BOM Universe is configured in the JVM.

		if ("N".equalsIgnoreCase(this.propertyResolver.getPropertyValue(BOMUniverseProperties.UNIVERSE_ENABLED, "N"))) {
			return;
		}
		
		this.cacheMgrService.cleanupDirectories();
    }
}
