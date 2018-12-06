package com.ambr.gtm.fta.qps.bootstrap;

import java.io.File;
import java.text.MessageFormat;

import org.apache.commons.io.FileUtils;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ambr.gtm.fta.qps.qualtx.engine.result.StatusCacheProperties;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.cache.CacheManagerService;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
public class StatusCacheConfiguration 
{
	public static final String							STATUS_TRACKER_CACHE_MANAGER_SERVICE_BEAN_NAME = "StatusTrackerCacheManager";
	@Autowired ConfigurationPropertyResolver			propertyResolver;
	@Autowired UniversalObjectIDGenerator				idGenerator;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@Bean(STATUS_TRACKER_CACHE_MANAGER_SERVICE_BEAN_NAME) 
	public CacheManagerService beanStatusTrackerCacheManagerService()
		throws Exception
	{
		CacheManagerService		aCacheMgrService;
		
		aCacheMgrService = new CacheManagerService("qps", this.propertyResolver, this.idGenerator);
		return aCacheMgrService;
	}
}
