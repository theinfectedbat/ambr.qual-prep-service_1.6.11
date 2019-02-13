package com.ambr.gtm.fta.qps.bootstrap;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import com.ambr.gtm.fta.qps.QPSProperties;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
public class UniversePostStartupInitializer 
{
	static Logger	logger = LogManager.getLogger(UniversePostStartupInitializer.class);
	
	@Autowired ConfigurationPropertyResolver propertyResolver;
	@Autowired BOMUniversePostStartupInitializer bomUniversePostStartupInitializer;
	@Autowired GPMClaimDetailsUniversePostStartupInitializer gpmClaimDetailsUniversePostStartupInitializer;
	@Autowired GPMClassificationUniversePostStartupInitializer gpmClassificationUniversePostStartupInitializer;
	@Autowired GPMSourceIVAUniversePostStartupInitializer gpmSourceIVAUniversePostStartupInitializer;
	@Autowired QualTXDetailUniversePostStartupInitializer qualTXDetailUniversePostStartupInitializer;
	@Autowired PartnerDetailUniversePostStartupInitializer ptnrDetailUniversePostStartupInitializer;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public UniversePostStartupInitializer()
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
		PerformanceTracker		aPerfTracker = new PerformanceTracker(logger, Level.INFO, "completeInitialization");

		Runnable[] aRunnableList = new Runnable[]
			{
				bomUniversePostStartupInitializer,
				gpmClaimDetailsUniversePostStartupInitializer,
				gpmClassificationUniversePostStartupInitializer,
				gpmSourceIVAUniversePostStartupInitializer,
				qualTXDetailUniversePostStartupInitializer,
				ptnrDetailUniversePostStartupInitializer,
			}
		;
		int aThreadCount = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QPSProperties.IS_LOCAL_UNIVERSE_MULTITHREADED, "Y"))? aRunnableList.length : 1;

		try {
			aPerfTracker.start();
			MessageFormatter.info(logger, "completeInitialization", "Initializing [{0}] qual prep caches using [{1}] threads", aRunnableList.length, aThreadCount);
			
			ExecutorService aExecutor = Executors.newFixedThreadPool(aThreadCount);
	
			for (Runnable aRunnable : aRunnableList) {
				aExecutor.submit(aRunnable);
			}
			
			aExecutor.shutdown();
			while (!aExecutor.isTerminated()) {
				try {
					aExecutor.awaitTermination(1, TimeUnit.MINUTES);
				}
				catch (Exception e) {
					MessageFormatter.debug(logger, "completeInitialization", e, "exception while waiting for initialization tasks to complete");
				}
			}
		}
		finally {
			aPerfTracker.stop("Post startup initialization complete", (Object[])null);
		}
	}
}
