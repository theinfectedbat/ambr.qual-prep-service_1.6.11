package com.ambr.gtm.fta.qps.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

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
		logger.info("Post startup launching universe threads");

		Thread bom = new Thread(bomUniversePostStartupInitializer);
		Thread gpmClaim = new Thread(gpmClaimDetailsUniversePostStartupInitializer);
		Thread gpmClass = new Thread(gpmClassificationUniversePostStartupInitializer);
		Thread gpmSource = new Thread(gpmSourceIVAUniversePostStartupInitializer);
		Thread qualtxDetail = new Thread(qualTXDetailUniversePostStartupInitializer);
		Thread ptnrDetail = new Thread(ptnrDetailUniversePostStartupInitializer);
		
		bom.start();
		gpmClaim.start();
		gpmClass.start();
		gpmSource.start();
		qualtxDetail.start();
		ptnrDetail.start();
	
		bom.join();
		gpmClaim.join();
		gpmClass.join();
		gpmSource.join();
		qualtxDetail.join();
		ptnrDetail.join();
		
		logger.info("Post startup initialization complete");
	}
}
