package com.ambr.gtm.fta.qps.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import com.ambr.gtm.fta.qts.QTSProperties;
import com.ambr.gtm.fta.qts.workmgmt.QTXCompWorkProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXStageProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkPersistenceProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkProducer;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Configuration
public class QTXWorkProducerPostStartupInitializer 
{
	static Logger	logger = LogManager.getLogger(QTXWorkProducerPostStartupInitializer.class);

	@Autowired private QTXWorkProducer qtxWorkProducer;
	@Autowired private QTXStageProducer qtxStageProducer;
	@Autowired private QTXCompWorkProducer qtxCompWorkProducer;
	@Autowired private QTXWorkPersistenceProducer qtxWorkPersistenceProducer;	
	@Autowired private ConfigurationPropertyResolver propertyResolver;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QTXWorkProducerPostStartupInitializer()
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
		boolean requalServiceRequired = "Y".equalsIgnoreCase(this.propertyResolver.getPropertyValue(QTSProperties.REQUAL_SERVICE_START, "N"));
		
		this.qtxWorkProducer.setQTXCompWorkProducer(this.qtxCompWorkProducer);
		this.qtxWorkProducer.setQTXWorkPersistenceProducer(this.qtxWorkPersistenceProducer);
		this.qtxWorkProducer.setQTXStageProducer(this.qtxStageProducer);
		this.qtxCompWorkProducer.setQTXWorkProducer(this.qtxWorkProducer);
		this.qtxWorkProducer.setStatus(qtxWorkProducer.REQUAL_SERVICE_AVAILABLE);

		/*if (requalServiceRequired == true)
		{
			this.qtxWorkProducer.startup();
		}*/
	}
}
