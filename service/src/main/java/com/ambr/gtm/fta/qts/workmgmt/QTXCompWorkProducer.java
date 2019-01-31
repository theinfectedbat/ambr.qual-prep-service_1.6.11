package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qts.RequalificationBOMStatus;
import com.ambr.gtm.fta.qts.RequalificationTradeLaneStatus;
import com.ambr.gtm.fta.qts.util.RunnableTuple;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;

public class QTXCompWorkProducer extends QTXProducer
{
	private static final Logger logger = LogManager.getLogger(QTXCompWorkProducer.class);
	
	private QTXWorkProducer workProducer;
	private DataExtensionConfigurationRepository repos;
	public PreparationEngineQueueUniverse		queueUniverse;
	private QualTXBusinessLogicProcessor qtxBusinessLogicProcessor;
	

	public QualTXBusinessLogicProcessor getQtxBusinessLogicProcessor()
	{
		return qtxBusinessLogicProcessor;
	}

	public void setQtxBusinessLogicProcessor(QualTXBusinessLogicProcessor qtxBusinessLogicProcessor)
	{
		this.qtxBusinessLogicProcessor = qtxBusinessLogicProcessor;
	}

	
	public QTXCompWorkProducer(DataExtensionConfigurationRepository repos, PlatformTransactionManager txMgr, JdbcTemplate template)
	{
		super(null, txMgr, template);
		
		this.repos = repos;
	}
	
	public QTXCompWorkProducer(DataExtensionConfigurationRepository repos, PlatformTransactionManager txMgr, JdbcTemplate template, SchemaDescriptorService schemaService)
	{
		super(schemaService, txMgr, template);
		
		this.repos = repos;
	}
	
	
	public void submitWork(CompWorkPackage work)
	{
		QTXCompWorkConsumer consumer = new QTXCompWorkConsumer(work);
		consumer.setQtxBusinessLogicProcessor(qtxBusinessLogicProcessor);
		consumer.setDataExtensionRepository(repos);
		consumer.setJdbcTemplate(template);
		
		this.submit(consumer);
	}

	public void submitWork(ArrayList<CompWorkPackage> work)
	{
		QTXCompWorkConsumer consumer = new QTXCompWorkConsumer(work);
		
		consumer.setDataExtensionRepository(this.repos);
		consumer.setQtxBusinessLogicProcessor(this.qtxBusinessLogicProcessor);
		this.submit(consumer);
	}

	
	public void setPlatformTransactionManager(PlatformTransactionManager platformTransactionManager)
	{
		this.txMgr = platformTransactionManager;
	}
	
	public void setJdbcTemplate(JdbcTemplate jdbcTemplate)
	{
		this.template = jdbcTemplate;
	}
	
	public void setSchemaDescriptorService(SchemaDescriptorService schemaService)
	{
		this.schemaService = schemaService;
	}
	
	public void setQueueUniverse(PreparationEngineQueueUniverse theQueueUniverse)
		throws Exception
	{
		this.queueUniverse = theQueueUniverse;
	}
	
	public void setQTXWorkProducer(QTXWorkProducer workProducer)
	{
		this.workProducer = workProducer;
	}
	
	protected void registeredCompWorkCompleted(CompWorkPackage compWorkPackage)
	{
		this.workProducer.registeredCompWorkCompleted(compWorkPackage);
	}

	@Override
	protected void findWork() throws Exception
	{
		//nothing ... findWork will be a blank message.  items will be submitted directly to this producer from the QTXWorkProducer
	}
	
	public RequalificationBOMStatus getCompRequalificationBOMStatus(long bomKey)
	{
		RequalificationBOMStatus bomStatus = new RequalificationBOMStatus();
		
		bomStatus.bomKey = bomKey;
		
		this.getTradeLaneStatsForBOM(bomKey, bomStatus);
		
		return bomStatus;
	}
	
	public void getTradeLaneStatsForBOM(long bomKey, RequalificationBOMStatus bomStatus)
	{
		long requestTime = System.currentTimeMillis();
		int counter = 0;
		for (Iterator<RunnableTuple> i = this.pendingQueueEntries(); i.hasNext();)
		{
			RunnableTuple tuple = i.next();
			
			if (tuple.future.isDone() || tuple.future.isCancelled())
				continue;
			
			QTXCompWorkConsumer workConsumer = (QTXCompWorkConsumer) tuple.runnable;
			
			if (workConsumer.workList != null)
			{
				for (CompWorkPackage workPackage : workConsumer.workList)
				{
					//TODO need to check for following work package
					if (workPackage.getParentWorkPackage().bom != null && workPackage.getParentWorkPackage().bom.alt_key_bom == bomKey)
					{
						RequalificationTradeLaneStatus tradeLaneStats = new RequalificationTradeLaneStatus();
						
						tradeLaneStats.qualtxKey = workPackage.qualtxComp.alt_key_qualtx;
						tradeLaneStats.requestTime = requestTime;
						
						//Calculate estimate based on metrics
						tradeLaneStats.estimate = System.currentTimeMillis();
						
						double throughput = this.getThroughput(1);  //returns throughput per millisecond
						
						logger.info(bomKey + " found at " + counter + " throughput " + throughput + " at " + tradeLaneStats.estimate);
						
						tradeLaneStats.position = counter;
						tradeLaneStats.duration = (long) ((double) tradeLaneStats.position / throughput);
						tradeLaneStats.estimate += tradeLaneStats.duration;
						
						bomStatus.putTradeLaneStatus(tradeLaneStats);
					}
				}
			}
			
			counter++;
		}
	}
}
