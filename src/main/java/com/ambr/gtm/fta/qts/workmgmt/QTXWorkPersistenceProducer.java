package com.ambr.gtm.fta.qts.workmgmt;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qts.RequalificationBOMStatus;
import com.ambr.gtm.fta.qts.RequalificationTradeLaneStatus;
import com.ambr.gtm.fta.qts.util.RunnableTuple;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;

public class QTXWorkPersistenceProducer extends QTXProducer
{
	private static final Logger logger = LogManager.getLogger(QTXWorkPersistenceProducer.class);
	
	public QTXWorkPersistenceProducer(SchemaDescriptorService schemaService, PlatformTransactionManager txMgr, JdbcTemplate template)
	{
		super(schemaService, txMgr, template);
	}
	
	public void submitWork(WorkPackage workPackage)
	{
		workPackage = workPackage.getRootPackage();
		
		QTXWorkPersistenceConsumer consumer = new QTXWorkPersistenceConsumer(workPackage);
		
		this.submit(consumer);
	}
	
	@Override
	protected void findWork() throws Exception
	{
		//Does not have a find work method.  work is posted to this producer by QTXWorkProducer as QTXWork/QTXCompWork is completed
	}
	
	public RequalificationBOMStatus getPersistentRequalificationBOMStatus(long bomKey)
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
			
			QTXWorkPersistenceConsumer workConsumer = (QTXWorkPersistenceConsumer) tuple.runnable;
			
			if (workConsumer.workList != null)
			{
				//TODO need to check for following work package
				for (WorkPackage workPackage : workConsumer.workList)
				{
					if (workPackage.bom != null && workPackage.bom.alt_key_bom == bomKey)
					{
						RequalificationTradeLaneStatus tradeLaneStats = new RequalificationTradeLaneStatus();
						
						tradeLaneStats.qualtxKey = workPackage.qualtx.alt_key_qualtx;
						tradeLaneStats.requestTime = requestTime;
						
						tradeLaneStats.ftaCode = workPackage.qualtx.fta_code;
						tradeLaneStats.ivaCode = workPackage.qualtx.iva_code;
						tradeLaneStats.effectiveFrom = (workPackage.qualtx.effective_from != null) ? workPackage.qualtx.effective_from.getTime() : null;
						tradeLaneStats.effectiveTo = (workPackage.qualtx.effective_to != null) ? workPackage.qualtx.effective_to.getTime() : null;
						tradeLaneStats.coi = workPackage.qualtx.ctry_of_import;

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
