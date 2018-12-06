package com.ambr.gtm.fta.qts.workmgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

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
}
