package com.ambr.gtm.fta.qts.workmgmt;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.WorkManagementException;
import com.ambr.gtm.fta.qts.trade.MDIQualTxRepository;
import com.ambr.gtm.fta.qts.util.BlockingExecutor;
import com.ambr.gtm.fta.qts.util.StatisticMonitor;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;

//TODO expose interface to get producer stats
//TODO expose interface to stop/start producer
//TODO expose interface to alter consumer pool size
//TODO intermediate producers cannot easily submit batches of work to consumer, since work is submitted to them in quantities of 1 ... review further
//TODO use rest template and check performance
//TODO setup producers as beans and initialize based on command line args (i.e. should producer initialize)
//TODO other stats to include current queue size, estimated time to complete based on last X minutes of processing?
public abstract class QTXProducer implements Runnable
{
	private static final Logger logger = LogManager.getLogger(QTXProducer.class);
	
	protected SchemaDescriptorService schemaService;
	protected JdbcTemplate template;
	protected PlatformTransactionManager txMgr;
	
	private BlockingExecutor m_pool;
	protected int fetchSize;
	protected int threads;
	protected int readAhead;
	protected int batchSize;
	protected int sleepInterval;
	
	private boolean isActive;
	
	private StatisticMonitor processingTime;
	
	private UniversalObjectIDGenerator idGenerator;
	private QTXWorkRepository workRepository;
	private MDIQualTxRepository qualtxRepository;
	
	protected long firstRecordAddedAt;
	protected long lastRecordAddedAt;
	protected long firstRecordCompletedAt;
	protected long lastRecordCompletedAt;
	protected long recordsAdded;
	protected long recordsCompleted;
	
	public QTXProducer(SchemaDescriptorService schemaService, PlatformTransactionManager txMgr, JdbcTemplate template)
	{
		this.schemaService = schemaService;
		this.template = template;
		this.txMgr = txMgr;
	}
	
	public PlatformTransactionManager getTransactionManager()
	{
		return this.txMgr;
	}
	
	public JdbcTemplate getJdbcTemplate()
	{
		return this.template;
	}
	
	public void init(int threads, int readAhead, int fetchSize, int batchSize, int sleepInterval, MDIQualTxRepository qualTxRepository, QTXWorkRepository workRepository, UniversalObjectIDGenerator idGenerator) throws WorkManagementException
	{
		this.processingTime = new StatisticMonitor("ConsumerProcessingTime", 15);
		
		this.idGenerator = idGenerator;
		this.workRepository = workRepository;
		this.qualtxRepository = qualTxRepository;
		
		this.threads = threads;
		this.readAhead = readAhead;
		this.fetchSize = fetchSize;
		this.batchSize = batchSize;
		this.sleepInterval = sleepInterval;
		
		this.template.setFetchSize(this.fetchSize);
		
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.threads + " threads");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.readAhead + " readAhead");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.fetchSize + " fetchSize");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.batchSize + " batchSize");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.sleepInterval + " sleepInterval");
		
		if (this.sleepInterval <= 0) throw new WorkManagementException(this.getClass().getName() + " cannot have a sleepInterval <= 0");
		
		this.m_pool = new BlockingExecutor(threads, readAhead);
	}
	
	public QTXWorkRepository getWorkRepository()
	{
		return this.workRepository;
	}
	
	public MDIQualTxRepository getQualTxRepository()
	{
		return this.qualtxRepository;
	}
	
	public synchronized void recordStats(long itemCount, long duration)
	{
		if (this.firstRecordCompletedAt == 0) this.firstRecordCompletedAt = System.currentTimeMillis();
		this.lastRecordCompletedAt = System.currentTimeMillis();
		this.recordsCompleted = this.recordsCompleted + itemCount;

		this.processingTime.addStatistic(itemCount, duration);
		
		if (this.recordsCompleted == this.recordsAdded)
		{
			logger.info(this.getClass().getSimpleName() + " First Record Added\t" + new Date(this.firstRecordAddedAt));
			logger.info(this.getClass().getSimpleName() + " Last Record Added\t" + new Date(this.lastRecordAddedAt));
			logger.info(this.getClass().getSimpleName() + " Duration(ms)\t" + (this.lastRecordAddedAt - this.firstRecordAddedAt));
			
			logger.info(this.getClass().getSimpleName() + " First Record Completed\t" + new Date(this.firstRecordAddedAt));
			logger.info(this.getClass().getSimpleName() + " Last Record Completed\t" + new Date(this.lastRecordCompletedAt));
			logger.info(this.getClass().getSimpleName() + " Duration(ms)\t" + (this.lastRecordCompletedAt - this.firstRecordCompletedAt));

			logger.info(this.getClass().getSimpleName() + " Total Records Added\t" + this.recordsAdded);
			logger.info(this.getClass().getSimpleName() + " Total Records Completed\t" + this.recordsCompleted);
		}
	}
	
	public synchronized void submit(QTXConsumer<?> task)
	{
		if (this.firstRecordAddedAt == 0) this.firstRecordAddedAt = System.currentTimeMillis();
		this.lastRecordAddedAt = System.currentTimeMillis();
		this.recordsAdded++;
		
		task.setProducer(this);
		this.m_pool.submit(task);
	}
	
	public void setTrackWork(boolean track)
	{
		this.m_pool.setTrackWork(track);
	}
	
	public void startup() throws WorkManagementException
	{
		logger.debug("Startup called");
		
		if (this.isActive == true) throw new WorkManagementException("Producer is already running");
		
		this.isActive = true;
		
		new Thread(this).start();
		
		logger.debug("Startup complete");
	}
	
	public void shutdown()
	{
		logger.debug("Shutdown called");
		
		this.isActive = false;
		this.m_pool.shutdown();
		
		logger.debug("Shutdown complete ... note that consumers may still be running until completion");
	}
	
	public void awaitTermination(long time, TimeUnit unit) throws InterruptedException
	{
		this.isActive = false;
		this.m_pool.awaitTermination(time, unit);
	}
	
	public boolean hasWork()
	{
		return this.m_pool.hasWork();
	}

	public void doWork() throws Exception
	{
		if (this.hasWork() == true)
		{
			return;
		}
		
		this.m_pool.clearTrackedWork();

		this.findWork();
	}
	
	protected abstract void findWork() throws Exception;

	public void run()
	{
		while (this.isActive)
		{
			try
			{
				this.doWork();
			}
			catch (Exception exe)
			{
				logger.error("Exception while proccessing the Requalification works, Retry happens after : " + this.sleepInterval * 1000 + " Seconds", exe);
			}

			try
			{
				Thread.sleep(this.sleepInterval * 1000);
			}
			catch (InterruptedException i)
			{
				logger.error("Interrupt captured.  Shutting down", i);
				this.isActive = false;
			}
		}
	}
	
	public long getNextSequence() throws Exception
	{
		return this.idGenerator.generate().getLSB();
	}
	
	public UniversalObjectIDGenerator getIDGenerator() throws Exception
	{
		return this.idGenerator;
	}
	
	void resetStats()
	{
		this.firstRecordAddedAt = 0;
		this.lastRecordAddedAt = 0;
		this.firstRecordCompletedAt = 0;
		this.lastRecordCompletedAt = 0;
		this.recordsAdded = 0;
		this.recordsCompleted = 0;
	}
}
