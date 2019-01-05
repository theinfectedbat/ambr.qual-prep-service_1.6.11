package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qts.QTXMonitoredMetrics;
import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.WorkManagementException;
import com.ambr.gtm.fta.qts.trade.MDIQualTxRepository;
import com.ambr.gtm.fta.qts.util.BlockingExecutor;
import com.ambr.gtm.fta.qts.util.RunnableTuple;
import com.ambr.gtm.fta.qts.util.StatisticMonitor;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;

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
	
	private BlockingExecutor threadExecutor;
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
	
	protected QTXMonitoredMetrics metrics = new QTXMonitoredMetrics(this.getClass().getSimpleName());
	protected ArrayList<QTXMonitoredMetrics> monitoredMetrics = new ArrayList<QTXMonitoredMetrics>();
	
	public QTXProducer(SchemaDescriptorService schemaService, PlatformTransactionManager txMgr, JdbcTemplate template)
	{
		this.schemaService = schemaService;
		this.template = template;
		this.txMgr = txMgr;
	}
	
	public QTXMonitoredMetrics getMetrics()
	{
		return this.metrics;
	}
	
	public synchronized QTXMonitoredMetrics addMonitoredMetrics()
	{
		QTXMonitoredMetrics monitoredMetric = new QTXMonitoredMetrics(this.getClass().getSimpleName());
		
		monitoredMetric.start();
		
		this.monitoredMetrics.add(monitoredMetric);
		
		return monitoredMetric;
	}
	
	public synchronized QTXMonitoredMetrics removeMonitoredMetrics(QTXMonitoredMetrics monitoredMetric)
	{
		this.monitoredMetrics.remove(monitoredMetric);
		
		monitoredMetric.stop();
		
		return monitoredMetric;
	}
	
	public Iterator<RunnableTuple> pendingQueueEntries()
	{
		return this.threadExecutor.pendingQueueEntries();
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
		
		this.threadExecutor = new BlockingExecutor(threads, readAhead);
	}
	
	public QTXWorkRepository getWorkRepository()
	{
		return this.workRepository;
	}
	
	public MDIQualTxRepository getQualTxRepository()
	{
		return this.qualtxRepository;
	}
	
	public synchronized void completedStats(long itemCount, long duration)
	{
		this.updateCompletedStats(this.metrics, itemCount, duration);
		
		for (QTXMonitoredMetrics monitoredMetric : this.monitoredMetrics)
		{
			this.updateCompletedStats(monitoredMetric, itemCount, duration);
		}
	}
	
	private void updateCompletedStats(QTXMonitoredMetrics monitoredMetric, long itemCount, long duration)
	{
		long time = System.currentTimeMillis();
		
		if (monitoredMetric.firstItemCompleted == 0) monitoredMetric.firstItemCompleted = time;
		monitoredMetric.lastItemCompleted = time;
		
		monitoredMetric.completed = monitoredMetric.completed + itemCount;
		monitoredMetric.aggregatedDuration = monitoredMetric.aggregatedDuration + duration;

		this.processingTime.addStatistic(itemCount, duration);
		
//		if (this.recordsCompleted == this.recordsAdded)
//		{
//			logger.info(this.getClass().getSimpleName() + " First Record Added\t" + new Date(this.firstRecordAddedAt));
//			logger.info(this.getClass().getSimpleName() + " Last Record Added\t" + new Date(this.lastRecordAddedAt));
//			logger.info(this.getClass().getSimpleName() + " Duration(ms)\t" + (this.lastRecordAddedAt - this.firstRecordAddedAt));
//			
//			logger.info(this.getClass().getSimpleName() + " First Record Completed\t" + new Date(this.firstRecordAddedAt));
//			logger.info(this.getClass().getSimpleName() + " Last Record Completed\t" + new Date(this.lastRecordCompletedAt));
//			logger.info(this.getClass().getSimpleName() + " Duration(ms)\t" + (this.lastRecordCompletedAt - this.firstRecordCompletedAt));
//
//			logger.info(this.getClass().getSimpleName() + " Total Records Added\t" + this.recordsAdded);
//			logger.info(this.getClass().getSimpleName() + " Total Records Completed\t" + this.recordsCompleted);
//		}
	}
	
	public synchronized void submit(QTXConsumer<?> task)
	{
		this.addedStats();
		
		task.setProducer(this);
		this.threadExecutor.submit(task);
	}
	
	private synchronized void addedStats()
	{
		this.updateAddedStats(this.metrics);
		
		for (QTXMonitoredMetrics monitoredMetric : this.monitoredMetrics)
		{
			this.updateAddedStats(monitoredMetric);
		}
	}
	
	private synchronized void updateAddedStats(QTXMonitoredMetrics monitoredMetric)
	{
		long time = System.currentTimeMillis();
		
		if (monitoredMetric.firstItemAdded == 0) monitoredMetric.firstItemAdded = time;
		
		monitoredMetric.lastItemAdded = time;
		monitoredMetric.added++;
	}
	
	public void setTrackWork(boolean track)
	{
		this.threadExecutor.setTrackWork(track);
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
		this.threadExecutor.shutdown();
		
		logger.debug("Shutdown complete ... note that consumers may still be running until completion");
	}
	
	public void awaitTermination(long time, TimeUnit unit) throws InterruptedException
	{
		this.isActive = false;
		this.threadExecutor.awaitTermination(time, unit);
	}
	
	public boolean hasWork()
	{
		return this.threadExecutor.hasWork();
	}

	public void doWork() throws Exception
	{
		if (this.hasWork() == true)
		{
			return;
		}
		
		this.threadExecutor.clearTrackedWork();

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
	
	public synchronized void resetStats()
	{
		this.metrics = new QTXMonitoredMetrics(this.getClass().getSimpleName());
		
		this.metrics.start();
	}
}
